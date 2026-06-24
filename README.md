# iceberg_delta

openGauss 扩展插件，通过 FDW 机制实现 Iceberg 外表的 INSERT 截流与 Delta 内表缓冲，并提供将缓冲数据落湖（flush）到 Iceberg 数据湖的能力。

---

## 一、设计思路

### 核心问题

直接把 Iceberg 当成可写的远端表来 `INSERT`，在数据库事务模型下代价极高：Iceberg 的每次写入都伴随 Parquet 文件落盘 + metadata snapshot 提交，属于"重"操作，与 openGauss 事务的轻量、行级、可回滚模型不匹配。

### 本插件的方案

不把数据直接写向 Iceberg，而是**用一张本地 USTORE 内表充当写缓冲区（delta 表）**：

1. **写入快**：`INSERT` 截流，数据落入本地 USTORE delta 表，走标准 executor 数据流，开销与普通本地表写入相当，且天然支持事务回滚。
2. **批量落湖**：用户在合适时机调用 `flush` 函数，把 delta 缓冲区里积攒的行一次性 Append 到 Iceberg（写 Parquet + 提交 snapshot），摊薄 Iceberg 的提交开销。
3. **自动维护**：delta 内表的创建、映射、级联删除由 DDL hook 全自动完成，用户只面对外表，感知不到 delta 表的存在。

一句话：**外表对用户是 Iceberg 表的入口，INSERT 实际写本地缓冲，flush 把缓冲搬运到湖。**

---

## 二、整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  用户 SQL 层                                                │
│                                                            │
│  CREATE FOREIGN TABLE t (...) SERVER iceberg_server ...    │
│  INSERT INTO t VALUES (...)                                │
│  SELECT * FROM iceberg_delta.iceberg_delta_flush('t');     │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│  iceberg_delta 扩展（.so 动态库，shared_preload_libraries） │
│                                                            │
│  ① DDL Hook（注册 ProcessUtility_hook）                     │
│       CREATE FT → 建 delta 表 + mapping + 依赖 + options 缓存│
│       DROP FT   → 清 mapping（delta 表 DEPENDENCY_AUTO 级联删）│
│                                                            │
│  ② FDW handler（iceberg_fdw）                                │
│       ExecForeignInsert → INSERT 截流，写入 delta 内表        │
│       Scan 回调         → 读取 delta 内表                    │
│                                                            │
│  ③ 落湖函数 iceberg_delta_flush()                            │
│       delta 行 → iceberg-lite → Append 到 S3 Iceberg → 清 delta│
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│  存储层                                                    │
│                                                            │
│  ┌────────────────────────┐   ┌───────────────────────────┐ │
│  │ delta USTORE 内表        │   │ Iceberg 表（S3 / MinIO）  │ │
│  │ iceberg_delta.<名>_delta│   │ Parquet + metadata snapshot│ │
│  │ 写缓冲区（未落湖数据）    │   │ 已落湖数据                 │ │
│  └────────────────────────┘   └───────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### 模块文件

| 文件 | 职责 |
|------|------|
| `iceberg_delta.cpp` | `_PG_init` 安装 `ProcessUtility_hook` |
| `ddl_hook.cpp` | CREATE/DROP FOREIGN TABLE 拦截，建 delta 表 + 维护映射 |
| `delta_table.cpp` | delta USTORE 内表的建表语句构造 |
| `catalog.cpp` | `iceberg_delta.mapping` 映射表的增删改查 |
| `fdw_handler.cpp` | FDW handler，注册 scan/modify 全部回调 |
| `fdw_modify.cpp` | INSERT 截流、`IsForeignRelUpdatable`、delta_relid 缓存查找 |
| `flush.cpp` | `iceberg_delta_flush` 落湖实现 + S3 配置解析 |
| `fdw_options.cpp` | 外表选项校验器 |

### 插件如何挂入 openGauss

插件通过 `_PG_init` 在 postmaster 启动时注册一个 `ProcessUtility_hook`，从而在所有 DDL 语句执行前获得拦截机会：

```cpp
// iceberg_delta.cpp —— 扩展加载入口
ProcessUtility_hook_type prev_ProcessUtility = NULL;   // 保存原 hook，卸载时还原

// 拦截器：所有 utility 语句（CREATE/DROP 等）都会先经过这里
static void IcebergDeltaProcessUtility(processutility_context* pucontext,
                                       DestReceiver* dest, ...) {
    IcebergDeltaDDLHook(...);   // 只处理本插件关心的 CREATE/DROP FOREIGN TABLE，其余透传
}

void _PG_init(void) {
    prev_ProcessUtility = ProcessUtility_hook;      // 记住上一个 hook（hook 链）
    ProcessUtility_hook = IcebergDeltaProcessUtility;  // 把自己挂到链头
}

void _PG_fini(void) {
    ProcessUtility_hook = prev_ProcessUtility;     // 卸载时还原，保证可热插拔
}
```

> 因为 hook 必须在 postmaster 启动时安装，所以 `shared_preload_libraries = 'iceberg_delta'` 是硬性要求——仅在会话里 `CREATE EXTENSION` 不够。

---

## 三、功能详解

### 3.1 创表（CREATE FOREIGN TABLE）

外表一旦建好，DDL hook 会**自动**在 `iceberg_delta` schema 下创建一张结构相同的 USTORE 内表作为缓冲区，用户无需手动操作。

#### 数据流

```
CREATE FOREIGN TABLE t (id int, name text, ...) SERVER iceberg_server OPTIONS (...)
        │
        ▼  DDL Hook 拦截 CreateForeignTableStmt（且 server == iceberg_server）
        │
   ① CallNextUtility()
        │   先让 openGauss 正常建好外表 t，拿到 t 的 relid 与 tupdesc
        │
   ② IcebergDeltaTableCreate()
        │   复制外表列结构，构造并执行：
        │     CREATE TABLE iceberg_delta.t_delta (<同外表列>) WITH (STORAGE_TYPE = USTORE)
        │
   ③ recordDependencyOn(delta → 外表, DEPENDENCY_AUTO)
        │   外表被 DROP 时，delta 表随之级联删除
        │
   ④ IcebergCatalogInsertDeltaTableMapping()
        │   写入映射表：
        │     iceberg_delta.mapping(foreign_relid, delta_relid, delta_schema, delta_name)
        │
   ⑤ IcebergDeltaUpdateForeignTableOptions()
            SPI 执行：
              ALTER FOREIGN TABLE t OPTIONS (
                ADD delta_relid '<oid>', ADD delta_schema '...', ADD delta_name '...')
            把 delta 表信息缓存进外表 options，后续 INSERT/flush 零 SPI 查询即可定位
```

#### 代码片段：DDL Hook 拦截与建表

hook 只对 `iceberg_server` 上的外表生效，其余 DDL 透传给内核：

```cpp
// ddl_hook.cpp —— 拦截 CREATE FOREIGN TABLE
void IcebergDeltaDDLHook(processutility_context* pucontext, ...) {
    Node* parsetree = pucontext->parse_tree;

    if (IsA(parsetree, CreateForeignTableStmt)) {
        CreateForeignTableStmt* stmt = (CreateForeignTableStmt*)parsetree;
        // 关键：只接管 server 名为 "iceberg_server" 的外表，其他外表交给内核
        if (IsIcebergServerByName(stmt->servername)) {
            CallNextUtility(...);                       // ① 先让内核把外表建好
            IcebergDeltaHandleCreateForeignTable(stmt); // ② 再自动建 delta 表 + 映射
            return;
        }
    }
    CallNextUtility(...);  // 非 iceberg 的语句：原样放行
}
```

delta 表的建表语句由列结构复制而成，固定为 USTORE（行存内存表）：

```cpp
// delta_table.cpp —— 按外表列结构生成 delta 表的 CREATE TABLE
StringInfoData buf;
initStringInfo(&buf);
appendStringInfo(&buf, "CREATE TABLE %s.%s (",
                 quote_identifier(schema_name),   // 固定 schema = "iceberg_delta"
                 quote_identifier(table_name));    // 表名 = "<外表名>_delta"

// 逐列复制外表的列名 + 类型，结构与外表完全一致
for (int i = 0; i < tupdesc->natts; i++) {
    Form_pg_attribute attr = &tupdesc->attrs[i];
    if (attr->attisdropped) continue;              // 跳过已删除列
    appendStringInfo(&buf, "%s %s",
                     quote_identifier(NameStr(attr->attname)),
                     format_type_be(attr->atttypid));
}
appendStringInfoString(&buf, ") WITH (STORAGE_TYPE = USTORE)");  // USTORE = 内存行存，支持原地更新

SPI_execute(buf.data, false, 0);   // 在插件事务内执行建表
```

随后记录"delta 依赖外表"的自动依赖——外表 DROP 时 delta 表随之级联删除：

```cpp
// ddl_hook.cpp —— 注册级联删除依赖
ObjectAddress delta_addr, ft_addr;
ObjectAddressSet(delta_addr, RelationRelationId, delta_relid);  // delta 表（依赖方）
ObjectAddressSet(ft_addr,    RelationRelationId, foreign_relid); // 外表（被依赖方）
recordDependencyOn(&delta_addr, &ft_addr, DEPENDENCY_AUTO);       // AUTO：外表删 → delta 删
```

> 之后 hook 还会把 `delta_relid/delta_schema/delta_name` 通过 `ALTER FOREIGN TABLE ... OPTIONS (ADD ...)` 写进外表 options，作为零 SPI 查询的缓存（见 `fdw_modify.cpp`）。

#### delta 内表命名规则

- schema 固定为 `iceberg_delta`
- 表名 = `<外表名>_delta`（如外表 `t_sales` → `t_sales_delta`）
- 外表名过长（含 `_delta` 后缀后超过 `NAMEDATALEN`）会直接报错：
  ```
  ERROR: foreign table name "..." is too long to generate delta table name (max N characters)
  ```
- 同名 delta 表已存在时报 `DUPLICATE_TABLE`（防止跨 schema 同名外表冲突）

#### 选项（OPTIONS）

| 类别 | 选项名 | 说明 |
|------|--------|------|
| 用户选项 | `location` | flush 落湖位置，**优先项**（见 3.3） |
| | `warehouse` + `table_name` | location 的回退组合 |
| | `foldername` | 其他用途选项（已放行） |
| S3 选项 | `s3_endpoint`、`s3_access_key_id`、`s3_secret_access_key`、`s3_region`、`s3_path_style_access`、`s3_ssl_enabled` | 落湖/远端访问配置 |
| 内部选项 | `delta_relid`、`delta_schema`、`delta_name` | **由 DDL hook 自动写入**，用户无需设置 |

> 选项校验在 `fdw_options.cpp::iceberg_fdw_validator` 中完成，未识别的选项名会报 `FDW_INVALID_OPTION_NAME`。

---

### 3.2 插入（INSERT）

`INSERT` 不触及远端 Iceberg，数据被 FDW 的 `ExecForeignInsert` 回调**截流**写入本地 delta USTORE 表。未 flush 前即可通过外表或直接查 delta 表读到。

> **前置依赖**：INSERT 能进入 executor 的前提是 `iceberg_fdw` 在内核 FDW 白名单中，详见[第四节](#四内核改动说明必读)。

#### 数据流

```
INSERT INTO t VALUES (...)
        │
        ▼  planner: PlanForeignModify
        │   校验 commandType ∈ {INSERT}，解析 delta_relid，写入 fdw_private
        ▼  BeginForeignModify
        │   relation_open(delta 表, RowExclusiveLock)
        │   构造 modify_state，存入 ri_FdwState
        │
        ▼  ExecForeignInsert(estate, rinfo, slot, planSlot)   ← 截流点
        │   ① heap_slot_getallattrs(slot)
        │        提取 slot 中全部属性值
        │   ② tableam_tslot_get_tuple_from_slot(delta_rel, slot)
        │        按外表列结构转为 USTORE 存储格式 tuple
        │   ③ tableam_tuple_insert(delta_rel, tuple, es_output_cid, ...)
        │        写入 delta 内表（非远端 Iceberg）
        │
        ▼  EndForeignModify
            relation_close(delta 表)
            返回 slot → 支持 RETURNING 子句
```

#### 代码片段：INSERT 截流实现

截流的全部工作就在 `ExecForeignInsert` 这一个回调里——三步把数据写进 delta 表，全程不碰远端 Iceberg：

```cpp
// fdw_modify.cpp —— INSERT 截流点（每插入一行调用一次）
TupleTableSlot* IcebergDeltaExecForeignInsert(EState* estate, ResultRelInfo* rinfo,
                                              TupleTableSlot* slot, TupleTableSlot* planSlot) {
    IcebergDeltaFdwModifyState* st = (IcebergDeltaFdwModifyState*)rinfo->ri_FdwState;

    // ① 从待插入 slot 提取全部属性值（解构成 Datum 数组）
    heap_slot_getallattrs(slot, false);

    // ② 按外表列结构，把 Datum 数组转成 USTORE 存储格式的物理 tuple
    Tuple tuple = tableam_tslot_get_tuple_from_slot(st->delta_rel, slot);

    // ③ 写入本地 delta 内表（不是远端 Iceberg！）—— 走标准 tableam 插入路径
    (void)tableam_tuple_insert(st->delta_rel, tuple, estate->es_output_cid, 0, NULL);

    return slot;  // 返回 slot，支持 INSERT ... RETURNING
}
```

INSERT 能走到这里的门槛是"外表声明自己可 INSERT"——`IsForeignRelUpdatable` 返回带 `CMD_INSERT` 的位掩码：

```cpp
// fdw_modify.cpp —— 告知 planner/executor：本外表支持哪些 DML
int IcebergDeltaIsForeignRelUpdatable(Relation rel) {
    return (1 << CMD_INSERT) | (1 << CMD_DELETE);  // 位掩码：每一位对应一种命令类型
}
```

> 上面 `CMD_DELETE` 的存在是为了支持 DELETE 功能（本 README 未展开）。若内核 FDW 白名单未含 `iceberg_fdw`，`CMD_INSERT` 这一位会被 planner 在更早阶段直接拒掉，根本到不了 `ExecForeignInsert`——这就是第四节内核改动的由来。

#### 关键点

- **截流而非转发**：`ExecForeignInsert` 是天然截流点，数据只进本地 delta 表，不产生任何远端 Iceberg I/O。
- **事务一致**：delta 写入在当前 PG 事务内，事务回滚则插入一并回滚。
- **列结构对齐**：delta 表按外表列定义创建（见 3.1），列顺序与外表一致，转换时按位置对齐。

---

### 3.3 落湖（flush）

将 delta 缓冲区中积攒的行批量写入远端 Iceberg 数据湖，成功后清空 delta 缓冲区。

```
SELECT iceberg_delta.iceberg_delta_flush('t');
```

返回值：本次刷写的行数（`int8`）。delta 为空时返回 `0`。

#### 数据流

```
iceberg_delta_flush('t')
        │
        ▼ 1. 解析落湖位置
        │   优先读外表 option location；
        │   缺失则回退 warehouse + table_name（但本地非 s3:// 路径直接报错）
        │   解析 s3://bucket/table_path → {bucket, table_path}
        │
        ▼ 2. 定位 delta 表
        │   delta_relid 优先取外表 options 缓存（零 SPI）；
        │   OID 失效则按 schema/name 查找；再回退到 mapping 表
        │
        ▼ 3. tableam 扫描 delta 表
        │   逐行 Datum → iceberg_lite::Record，同时收集每行物理 ctid
        │   若 nrows == 0 → 直接返回 0
        │
        ▼ 4. 懒创建 Iceberg 表
        │   IcebergTable::Open(table_path)
        │     成功 → 复用已有表
        │     失败 → IcebergTable::Create（用 delta 的 schema 建表）
        │   即首次 flush 才在 S3 上真正建表
        │
        ▼ 5. Append 到数据湖
        │   iceberg_table.Append(records)  →  写 Parquet + 提交新 snapshot
        │
        ▼ 6. 清空 delta 缓冲区
            DeleteDeltaRows(delta, ctids)  按物理位置删 delta 行 + CommandCounterIncrement
            返回 nrows
```

#### 代码片段：懒创建与 Append

首次 flush 时 Iceberg 表可能尚不存在，用"先 Open，失败则 Create"的模式实现懒创建——表结构与 delta 表一致：

```cpp
// flush.cpp —— 懒创建：首次 flush 才在 S3 上真正建表
iceberg_lite::IcebergTable iceberg_table = [&]() {
    try {
        return iceberg_lite::IcebergTable::Open(s3_client, table_path);  // 尝试打开已有表
    } catch (const std::exception&) {
        // 打开失败 = 表不存在：用 delta 表的 schema 现场创建一张 Iceberg 表
        iceberg_lite::Schema schema = BuildIcebergSchemaFromTupleDesc(tupdesc);
        return iceberg_lite::IcebergTable::Create(s3_client, table_path, schema);
    }
}();

// 批量追加：一次 Append 把所有缓冲行写成一个 Parquet 文件并提交新 snapshot
try {
    iceberg_table.Append(records);   // records 来自对 delta 表的扫描结果
} catch (const std::exception& e) {
    ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                    errmsg("failed to append records to iceberg table: %s", e.what())));
}

// Append 成功后才删 delta 行；若后续事务回滚，删除也回滚，依赖 Iceberg 提交的原子性
DeleteDeltaRows(delta_rel, ctids, nrows);
```

S3 凭证按"options → 环境变量 → 默认值"三级解析，access_key/secret 无默认、缺失即报错：

```cpp
// flush.cpp —— 三级凭证解析（options 最高，环境变量次之，默认值兜底）
static const char* ResolveS3Option(List* options, const char* opt_name,
                                   const char* env_name, const char* fallback) {
    const char* from_opt = GetOptionString(options, opt_name);   // ① 外表 options
    if (from_opt != NULL && from_opt[0] != '\0') return from_opt;

    const char* from_env = getenv(env_name);                      // ② 环境变量
    if (from_env != NULL && from_env[0] != '\0') return from_env;

    return fallback;                                             // ③ 默认值（ak/sk 传 NULL）
}

// access_key 不留默认值：三级都没给就直接报错，杜绝凭证进源码
const char* ak = ResolveS3Option(options, ICEBERG_OPT_S3_ACCESS_KEY_ID,
                                 ENV_S3_ACCESS_KEY_ID, NULL);
if (ak == NULL) {
    ereport(ERROR, (errmsg("iceberg_delta: S3 access key not provided; "
                           "set option 's3_access_key_id' or env ICEBERG_S3_ACCESS_KEY_ID")));
}
```

#### S3 凭证解析优先级（三级，从高到低）

| 优先级 | 来源 |
|--------|------|
| 1 | 外表 options（`s3_endpoint` / `s3_access_key_id` / ...） |
| 2 | 环境变量（`ICEBERG_S3_ENDPOINT` / `ICEBERG_S3_ACCESS_KEY_ID` / ...） |
| 3 | 编译期默认值（仅 `endpoint`/`region` 有默认） |

> `access_key` 与 `secret_access_key` **无任何默认值**，三级均未提供时直接报错，避免敏感凭证进入源码与仓库。

#### PG → Iceberg 类型映射

| PG 类型 | Iceberg 类型 |
|---------|--------------|
| `bool` | Boolean |
| `int2` / `int4` | Int |
| `int8` | Long |
| `float4` | Float |
| `float8` | Double |
| `date` / `time` | Date / Time |
| `timestamp` / `timestamptz` | Timestamp / TimestampTz |
| `bytea` | Binary |
| `numeric` | Decimal |
| 其他（含 `text`/`varchar`） | String |

#### 示例（S3 / MinIO）

```sql
CREATE FOREIGN TABLE t_sales (
    id   bigint,
    name text,
    score double precision
) SERVER iceberg_server OPTIONS (
    location              's3://mybucket/warehouse/t_sales',
    s3_endpoint           'http://127.0.0.1:19000',
    s3_access_key_id      'minioadmin',
    s3_secret_access_key  'minioadmin',
    s3_region             'us-east-1',
    s3_path_style_access  'true',
    s3_ssl_enabled        'false'
);

INSERT INTO t_sales VALUES (1, 'alice', 95.5), (2, 'bob', 88.0);

-- 落湖（返回刷写行数，此例为 2）
SELECT iceberg_delta.iceberg_delta_flush('t_sales');
```

---

## 四、内核改动说明（必读）

本扩展的 `INSERT` 需要通过 openGauss 的 FDW 白名单校验，否则在 planner 阶段即被拒绝：

```
ERROR:  Un-support feature
DETAIL: insert statement is an INSERT INTO VALUES(...)
```

因此需要**两处内核修改**（详见 [`BUILD_AND_DEBUG.md`](./BUILD_AND_DEBUG.md)「问题 5」）：

| 文件 | 改动 |
|------|------|
| `src/include/postgres.h` | 新增宏 `#define ICEBERG_FDW "iceberg_fdw"`（置于 `MOT_FDW_SERVER` 之后、`DFS_FDW` 之前） |
| `src/gausskernel/cbb/extension/foreign/foreign.cpp` | `supportFDWType[]` 数组末尾追加 `ICEBERG_FDW` |

修改后需重新编译 openGauss 内核：

```bash
cd ~/openGauss-server
./build.sh -m release -3rd ~/binarylibs
```

> 内核重编会清空插件 `.so`，需随后重新 `make install` 并重建 `proc_srclib` 符号链接。

---

## 五、数据可见性与边界

| 时机 | 外表 `SELECT` 能看到的数据 |
|------|---------------------------|
| INSERT 后、flush 前 | ✅ **能看到**（scan 回调读取 delta 内表） |
| flush 后 | ❌ **看不到**（delta 已清空，外表 scan 不读 Iceberg 已落湖数据） |

> **重要**：当前外表 `SELECT` 只读取 delta 缓冲区（未落湖数据），不合并读取已落湖的 Iceberg 数据。因此 flush 后这些行从外表查询中"消失"是预期行为，并非数据丢失——它们已安全落湖。两阶段合并读取（lake + delta）属后续路线。

其他边界：

- **flush 仅支持 S3 / MinIO**：本地 `warehouse`（非 `s3://`）路径在 flush 时直接报错，不支持本地文件系统。
- **DELETE 尚未在文档范围**：本 README 聚焦创表/插入/flush 三项功能。

---

## 六、编译与部署

> 完整流程与排障见 [`BUILD_AND_DEBUG.md`](./BUILD_AND_DEBUG.md)，此处仅列要点。

```bash
# 1. 编译插件（须先用 binarylibs GCC 10.3，依赖 iceberg-lite）
cd ~/plugin
make clean && make && make install

# 2. 安装 NOT FENCED 函数库
mkdir -p $GAUSSHOME/lib/postgresql/proc_srclib
ln -sf $GAUSSHOME/lib/postgresql/iceberg_delta.so \
       $GAUSSHOME/lib/postgresql/proc_srclib/iceberg_delta

# 3. 配置并重启
#    postgresql.conf:  shared_preload_libraries = 'iceberg_delta'
gs_ctl restart -D ~/ogdata -Z single_node

# 4. 安装扩展
gsql -d postgres -p 9876 -c "CREATE EXTENSION iceberg_delta;"
```

端到端验证：

```bash
ICEBERG_S3_ACCESS_KEY_ID=... ICEBERG_S3_SECRET_ACCESS_KEY=... \
bash test_flush_e2e.sh
```

---

## 七、文档索引

| 文档 | 说明 |
|------|------|
| [`BUILD_AND_DEBUG.md`](./BUILD_AND_DEBUG.md) | 编译流程、内核改动、排障记录、功能验证 |
| [`iceberg_delta--1.0.sql`](./iceberg_delta--1.0.sql) | 扩展安装脚本（mapping 表、FDW、server、flush 函数） |
| [`test_flush_e2e.sh`](./test_flush_e2e.sh) | flush 端到端测试（建表→插入→落湖→MinIO 校验） |
