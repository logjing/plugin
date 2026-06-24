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

### 3.4 删除（DELETE）

> **本节内容仅在 `delete` 分支实现**，其余分支不含 DELETE 能力。

外表的 `DELETE FROM <外表> WHERE <条件>` 执行**双删**：同时删除**未 flush 的 delta 缓冲行**和**已 flush 进 Iceberg 数据湖的行**。用户面对外表写一条 DELETE 即可，两侧删除由 FDW 回调协作完成。

#### 设计思路：为什么需要双删

一份数据可能存在于两个位置：

- **delta 内表**：INSERT 后、flush 前，行落在本地缓冲区。
- **Iceberg 湖表**：flush 之后，行已落湖（Parquet + snapshot）。

如果 DELETE 只删一侧，就会漏删另一侧，导致数据"删不干净"。因此 DELETE 必须两边都处理。两侧的删除标识互相独立——

- delta 侧按行的**物理 ctid** 删（不需要主键）；
- Iceberg 侧用原 **WHERE 条件重建的 filter 字符串**下发删除（也不依赖主键）。

因此本实现**不强制外表定义 PRIMARY KEY**。

#### 设计思路：标准 FDW 逐行模型

DELETE 必须先"扫到要删的行"，再逐行调 `ExecForeignDelete`。这要求外表的 scan 从 placeholder 升级为**真实 delta 扫描**（读取 delta 内表行 + 暴露 ctid 作为 junk 列）。这正是与 INSERT 截流不同的地方——DELETE 复用了 delta scan 能力。

#### 数据流

```
DELETE FROM 外表 WHERE <条件>
        │
        ▼  planner: PlanForeignModify
        │   放开 CMD_DELETE；校验 WHERE 是否在支持范围内
        │   遍历 WHERE 的 Expr 树，重建为 PyIceberg filter 字符串（如 "id == 1 AND name == 'a'"）
        │   把 delta_relid + filter 字符串塞进 fdw_private
        │
        ▼  AddForeignUpdateTargets
        │   给外表 scan 注入一个 resjunk 列 "ctid"（行物理位置）
        │
        ▼  BeginForeignScan / IterateForeignScan   ← scan 已升级为真扫描
        │   逐行读 delta 内表，把每行的物理 ctid 填进 junk 列
        │
        ▼  ExecForeignDelete(逐行)                  ← 先删 delta
        │   从 planSlot 的 junk 列取出该行 ctid
        │   tableam_tuple_delete(delta_rel, ctid)  删本地缓冲行
        │
        ▼  EndForeignModify                         ← 后删 Iceberg
            IcebergDeltaDeleteFromLake(外表, filter)
            iceberg_table.Delete(filter)  批量删已落湖数据 + 提交新 snapshot
```

#### 删除时序：先删 delta，后删 Iceberg

这是刻意安排的顺序，保证失败时数据一致：

- **delta 删除是本地事务**，回滚成本低；
- **Iceberg delete 涉及 S3 写 + snapshot 提交**，代价高。

若先删 Iceberg 成功、删 delta 失败 → delta 残留行下次 flush 会重新写回 Iceberg，等于"删了又回来"。反之先删 delta，若失败整体回滚，Iceberg 尚未动。

#### 代码片段：WHERE → filter 字符串转换

转换器遍历 openGauss 的 `Expr` 树，把比较/逻辑/字面量重建为 PyIceberg 可解析的字符串。超出支持范围一律 `ereport(ERROR)`：

```cpp
// fdw_modify.cpp —— WHERE Expr 树 → filter 字符串的递归核心
static void AppendFilterExpr(StringInfo buf, Node* node, PlannerInfo* root,
                              int targetRti, bool* hasError) {
    switch (nodeTag(node)) {
        case T_OpExpr:        // 比较表达式：a = 1
            AppendFilterOpExpr(buf, (OpExpr*)node, root, targetRti, hasError);
            break;
        case T_BoolExpr: {    // 逻辑组合：a AND b / a OR b
            BoolExpr* bexpr = (BoolExpr*)node;
            const char* joiner = (bexpr->boolop == OR_EXPR) ? " OR " : " AND ";
            // ... 拼接各子条件
            break;
        }
        case T_Var: {         // 列引用：必须是目标外表的列，否则报错
            Var* var = (Var*)node;
            if ((int)var->varno != targetRti) { *hasError = true; return; }
            // ... 输出列名
            break;
        }
        case T_Const:         // 字面量：1 / 'abc' / 1.5
            AppendFilterConst(buf, (Const*)node);
            break;
        default:              // 不支持的节点类型（IN/LIKE/子查询等）
            *hasError = true;
            break;
    }
}
```

比较运算符在转换时映射成 PyIceberg 的语法（PG 的 `=` → PyIceberg 的 `==`）：

```cpp
// fdw_modify.cpp —— PG 运算符 → PyIceberg 表达式语法
if (strcmp(opname, "=") == 0)      appendStringInfoString(buf, " == ");   // 注意：== 而非 =
else if (strcmp(opname, "<>") == 0) appendStringInfoString(buf, " != ");
else if (strcmp(opname, "<") == 0)  appendStringInfoString(buf, " < ");
// ... <=, >, >= 同理
else { *hasError = true; return; }   // 未知运算符：报"暂不支持"
```

转换失败时给用户清晰的报错（支持范围见下文）：

```cpp
// fdw_modify.cpp —— 不支持的 WHERE 直接 ERROR
if (hasError) {
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("iceberg_fdw DELETE: unsupported WHERE expression"),
             errdetail("Only =, <>, <, <=, >, >=, AND, OR, and simple "
                       "literals are supported in DELETE WHERE clauses")));
}
```

#### 代码片段：注入 ctid junk 列

DELETE 走标准 FDW 逐行模型，需要把 delta 行的物理 ctid 作为 junk 列暴露给 executor，这样 `ExecForeignDelete` 才能拿到"删哪一行"：

```cpp
// fdw_modify.cpp —— 给 DELETE 计划注入 ctid junk 列
void IcebergDeltaAddForeignUpdateTargets(Query* parsetree, RangeTblEntry* target_rte,
                                          Relation target_relation) {
    // SelfItemPointerAttributeNumber = 外表自身的 ctid 列号（与 postgres_fdw 一致）
    Var* var = makeVar((Index)linitial_int(parsetree->resultRelations),
                       SelfItemPointerAttributeNumber, TIDOID, -1, InvalidOid, 0);
    // resjunk=true：该列不对外可见，仅 executor 内部用于定位删除目标
    TargetEntry* tle = makeTargetEntry((Expr*)var,
        list_length(parsetree->targetList) + 1, pstrdup("ctid"), true);
    parsetree->targetList = lappend(parsetree->targetList, tle);
}
```

#### 代码片段：逐行删 delta

每扫到一行，executor 就调一次 `ExecForeignDelete`，从 planSlot 取出该行 ctid 删本地缓冲行——这一步**不碰 Iceberg**：

```cpp
// fdw_modify.cpp —— DELETE 的逐行删除（delta 侧）
TupleTableSlot* IcebergDeltaExecForeignDelete(EState* estate, ResultRelInfo* rinfo,
                                               TupleTableSlot* slot, TupleTableSlot* planSlot) {
    IcebergDeltaFdwModifyState* st = (IcebergDeltaFdwModifyState*)rinfo->ri_FdwState;

    // 从 junk 列取出当前行的 ctid（由 AddForeignUpdateTargets 注入、由 scan 填充）
    Datum ctid_datum = ExecGetJunkAttribute(planSlot, st->ctid_attno, &isNull);
    ItemPointer ctid = (ItemPointer)DatumGetPointer(ctid_datum);

    // 按物理位置删本地 delta 行（非远端 Iceberg）
    TM_FailureData tmfd;
    TM_Result result = tableam_tuple_delete(st->delta_rel, ctid,
                                             GetCurrentCommandId(false),
                                             InvalidSnapshot, GetActiveSnapshot(),
                                             true, NULL, &tmfd, false);
    return slot;
}
```

> 注意 `IsForeignRelUpdatable` 在 3.2 已含 `CMD_DELETE` 位（`return (1<<CMD_INSERT) | (1<<CMD_DELETE)`），这是 DELETE 能进入 executor 的前提。

#### 代码片段：批量删 Iceberg（落湖侧）

所有行处理完后，`EndForeignModify` 拿出之前在 `PlanForeignModify` 重建好的 filter 字符串，对数据湖批量执行一次删除。空 filter 表示全删：

```cpp
// flush.cpp —— 删除 Iceberg 数据湖中匹配的行
void IcebergDeltaDeleteFromLake(Oid foreign_relid, const char* filter) {
    // ... 解析 S3 位置、打开 Iceberg 表 ...
    iceberg_lite::IcebergTable iceberg_table =
        iceberg_lite::IcebergTable::Open(s3_client, table_path);

    if (filter != NULL && filter[0] != '\0') {
        iceberg_table.Delete(std::string(filter));   // 有 WHERE：按 filter 删
    } else {
        iceberg_table.Delete(std::string("True"));    // 空 WHERE：等价于删全表
    }
}
```

#### WHERE 支持范围

| 支持 | 不支持（报 `FEATURE_NOT_SUPPORTED`） |
|------|------|
| 比较运算符 `=, <>, <, <=, >, >=` | `IN`、`LIKE`、`BETWEEN` |
| 逻辑组合 `AND`、`OR` | 函数调用、子查询 |
| 字面量：整数/浮点/字符串 | 跨表 JOIN 条件（`DELETE FROM a USING b ...`） |

#### 失败处理

- **delta 删除失败** → 整语句 ERROR，Iceberg 未动，事务回滚。安全。
- **Iceberg delete 失败** → delta 已删但处于未提交事务内，整语句 ERROR 触发回滚，delta 行恢复。安全。极端情况下若 Iceberg 已部分提交 snapshot 再抛异常，可能产生"delta 已回滚但 Iceberg 已删"的短暂窗口——本版本接受此风险，后续可通过两阶段（先收集待删主键、验证后再删）加固。

#### 已知问题：浮点 filter 精度漏删与类型转换改进计划

##### 问题

当前 `AppendFilterConst`（`fdw_modify.cpp:76`）按 **PG 字面量类型**序列化 WHERE 条件中的常量值，不感知 Iceberg 列的实际存储类型。当列是 `REAL`（float4/单精度）时，`%f` 默认仅 6 位小数，加上 planner 会将字面量提升精度（如 `0.1` 被编码为 float8 const），导致 filter 值与 Iceberg 列中实际存储的单精度值不匹配 → 漏删。

实测验证（`test_delete_float_precision.sh`）：

```
flush 写入 Iceberg 的值: 0.10000000149011612  (float4 精确)
DELETE WHERE 序列化:      "0.100000"           (%f, 仅 6 位)
PyIceberg 解析:          0.1                   (与之不等)
结果: 漏删 — id=1 未被删除
```

##### 改进计划：PG → Iceberg 类型映射转换器

**核心思路**：字面量按 Iceberg 列的目标类型（而非 PG 字面量自身的类型）来序列化，并对数值做精度降级（如 double → float），确保 filter 中的值与 Iceberg 表里实际存储的值精确匹配。

**Step 1 — 定义 PG → Iceberg 类型映射**

在 `fdw_modify.cpp` 中新增枚举和映射函数（与 `flush.cpp::PgTypeToIcebergTypeKind` 一致但不依赖 iceberg-lite 头文件）：

```c
typedef enum { ICB_BOOLEAN, ICB_INT, ICB_LONG, ICB_FLOAT, ICB_DOUBLE,
               ICB_STRING, ICB_DATE, ICB_TIME, ICB_TIMESTAMP,
               ICB_TIMESTAMPTZ, ICB_BINARY, ICB_DECIMAL } IcebergType;

PgTypeToIcebergType(pg_type) → IcebergType
    // INT2/INT4 → ICB_INT, INT8 → ICB_LONG
    // FLOAT4 → ICB_FLOAT, FLOAT8 → ICB_DOUBLE
    // BOOL → ICB_BOOLEAN, TEXTOID/VARCHAR/... → ICB_STRING
    // DATE/TIME/TIMESTAMP/... → 对应时态类型
    // BYTEA → ICB_BINARY, NUMERIC → ICB_DECIMAL
```

**Step 2 — 修改 `AppendFilterExpr` 签名，增加列类型参数**

```
原: AppendFilterExpr(buf, node, root, targetRti, hasError)
新: AppendFilterExpr(buf, node, root, targetRti, hasError, column_pg_type)
    // column_pg_type: 当序列化为 "col OP const" 中 const 一侧时传入列 PG 类型
    //                 其他情况传 InvalidOid（无列上下文）
```

**Step 3 — 修改 `AppendFilterOpExpr`，识别 `col OP const` 模式**

在比较运算符处理中增加：
1. 检查 `left/right` 一侧为 `T_Var`、另一侧为 `T_Const`
2. 若是 → 通过 `get_atttype(rte->relid, var->varattno)` 获取列的 PG 类型
3. 序列化 Var 侧时传 `InvalidOid`（列引用不需要列类型），Const 侧时传列的 PG 类型

**Step 4 — 重写 `AppendFilterConst`，按 Iceberg 类型 + 精度降级序列化**

```
AppendFilterConst(buf, con, column_pg_type):
    1. NULL → "None"
    2. 有效 PG 类型 = column_pg_type ?? con->consttype  (列类型优先)
    3. Iceberg 类型 = PgTypeToIcebergType(有效 PG 类型)
    4. 按 Iceberg 类型分支:
```

| Iceberg 类型 | 序列化格式 | 精度降级 |
|---|---|---|
| `ICB_BOOLEAN` | `True` / `False` | — |
| `ICB_INT` (32-bit) | `%d` | float8→int4 降级 |
| `ICB_LONG` (64-bit) | `%ld` | 同上 |
| `ICB_FLOAT` (32-bit) | **`%.17g`** | **const 是 float8 时 `(float)DatumGetFloat8` 降至 float4** |
| `ICB_DOUBLE` (64-bit) | **`%.17g`** | float4→double 提升 |
| `ICB_STRING` | `'xxx'`（引号+单引号双写转义） | OidOutputFunctionCall |
| `ICB_DATE/TIME/TS/DECIMAL/BINARY` | 引号包围 PG output | OidOutputFunctionCall |

**精度降级核心逻辑**（解决浮点漏删的关键）：

```c
/* 列是 float4，但 planner 把 const 编码为 float8 */
case ICB_FLOAT: {
    float fval;
    if (con->consttype == FLOAT8OID && column_pg_type == FLOAT4OID)
        fval = (float)DatumGetFloat8(con->constvalue);  // double → float demotion
    else if (con->consttype == FLOAT4OID)
        fval = DatumGetFloat4(con->constvalue);
    // ...
    appendStringInfo(buf, "%.17g", (double)fval);   // 17 位保证 float4 往返不丢精度
}
```

**效果**：`DELETE WHERE score = 0.1`（列 float4，const float8）
- 降级: `(float)0.1` → `0.10000000149011612`
- 序列化: `%.17g` → `"0.10000000149011612"`
- PyIceberg 解析 → `0.10000000149011612`，与 Iceberg Float 列值精确匹配 → **不漏删**

**修改范围**：仅 `fdw_modify.cpp` 一个文件，不改任何头文件或 API。

**验证方式**：`test_delete_float_precision.sh`——建 float4 外表 → INSERT 0.1 → flush → DELETE WHERE score = 0.1 → pyiceberg 校验 id=1 是否被正确删除。

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
- **DELETE 仅在 `delete` 分支**：DELETE 功能（双删、WHERE 范围）见 [3.4](#34-删除delete)，主分支默认不含。

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
