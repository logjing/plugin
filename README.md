# iceberg_delta

openGauss 扩展插件，通过 FDW 机制实现 Iceberg 外表的 INSERT 截流和 Delta 内表自动管理，无需修改任何内核文件。

## 功能概述

当用户向 Iceberg 外表执行 `INSERT` 时，数据并不写入远端 Iceberg 存储，而是被截流写入一张本地 Delta 内表（USTORE）。Delta 内表的创建、映射、级联删除全部由 DDL hook 自动完成，用户无需手动操作。

```sql
CREATE EXTENSION iceberg_delta;

CREATE FOREIGN TABLE t_sales (
    id INTEGER,
    name TEXT,
    score FLOAT
) SERVER iceberg_server OPTIONS (warehouse '/data/iceberg', table_name 'sales');

-- INSERT 数据自动写入 Delta 内表，而非远端 Iceberg
INSERT INTO t_sales VALUES (1, 'alice', 95.5);

-- 通过 Delta 内表直接查询新写入的数据
SELECT * FROM iceberg_delta.delta_<oid>;

-- 删除外表时 Delta 内表自动级联删除
DROP FOREIGN TABLE t_sales;
```

**当前限制**：外表的 `SELECT` 查询走 placeholder scan 回调，代价极高且不返回有效数据。后续版本将实现两阶段合并扫描（lake + delta），使外表 `SELECT` 可看到完整数据。

## 实现路线

原 pg_lake_delta 项目通过侵入式修改 openGauss 内核实现 INSERT 截流，涉及 8+ 个内核文件（新增系统 catalog、枚举类型、FDW 白名单扩展、nodeModifyTable.cpp 截流逻辑、execMain.cpp FDW 校验绕过等），导致无法跟随上游版本升级，运维成本高。

本插件将所有功能以 openGauss 扩展形式实现，核心思路：

| 原侵入方案 | 插件方案如何替代 |
|---|---|
| 新增 `pg_delta_table` 系统 catalog | 普通用户表 `iceberg_delta.mapping` |
| 新增 `T_ICEBERG_SERVER` 枚举 | 不需要 — FDW handler 本身就是标识 |
| 新增 `ICEBERG_FDW` 宏/常量 | 内核已有，直接沿用 |
| `nodeModifyTable.cpp` 截流逻辑 | FDW `ExecForeignInsert` 回调天然就是截流点 |
| `execMain.cpp` FDW 校验绕过 | 不需要 — FDW 提供了 `ExecForeignInsert` 回调，校验自然通过 |
| `foreign.cpp` FDW 白名单扩展 | 不需要 — `iceberg_fdw` 已在白名单中 |

**设计参考**：og_iceberg 项目已验证单 FDW 方案在 openGauss 上的可行性，本插件沿用了其 DDL hook、delta_relid 缓存等关键设计模式。

## 整体架构

```
┌───────────────────────────────────────────────────────────────┐
│                      用户 SQL 层                              │
│                                                               │
│  CREATE FOREIGN TABLE t_sales (...) SERVER iceberg_server;   │
│    → DDL hook 自动创建 Delta 内表 + 映射 + 写入 options     │
│  INSERT INTO t_sales VALUES (...);                            │
│    → ExecForeignInsert 截流写入 Delta 内表                   │
│  DROP FOREIGN TABLE t_sales;                                  │
│    → 映射清理 + DEPENDENCY_AUTO 级联删除 Delta 内表          │
└───────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────────────────┐
│           openGauss 内核（白名单已有 iceberg_fdw）            │
│                                                               │
│  CheckSupportedFDWType: "iceberg_fdw" 在白名单中 ✓           │
│  InitPlan: ExecForeignInsert != NULL ✓                        │
│  ExecInsertT: ri_FdwRoutine → 调用 iceberg_fdw 回调          │
└───────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────────────────┐
│            iceberg_delta 插件（.so 动态库）                   │
│                                                               │
│  _PG_init(): 安装 ProcessUtility_hook                        │
│                                                               │
│  DDL Hook:                                                   │
│    ├─ CREATE FOREIGN TABLE → 自动创建 Delta 内表              │
│    │   + 映射记录 + DEPENDENCY_AUTO + delta_relid 写入 opts  │
│    └─ DROP FOREIGN TABLE → 清理映射记录                       │
│      (Delta 内表通过 DEPENDENCY_AUTO 级联删除)               │
│                                                               │
│  iceberg_fdw_handler() → 返回 FdwRoutine                     │
│    ├─ Insert 回调: Plan → Begin → Exec → EndForeignModify    │
│    ├─ IsForeignRelUpdatable: 支持 INSERT                     │
│    └─ Scan 回调: placeholder（后续版本实现）                  │
│                                                               │
│  映射表: iceberg_delta.mapping                               │
│    (foreign_relid, delta_relid, delta_schema, delta_name)    │
│  外表 options 缓存: delta_relid, delta_schema, delta_name    │
└───────────────────────────────────────────────────────────────┘
```

### INSERT 截流流程

```
INSERT INTO t_sales VALUES (1, 'alice', 95.5)
  → ExecInsertT() 检测到 ri_FdwRoutine 非空
  → 调用 ExecForeignInsert(estate, rinfo, slot, planSlot)
  → 插件回调中:
      1. heap_slot_getallattrs(slot)  — 提取所有属性值
      2. tableam_tslot_get_tuple_from_slot() — 转换为存储格式 tuple
      3. tableam_tuple_insert(delta_rel, tuple) — 写入 Delta 内表
  → 返回 slot（支持 RETURNING 子句）
```

### delta_relid 查找策略

优先用外表 options 中的 OID 缓存（零 SPI 查询），OID 失效时 fallback 到按 schema/name 查找（保证 dump/restore 后可用），并自动更新映射表中的 OID 缓存。

## 编译方法

### 前置条件

- openGauss 已编译（源码树或安装目录中有 `pg_config`）
- binarylibs 工具链（GCC 10.3）

### 编译

```bash
# 设置 PATH，优先使用 binarylibs 的 GCC 10.3 和 openGauss 的 pg_config
export PATH=/path/to/binarylibs/buildtools/gcc10.3/gcc/bin:/path/to/openGauss_install/bin:$PATH
export LD_LIBRARY_PATH=/path/to/binarylibs/buildtools/gcc10.3/gcc/lib64:$LD_LIBRARY_PATH

# 编译
cd iceberg_delta
make
```

如果 openGauss 的 `pg_config` 返回的安装路径不存在（常见于 pg_lake_delta 等二次构建场景），需要创建符号链接：

```bash
# 例如 pg_config 返回 /path/to/openGauss-server/mppdb_temp_install
# 但实际文件在 /path/to/pg_lake_delta/mppdb_temp_install
ln -sf /path/to/pg_lake_delta/mppdb_temp_install /path/to/openGauss-server/mppdb_temp_install
```

如果 openGauss 安装目录的头文件不完整（缺少 `access/htap/` 等子目录），需要在 Makefile 中添加源码树的 `src/include` 路径。当前 Makefile 已包含此配置：

```makefile
override CPPFLAGS += -I/home/sin/pg_lake_delta/src/include
```

使用其他环境时需将此路径改为对应的 openGauss 源码头文件目录。

### 安装

```bash
make install
```

### 部署

```bash
# 1. 在 postgresql.conf 中配置 shared_preload_libraries
#    DDL hook 必须在 postmaster 启动时全局安装
shared_preload_libraries = 'iceberg_delta'

# 2. 重启 openGauss
gs_ctl restart -D /path/to/datadir

# 3. 在目标数据库中安装扩展
gsql -d postgres -c "CREATE EXTENSION iceberg_delta;"
```

### 卸载

```sql
-- 先删除所有使用 iceberg_server 的外表
-- Delta 内表随外表级联删除（DEPENDENCY_AUTO）

DROP EXTENSION iceberg_delta CASCADE;
-- 自动删除：schema、映射表、FDW、server

-- 从 postgresql.conf 中移除 shared_preload_libraries
-- 重启 openGauss
```

---

## DELETE 实现计划（`delete` 分支）

### 目标

实现外表的 `DELETE FROM <外表> WHERE <条件>`，删除**已落湖（Iceberg）的数据**，并同步删除 delta 缓冲区中尚未 flush 的匹配行。当前插件只支持 INSERT（`fdw_handler.cpp` 的 `ExecForeignDelete = NULL`，`fdw_modify.cpp::PlanForeignModify` 对非 INSERT 直接报错）。

### 语义定义（已确认：理解 2 — delta + Iceberg 双删）

| 数据所在位置 | DELETE 处理方式 |
|---|---|
| delta 内表（未 flush 的缓冲行） | `tableam_tuple_delete` 按 ctid 删本地行 |
| 已 flush 进 Iceberg | 调用 SDK `IcebergTable::Delete(filter)` 下发原 WHERE |

delta 表本身就是"未 flush 数据"的权威来源（flush 末尾会清空 delta），因此扫描 delta 即可拿到所有未 flush 行；已 flush 行需通过 WHERE 过滤器对数据湖执行删除。

### 前置可行性结论（已完成调研）

1. **SDK 侧**：`iceberg-lite` 的数据层全部桥接 PyIceberg（`popen("python3 ...")`）。PyIceberg 0.11.1 原生支持 `tbl.delete(filter)`，且 filter 接受 SQL 字符串。已验证 `==`、`<`、`>`、`AND`、字符串字面量全部正确解析，每次 delete 生成 `operation=OVERWRITE` 的新 snapshot，不匹配条件仅抛 warning 不报错。SDK 侧新增 `IcebergTable::Delete(filter)` 的工作量约 0.5–1 天（复用 `Append` 的 catalog/register_table/popen 脚手架）。
2. **内核侧**：DELETE 路径**无需改内核**。`CheckSupportedFDWType` 白名单已含 `ICEBERG_FDW`；`CheckUnsupportInsertSelectClause` 仅断言 `CMD_INSERT`，不碰 DELETE。DELETE 的两道闸门都在 executor 初始化阶段、且都在插件代码可控范围内（见下文"闸门清单"）。

### 闸门清单（DELETE 走完整条路径的内核检查点）

| 检查点 | 位置 | 现状 | 改动 |
|---|---|---|---|
| 插件 Plan 阶段 | `fdw_modify.cpp:66` | 非 INSERT 直接 `ereport(ERROR)` | 放开 `CMD_DELETE`，走 DELETE plan 逻辑 |
| executor 闸门① | `execMain.cpp:1747` | `ExecForeignDelete == NULL` → 报 "cannot delete" | 实现 `ExecForeignDelete`（非 NULL） |
| executor 闸门② | `execMain.cpp:1751` | `IsForeignRelUpdatable` 不含 DELETE 位 → 报 "does not allow deletes" | 返回值加上 `1 << CMD_DELETE` |
| planner AddForeignUpdateTargets | `rewriteHandler.cpp:1737` | NULL（有 NULL 保护，不崩） | 实现以暴露 junk 列给 ExecForeignDelete |
| 实际执行 | `nodeModifyTable.cpp:1773` | 直接解引用 `ExecForeignDelete` | 无白名单校验，闸门①②放行后即调用 |

**关键边界**：`nodeModifyTable.cpp:1773` 的外表 DELETE 分支无任何白名单/类型校验，放行后所有 DELETE 都会进 `ExecForeignDelete`。实现必须健壮处理：全表删除、复杂 WHERE、空结果，不得假设只来单行。

### 设计决策

1. **不强制外表定义 PRIMARY KEY**：delta 侧按 ctid 删（行物理位置，不依赖主键）；Iceberg 侧用原 WHERE 字符串过滤器删。两侧删删除标识独立，无需主键。
2. **WHERE 下发策略 = 原表达式重建为 filter 字符串**：在 `PlanForeignModify` 从 `root->parse` 提取 WHERE 子句，遍历 `Expr` 树重建为 PyIceberg 认的 SQL 字符串，经 `fdw_private` 传到 `EndForeignModify`，调一次 `IcebergTable::Delete(filter)`。第一版支持范围见下。
3. **第一版 WHERE 支持范围**：比较操作符 `=, <>, <, <=, >, >=`、逻辑 `AND/OR`、字面量（整数/浮点/字符串/NULL）。超出范围（`IN, LIKE, BETWEEN, 函数调用, 子查询, 跨表 JOIN 条件`）一律 `ereport(ERROR)` 报"暂不支持"。

### 实现路线对比（已选定路线 A）

DELETE 的数据流存在一个核心矛盾：当前外表 scan 是 **placeholder**（`IcebergDeltaIterateForeignScan` 返回空 slot），标准 FDW 的 scan → `ExecForeignDelete` 逐行模型在 DELETE 时拿不到任何行，`ExecForeignDelete` 不会被调用。两条路线都能实现双删，但形态不同：

**路线 A —— 逐行模型 + 实现 delta scan（选定）**

```
DELETE FROM 外表 WHERE id=1
  → AddForeignUpdateTargets: 注入 delta ctid 作为 junk 列
  → BeginForeignScan: 打开 delta 表扫描器
  → IterateForeignScan: 返回 delta 行 + ctid   ← scan 从 placeholder 升级为真扫描
  → ExecForeignDelete: tableam_tuple_delete(delta_rel, ctid) 逐行删
  → EndForeignModify: IcebergTable::Delete(filter) 批量删已落湖数据
```

**路线 B —— 整语句拦截 + 绕过逐行**

```
DELETE FROM 外表 WHERE id=1
  → BeginForeignModify: 打开 delta_rel, 存 filter 到 state
  → ExecForeignDelete:  no-op（仅为过 executor 闸门①）
  → EndForeignModify: SPI 跑 DELETE FROM delta_xxx WHERE <条件>
                      + IcebergTable::Delete(filter)
```

| 维度 | 路线 A | 路线 B |
|---|---|---|
| 模型 | 标准 FDW 逐行，与 postgres_fdw / mot_fdw 一致 | 偏离标准，scan 与 DML 行为脱节 |
| scan | 升级为真 delta scan（本就属于 roadmap：未来 SELECT 合并扫描复用） | 保持 placeholder |
| ExecForeignDelete | 逐行按 ctid 删 delta，职责清晰 | no-op 空函数（为过闸门而存在，反直觉） |
| delta 删除 | `tableam_tuple_delete(ctid)`，走标准 executor 数据流 | SPI 嵌套执行 SQL，绕过 executor |
| 可读性 | 回调各司其职 | "为过闸门的空函数" + 隐藏在 EndForeignModify 的批量逻辑 |
| 工作量 | 较大（要实现 delta scan 四回调） | 较小 |

**结论：采用路线 A。** 理由：openGauss 内置 FDW（postgres_fdw、mot_fdw）全部是逐行模型，路线 A 与之结构一致，代码可读性最好、不堆屎山；delta scan 是项目 roadmap 本来就要实现的（README 功能概述已注明"后续版本将实现两阶段合并扫描"），属"正确方向上的投入"而非为 DELETE 临时强加。路线 B 的 `ExecForeignDelete` no-op 与 scan/DML 脱节属设计异味。

### 双删时序与一致性

```
DELETE FROM 外表 WHERE <条件>
  → PlanForeignModify: 提取 WHERE, 重建为 filter 字符串, 放入 fdw_private
                       (同时校验 WHERE 是否在支持范围内)
  → AddForeignUpdateTargets: 给外表 scan 暴露 delta ctid 作为 junk 列
  → BeginForeignModify: 打开 delta_rel
  → ExecForeignDelete(逐行): 不立即删 Iceberg, 仅按 ctid 删 delta 本地行
  → EndForeignModify: 批量调 IcebergTable::Delete(filter) 删已落湖数据
```

**时序选择：先删 delta，后删 Iceberg**。理由：
- delta 删除是本地事务，回滚成本低；Iceberg delete 涉及 S3 写入 + snapshot 提交，代价高。
- 若先删 Iceberg 成功、删 delta 失败 → delta 残留行下次 flush 会重新写回 Iceberg，等于删了又回来（数据不一致）。反之先删 delta 失败，Iceberg 尚未动，整体回滚即可。

**失败处理**：
- delta 删除失败 → 整个语句 ERROR，Iceberg 不动，事务回滚。安全。
- Iceberg delete 失败 → delta 已删（本地事务内尚未提交），整语句 ERROR 触发回滚，delta 行恢复。安全。但需注意：若 Iceberg delete 抛异常前已部分提交 snapshot，可能产生"delta 已回滚但 Iceberg 已删"的窗口——第一版接受此风险并在文档注明，后续可通过两阶段（先扫描 delta 收集要删的主键、验证后再删）加固。

### 工作分解与估时

| 工作项 | 估时 | 依赖 |
|---|---|---|
| SDK 新增 `IcebergTable::Delete(filter)` + filter JSON 转义 + 单测 | 1 天 | 无 |
| 插件 `IsForeignRelUpdatable` 加 DELETE 位 + `PlanForeignModify` 放开 CMD_DELETE | 0.5 天 | 无 |
| `AddForeignUpdateTargets` 实现（暴露 delta ctid junk 列） | 1 天 | 上一项 |
| `ExecForeignDelete`（按 ctid 删 delta 本地行） | 1 天 | 上一项 |
| WHERE `Expr` 树 → filter 字符串转换器（含范围校验与报错） | 2 天 | SDK Delete 可用 |
| `EndForeignModify` 批量下发 `IcebergTable::Delete(filter)` | 1 天 | 转换器 + SDK Delete |
| e2e 测试 + 与 flush 并发测试 | 1 天 | 全部 |
| **合计** | **7.5 天** | |

### 待确认 / 后续

- WHERE 转换器对 NULL 字面量的处理（PyIceberg `delete(str)` 对 `IS NULL` 的语法待验证）。
- 多外表 JOIN 的 DELETE（`DELETE FROM a USING b WHERE ...`）第一版不支持，明确报错。
- 长期：flush 与 delete 并发时，已 flush 行的删除可能产生 snapshot 竞争，第一版不做并发控制，文档注明单写者假设。
