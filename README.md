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

### 环境变量配置

`iceberg_delta_flush` 在向 MinIO/S3 数据湖写数据时需要 S3 凭证。凭证来源优先级为（从高到低）：

1. **外表 options**（`CREATE FOREIGN TABLE ... OPTIONS (s3_endpoint '...', s3_access_key_id '...', s3_secret_access_key '...', s3_region '...')`）——优先级最高，单表覆盖。
2. **环境变量**——外表未指定 option 时的兜底来源。
3. **编译期默认值**——仅 `endpoint`（`http://127.0.0.1:19000`）和 `region`（`us-east-1`）有非敏感默认值。

为避免敏感凭证进入源码与仓库，**access key 与 secret access key 不再有硬编码默认值**；若外表 option 与环境变量均未提供，flush 将报错并提示缺失项。

在运行 openGauss 的环境（启动 `gs_ctl` 的 shell，或 systemd unit 的 `Environment=`）中设置以下环境变量：

| 环境变量 | 说明 | 默认值 |
|---|---|---|
| `ICEBERG_S3_ENDPOINT` | MinIO/S3 端点 URL | `http://127.0.0.1:19000` |
| `ICEBERG_S3_ACCESS_KEY_ID` | 访问密钥 ID | **无（必填）** |
| `ICEBERG_S3_SECRET_ACCESS_KEY` | 访问密钥 | **无（必填）** |
| `ICEBERG_S3_REGION` | region | `us-east-1` |

```bash
# 在启动 openGauss 的 shell 中设置（gs_ctl 继承这些变量）
export ICEBERG_S3_ENDPOINT="http://172.168.22.25:19000"
export ICEBERG_S3_ACCESS_KEY_ID="your-access-key"
export ICEBERG_S3_SECRET_ACCESS_KEY="your-secret-key"
export ICEBERG_S3_REGION="us-east-1"
export ICEBERG_S3_BUCKET="your-bucket"

gs_ctl restart -D /path/to/datadir
```

> 端到端测试脚本 `test_flush_e2e.sh` 同样读取上述环境变量（其中 `ICEBERG_S3_BUCKET` 用于指定 bucket）。access key / secret 未设置时脚本会立即退出并报错。建议将凭证写入仅当前用户可读的文件（如 `~/.iceberg_env`），通过 `source` 加载，避免明文出现在 shell 历史或进程列表中。

### 卸载

```sql
-- 先删除所有使用 iceberg_server 的外表
-- Delta 内表随外表级联删除（DEPENDENCY_AUTO）

DROP EXTENSION iceberg_delta CASCADE;
-- 自动删除：schema、映射表、FDW、server

-- 从 postgresql.conf 中移除 shared_preload_libraries
-- 重启 openGauss
```
