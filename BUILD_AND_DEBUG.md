# iceberg_delta 插件编译与调试文档

## 环境信息

| 项目 | 详情 |
|------|------|
| 操作系统 | openEuler 24.03 x86_64 |
| 系统 GCC | 12.3.1 |
| 编译用 GCC | 10.3.0 (来自 binarylibs) |
| openGauss 版本 | 7.0.0-RC2 |
| 安装目录 | `/home/sjl/openGauss-server/mppdb_temp_install` |
| 数据目录 | `/home/sjl/ogdata` |
| 数据库端口 | 9876 |
| 插件目录 | `/home/sjl/plugin` |

---

## 一、环境变量更改

编辑 `~/.bashrc`，新增以下内容：

```bash
# openGauss environment
export GAUSSHOME=/home/sjl/openGauss-server/mppdb_temp_install
export LD_LIBRARY_PATH=$GAUSSHOME/lib:/home/sjl/binarylibs/buildtools/gcc10.3/gcc/lib64:/home/sjl/binarylibs/buildtools/gcc10.3/isl/lib:/home/sjl/binarylibs/buildtools/gcc10.3/mpc/lib:/home/sjl/binarylibs/buildtools/gcc10.3/mpfr/lib:/home/sjl/binarylibs/buildtools/gcc10.3/gmp/lib:$LD_LIBRARY_PATH
export PATH=$GAUSSHOME/bin:$PATH
```

**关键点**：

- `GAUSSHOME`：openGauss 安装根目录，`gs_ctl`/`gsql`/`gs_initdb` 等工具均从此处查找。**运行时也必须设置此变量**，否则 gaussdb 启动报 `Get environment of GAUSSHOME failed`。
- `LD_LIBRARY_PATH`：必须同时包含：
  - `$GAUSSHOME/lib`：openGauss 运行库（libgaussdb.so 等）
  - `gcc/lib64`：GCC 10.3 的 libstdc++
  - `isl/lib`、`mpc/lib`、`mpfr/lib`、`gmp/lib`：GCC 10.3 编译工具链依赖库（libisl.so.15 等）
- 缺失任一路径都会导致编译或运行失败。

---

## 二、编译过程中遇到的问题及修复

### 问题 1：Makefile 路径硬编码为 `/home/sin/`

**现象**：Makefile 中 CC、CXX、PG_CONFIG、CPPFLAGS 均指向 `/home/sin/` 路径。

**修复**：修改 `~/plugin/Makefile`，将所有路径改为当前环境：

```makefile
CC = /home/sjl/binarylibs/buildtools/gcc10.3/gcc/bin/g++
CXX = /home/sjl/binarylibs/buildtools/gcc10.3/gcc/bin/g++

PG_CONFIG = /home/sjl/openGauss-server/mppdb_temp_install/bin/pg_config

override CPPFLAGS += -I/home/sjl/openGauss-server/src/include
```

---

### 问题 2：GCC 10.3 编译工具链缺少动态库路径

**现象**：编译时报错 `libisl.so.15: cannot open shared object file: No such file or directory`

**根因**：GCC 10.3 的 cc1plus 运行时依赖 isl/mpc/mpfr/gmp 等库，但 `LD_LIBRARY_PATH` 只包含了 `gcc/lib64`，未包含其他依赖库路径。

**修复**：将 isl/lib、mpc/lib、mpfr/lib、gmp/lib 全部加入 `LD_LIBRARY_PATH`（见上文环境变量更改）。

**注意**：openGauss 自身的 `build/script/utils/common.sh` 中完整设置了这些路径，编译 openGauss 内核时不会遇到此问题。但编译外部插件时需要手动设置。

---

### 问题 3：ALTER FOREIGN TABLE 语法不兼容

**现象**：
```
ERROR: "t_sales" is of the wrong type
CONTEXT: SQL statement "ALTER FOREIGN TABLE public.t_sales SET (delta_relid = '...')"
```

**根因**：插件原代码使用 PostgreSQL 语法 `ALTER FOREIGN TABLE ... SET (option = 'value')`，但 openGauss 的语法是 `ALTER FOREIGN TABLE ... OPTIONS (ADD option 'value')`。

**修复**：修改 `ddl_hook.cpp` 中 `IcebergDeltaUpdateForeignTableOptions` 函数的 SQL 语句：

```cpp
// 原代码（PostgreSQL 语法）
"ALTER FOREIGN TABLE %s.%s SET (delta_relid = '%s', "
"delta_schema = '%s', delta_name = '%s')"

// 修改后（openGauss 语法）
"ALTER FOREIGN TABLE %s.%s OPTIONS (ADD delta_relid '%s', "
"ADD delta_schema '%s', ADD delta_name '%s')"
```

**注意**：使用 `ADD` 而非 `SET`，因为 delta_relid/delta_schema/delta_name 不是 CREATE FOREIGN TABLE 时的原有 option，`SET` 只能修改已存在的 option。

---

### 问题 4：SPI ALTER FOREIGN TABLE 锁冲突

**现象**：
```
ERROR: cannot ALTER TABLE "t_sales" because it is being used by active queries in this session
```

**根因**：`IcebergDeltaHandleCreateForeignTable` 中先 `relation_open(AccessShareLock)` 获取 tupdesc，然后通过 SPI 执行 `ALTER FOREIGN TABLE`（需要 AccessExclusiveLock），但 relation 锁在 SPI 执行时仍未释放。

**修复**：在用完 tupdesc（即 `IcebergDeltaTableCreate` 调用完成）后立即 `relation_close`，再执行 SPI ALTER：

```cpp
// 修复前：relation_close 在 IcebergDeltaUpdateForeignTableOptions 之后
Relation rel = relation_open(foreign_relid, AccessShareLock);
TupleDesc tupdesc = RelationGetDescr(rel);
// ... 创建 delta 表、插入映射 ...
IcebergDeltaUpdateForeignTableOptions(...);  // SPI ALTER
relation_close(rel, AccessShareLock);        // ← 太晚了，锁冲突

// 修复后：提前释放 relation 锁
Relation rel = relation_open(foreign_relid, AccessShareLock);
TupleDesc tupdesc = RelationGetDescr(rel);
Oid delta_relid = IcebergDeltaTableCreate(..., tupdesc);
relation_close(rel, AccessShareLock);        // ← 尽早释放
// ... 插入映射 ...
IcebergDeltaUpdateForeignTableOptions(...);  // SPI ALTER 不再冲突
```

---

### 问题 5：`iceberg_fdw` 不在内核 FDW 白名单中

**现象**：
```
ERROR: Un-support feature
DETAIL: insert statement is an INSERT INTO VALUES(...)
```

**根因**：openGauss 内核在 `CheckUnsupportInsertSelectClause` 中对非白名单 FDW 的 INSERT 做了限制。`CheckSupportedFDWType` 函数中的白名单只有 `MOT_FDW, MYSQL_FDW, ORACLE_FDW, POSTGRES_FDW`，不含 `iceberg_fdw`。

**修复**：需修改两个内核文件后重新编译 openGauss。

**文件 1**：`src/include/postgres.h` — 添加宏定义：

```c
#ifndef ICEBERG_FDW
#define ICEBERG_FDW "iceberg_fdw"
#endif
```

插入位置：在 `MOT_FDW_SERVER` 定义之后、`DFS_FDW` 之前。

**文件 2**：`src/gausskernel/cbb/extension/foreign/foreign.cpp` — 加入白名单：

```c
// 修改前
static const char* supportFDWType[] = {MOT_FDW, MYSQL_FDW, ORACLE_FDW, POSTGRES_FDW};

// 修改后
static const char* supportFDWType[] = {MOT_FDW, MYSQL_FDW, ORACLE_FDW, POSTGRES_FDW, ICEBERG_FDW};
```

修改后需重新编译 openGauss 内核：
```bash
cd openGauss-server
./build.sh -m release -3rd /path/to/binarylibs
```

**注意**：内核重新编译后 `make install` 会清空 `lib/postgresql/` 目录下的插件 .so 文件，需重新编译安装插件。

---

### 问题 6：DROP FOREIGN TABLE 时序错误

**现象**：
```
ERROR: relation "t_sales" does not exist
```
使用 `DROP FOREIGN TABLE t_sales`（无 IF EXISTS）时触发，使用 `DROP FOREIGN TABLE IF EXISTS` 可避免报错但映射表无法清理。

**根因**：DDL hook 中先调用 `CallNextUtility` 执行 DROP（此时表被删除），然后再调用 `IcebergDeltaHandleDropForeignTable` 清理映射，后者通过 `RangeVarGetRelid(..., missing_ok=false)` 查找已被删除的表从而报错。

**修复**：将映射清理操作移到 `CallNextUtility` 之前：

```cpp
// 修复前
if (has_iceberg) {
    CallNextUtility(...);                    // 先删除表
    IcebergDeltaHandleDropForeignTable(stmt); // 再清理映射 → 报错
    return;
}

// 修复后
if (has_iceberg) {
    IcebergDeltaHandleDropForeignTable(stmt); // 先清理映射
    CallNextUtility(...);                    // 再删除表
    return;
}
```

---

### 问题 7：NOT FENCED 函数需要 proc_srclib 目录

**现象**：
```
ERROR: Copy file "$libdir/proc_srclib/iceberg_delta" failed: No such file or directory
```

**根因**：openGauss 的 `NOT FENCED` 函数（在 gaussdb 进程内运行的 C 函数）需要将 .so 文件复制到 `$libdir/proc_srclib/` 目录下加载。

**修复**：创建符号链接：
```bash
mkdir -p $GAUSSHOME/lib/postgresql/proc_srclib
ln -sf $GAUSSHOME/lib/postgresql/iceberg_delta.so $GAUSSHOME/lib/postgresql/proc_srclib/iceberg_delta
```

每次 `make install` 或内核重新编译后都需要重新执行此步骤。

---

## 三、编译流程总结

```bash
# 1. 编译 openGauss 内核（含白名单修改）
cd ~/openGauss-server
./build.sh -m release -3rd /home/sjl/binarylibs

# 2. 编译插件
cd ~/plugin
make clean && make && make install

# 3. 安装 NOT FENCED 函数库
mkdir -p $GAUSSHOME/lib/postgresql/proc_srclib
ln -sf $GAUSSHOME/lib/postgresql/iceberg_delta.so \
       $GAUSSHOME/lib/postgresql/proc_srclib/iceberg_delta

# 4. 配置并重启数据库
# 在 postgresql.conf 中设置：
#   shared_preload_libraries = 'iceberg_delta'
gs_ctl restart -D /home/sjl/ogdata -Z single_node
```

---

## 四、功能验证结果

| 功能 | 状态 | 说明 |
|------|------|------|
| CREATE FOREIGN TABLE | ✅ | DDL hook 自动创建 USTORE Delta 内表，写入映射表 `iceberg_delta.mapping`，设置 DEPENDENCY_AUTO |
| INSERT INTO 外表 | ✅ | `ExecForeignInsert` 截流，数据写入 Delta 内表而非远端 Iceberg |
| Delta 内表查询 | ✅ | `SELECT * FROM iceberg_delta.delta_<oid>` 可查到所有截流数据 |
| DROP FOREIGN TABLE | ✅ | 映射记录自动清理，Delta 内表通过 DEPENDENCY_AUTO 级联删除 |

### 测试 SQL 示例

```sql
CREATE FOREIGN TABLE t_sales (id INTEGER, name TEXT, score FLOAT)
    SERVER iceberg_server OPTIONS (warehouse '/tmp/iceberg', table_name 'sales');

INSERT INTO t_sales VALUES (1, 'alice', 95.5);

SELECT * FROM iceberg_delta.mapping;          -- 查看映射
SELECT * FROM iceberg_delta.delta_<oid>;      -- 查看截流数据

DROP FOREIGN TABLE t_sales;                   -- 外表删除，delta 表级联清理
```

---

## 五、注意事项

1. **编译工具链**：始终使用 binarylibs 自带的 GCC 10.3，不要混用系统 GCC 12.3.1。`-D_GLIBCXX_USE_CXX11_ABI=0` 由 PGXS Makefile 自动添加，不要改动。

2. **内核重新编译后**：`make install` 会覆盖 `lib/postgresql/` 目录，必须重新执行插件 `make install` 和 `proc_srclib` 符号链接。

3. **运行 gaussdb 时**：`GAUSSHOME` 和 `LD_LIBRARY_PATH` 必须设置，否则启动时 core dump。

4. **CREATE EXTENSION**：当前版本 `CREATE EXTENSION iceberg_delta` 在 openGauss 7.0.0-RC2 上有 schema 成员检查问题，可改为手动执行 `iceberg_delta--1.0.sql` 中的 SQL 语句来安装扩展对象。

5. **多用户环境**：端口 5432/5433/15432 等已被其他实例占用，本实例使用端口 9876。
