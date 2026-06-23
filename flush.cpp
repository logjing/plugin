/**
 * iceberg_delta flush — 将 delta 表数据刷写到 Iceberg 数据湖 (MinIO)
 *
 * 使用 iceberg-lite 作为 Iceberg 写入引擎，将 PostgreSQL 外表关联的
 * delta 表中的所有行写入 MinIO 上的 Iceberg 表，然后删除 delta 行。
 */

#include "postgres.h"
#include "fmgr.h"

#include "pgxc/locator.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "iceberg_delta/catalog.h"
#include "iceberg_delta/delta_table.h"
#include "iceberg_delta/fdw_storage_options.h"
#include "iceberg_delta/flush.h"

#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>

#include "iceberg_lite/s3_io.h"
#include "iceberg_lite/schema.h"
#include "iceberg_lite/table.h"
#include "iceberg_lite/types.h"

PG_FUNCTION_INFO_V1(iceberg_delta_flush);

/* ════════════════════════════════════════════════════════════════════
 * 常量
 * ════════════════════════════════════════════════════════════════════ */

/* MinIO/S3 配置来源优先级（从高到低）：
 *   1. 外表 options（s3_endpoint / s3_access_key_id / s3_secret_access_key / s3_region ...）
 *   2. 环境变量（ICEBERG_S3_ENDPOINT / ICEBERG_S3_ACCESS_KEY_ID / ...）
 *   3. 下方的编译期默认值
 *
 * access_key 与 secret_access_key 不再硬编码：若 options 与环境变量均未提供则报错，
 * 避免敏感凭证进入源码与仓库。endpoint/region 的默认值仅供本地开发，无敏感性。 */
#define DEFAULT_S3_ENDPOINT        "http://127.0.0.1:19000"
#define DEFAULT_S3_REGION          "us-east-1"
#define DEFAULT_S3_PATH_STYLE      true
#define DEFAULT_S3_USE_SSL         false

/* 凭证相关项的环境变量名 */
#define ENV_S3_ENDPOINT            "ICEBERG_S3_ENDPOINT"
#define ENV_S3_ACCESS_KEY_ID       "ICEBERG_S3_ACCESS_KEY_ID"
#define ENV_S3_SECRET_ACCESS_KEY   "ICEBERG_S3_SECRET_ACCESS_KEY"
#define ENV_S3_REGION              "ICEBERG_S3_REGION"

/* ════════════════════════════════════════════════════════════════════
 * 辅助函数：选项提取
 * ════════════════════════════════════════════════════════════════════ */

static const char* GetOptionString(List* options, const char* name)
{
    ListCell* lc;
    foreach (lc, options) {
        DefElem* def = (DefElem*)lfirst(lc);
        if (strcmp(def->defname, name) == 0)
            return defGetString(def);
    }
    return NULL;
}

/* 从环境变量读取字符串，未设置则返回 fallback。空字符串视为未设置。 */
static const char* GetEnvOrDefault(const char* env_name, const char* fallback)
{
    const char* val = getenv(env_name);
    if (val != NULL && val[0] != '\0')
        return val;
    return fallback;
}

/* 三段式凭证解析：options → 环境变量 → fallback。返回 NULL 表示三级均无。 */
static const char* ResolveS3Option(List* options, const char* opt_name,
                                   const char* env_name, const char* fallback)
{
    const char* from_opt = GetOptionString(options, opt_name);
    if (from_opt != NULL && from_opt[0] != '\0')
        return from_opt;
    const char* from_env = getenv(env_name);
    if (from_env != NULL && from_env[0] != '\0')
        return from_env;
    return fallback;
}

static Oid GetDeltaRelidFromOptions(Oid foreign_relid)
{
    /* 优先从外表选项读取 */
    ForeignTable* ft = GetForeignTable(foreign_relid);
    const char* delta_relid_str = GetOptionString(ft->options, ICEBERG_OPT_DELTA_RELID);
    if (delta_relid_str != NULL) {
        Oid oid = (Oid)strtoul(delta_relid_str, NULL, 10);
        if (OidIsValid(oid) && get_rel_relkind(oid) != '\0')
            return oid;

        /* OID 已失效，尝试通过 schema + name 查找 */
        const char* delta_schema = GetOptionString(ft->options, ICEBERG_OPT_DELTA_SCHEMA);
        const char* delta_name   = GetOptionString(ft->options, ICEBERG_OPT_DELTA_NAME);
        if (delta_schema != NULL && delta_name != NULL) {
            Oid ns_oid = get_namespace_oid(delta_schema, true);
            if (OidIsValid(ns_oid))
                return get_relname_relid(delta_name, ns_oid);
        }
        return InvalidOid;
    }

    /* 回退到 catalog 映射表 */
    return IcebergCatalogGetDeltaRelid(foreign_relid);
}

/* ════════════════════════════════════════════════════════════════════
 * 辅助函数：S3 位置解析
 * ════════════════════════════════════════════════════════════════════ */

/* 解析 s3://bucket/prefix/table → {bucket, table_path}
 * 例如 "s3://sin/iceberg_warehouse/default/test_table"
 *   → bucket="sin", table_path="iceberg_warehouse/default/test_table" */
static bool ParseS3Location(const char* location,
                             std::string& bucket,
                             std::string& table_path)
{
    if (location == NULL) return false;
    if (strncmp(location, "s3://", 5) != 0) return false;

    const char* rest = location + 5;   /* 跳过 "s3://" */
    const char* slash = strchr(rest, '/');
    if (slash == NULL) {
        bucket = rest;
        table_path = "";
        return true;
    }
    bucket.assign(rest, slash - rest);
    table_path = slash + 1;
    return true;
}

/* ════════════════════════════════════════════════════════════════════
 * 辅助函数：PG 类型 → Iceberg 类型
 * ════════════════════════════════════════════════════════════════════ */

static iceberg_lite::TypeKind PgTypeToIcebergTypeKind(Oid pg_type)
{
    switch (pg_type) {
        case BOOLOID:       return iceberg_lite::TypeKind::kBoolean;
        case INT2OID:
        case INT4OID:       return iceberg_lite::TypeKind::kInt;
        case INT8OID:       return iceberg_lite::TypeKind::kLong;
        case FLOAT4OID:     return iceberg_lite::TypeKind::kFloat;
        case FLOAT8OID:     return iceberg_lite::TypeKind::kDouble;
        case DATEOID:       return iceberg_lite::TypeKind::kDate;
        case TIMEOID:       return iceberg_lite::TypeKind::kTime;
        case TIMESTAMPOID:  return iceberg_lite::TypeKind::kTimestamp;
        case TIMESTAMPTZOID:return iceberg_lite::TypeKind::kTimestampTz;
        case BYTEAOID:      return iceberg_lite::TypeKind::kBinary;
        case NUMERICOID:    return iceberg_lite::TypeKind::kDecimal;
        default:            return iceberg_lite::TypeKind::kString;
    }
}

/* 从 TupleDesc 构建 iceberg_lite::Schema */
static iceberg_lite::Schema BuildIcebergSchemaFromTupleDesc(TupleDesc tupdesc)
{
    std::vector<iceberg_lite::NestedFieldPtr> fields;
    int field_id = 1;

    for (int i = 0; i < tupdesc->natts; i++) {
        Form_pg_attribute attr = &tupdesc->attrs[i];
        if (attr->attisdropped) continue;

        iceberg_lite::TypePtr type =
            iceberg_lite::TypeFromKind(PgTypeToIcebergTypeKind(attr->atttypid));

        auto field = std::make_shared<iceberg_lite::NestedField>(
            field_id++,
            NameStr(attr->attname),
            !attr->attnotnull,   /* required = NOT attnotnull */
            type
        );
        fields.push_back(field);
    }

    return iceberg_lite::Schema(fields, 0);
}

/* ════════════════════════════════════════════════════════════════════
 * 辅助函数：Datum → iceberg_lite::Value
 * ════════════════════════════════════════════════════════════════════ */

static iceberg_lite::Value DatumToIcebergValue(Datum datum, bool isnull, Oid pg_type)
{
    if (isnull)
        return nullptr;

    switch (pg_type) {
        case BOOLOID:
            return (bool)DatumGetBool(datum);

        case INT2OID:
            return (int32_t)DatumGetInt16(datum);

        case INT4OID:
            return DatumGetInt32(datum);

        case INT8OID:
            return DatumGetInt64(datum);

        case FLOAT4OID:
            return (float)DatumGetFloat4(datum);

        case FLOAT8OID:
            return DatumGetFloat8(datum);

        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID:
        case NAMEOID: {
            /* 直接提取文本 */
            text* txt = DatumGetTextPP(datum);
            char* str = TextDatumGetCString(datum);
            std::string result(str);
            pfree(str);
            return result;
        }

        case CHAROID: {
            char c = DatumGetChar(datum);
            return std::string(1, c);
        }

        default: {
            /* 通过类型的 output 函数转换为字符串 */
            Oid typOutput;
            bool typIsVarlena;
            getTypeOutputInfo(pg_type, &typOutput, &typIsVarlena);
            char* str = OidOutputFunctionCall(typOutput, datum);
            std::string result(str);
            pfree(str);
            return result;
        }
    }
}

/* ════════════════════════════════════════════════════════════════════
 * 辅助函数：读取 delta 表
 * ════════════════════════════════════════════════════════════════════ */

static int ReadDeltaTable(Relation delta_rel, TupleDesc tupdesc,
                           MemoryContext ctx,
                           std::vector<iceberg_lite::Record>& out_records,
                           ItemPointerData** out_ctids)
{
    int ncols = tupdesc->natts;
    int capacity = 1024;
    int nrows = 0;

    MemoryContext oldctx = MemoryContextSwitchTo(ctx);

    ItemPointerData* ctids = (ItemPointerData*)palloc(sizeof(ItemPointerData) * capacity);

    /* 构造列信息数组，避免反复解引用 */
    struct ColInfo {
        Oid  type_oid;
        bool attbyval;
        int  attlen;
        bool attisdropped;
    };
    ColInfo* cols = (ColInfo*)palloc(sizeof(ColInfo) * ncols);
    for (int i = 0; i < ncols; i++) {
        cols[i].type_oid     = tupdesc->attrs[i].atttypid;
        cols[i].attbyval     = tupdesc->attrs[i].attbyval;
        cols[i].attlen       = tupdesc->attrs[i].attlen;
        cols[i].attisdropped = tupdesc->attrs[i].attisdropped;
    }

    TupleTableSlot* slot = MakeSingleTupleTableSlot(tupdesc, false, delta_rel->rd_tam_ops);
    RangeScanInRedis range_scan = {false, 0, 0};
    TableScanDesc scan = tableam_scan_begin(delta_rel, GetActiveSnapshot(), 0, NULL, range_scan);

    Tuple tuple;
    while ((tuple = tableam_scan_getnexttuple(scan, ForwardScanDirection, NULL)) != NULL) {
        if (nrows >= capacity) {
            capacity *= 2;
            ctids = (ItemPointerData*)repalloc(ctids, sizeof(ItemPointerData) * capacity);
        }

        tableam_tslot_store_tuple(tuple, slot, InvalidBuffer, false, false);
        tableam_tslot_getallattrs(slot, false);

        ItemPointer tid = tableam_tops_get_t_self(delta_rel, tuple);
        ctids[nrows] = *tid;

        /* 构建 Record */
        iceberg_lite::Record rec;
        rec.values.reserve(ncols);

        for (int col = 0; col < ncols; col++) {
            if (cols[col].attisdropped) {
                rec.values.push_back(nullptr);
                continue;
            }

            Datum d = slot->tts_values[col];
            bool isnull = slot->tts_isnull[col];

            /* Detoast + 拷贝 pass-by-reference 值，确保在 flush_ctx 中存活 */
            Datum datum_copy;
            if (!isnull && !cols[col].attbyval) {
                datum_copy = datumCopy(d, cols[col].attbyval, cols[col].attlen);
            } else {
                datum_copy = d;
            }

            rec.values.push_back(DatumToIcebergValue(datum_copy, isnull, cols[col].type_oid));
        }

        out_records.push_back(std::move(rec));
        nrows++;
    }

    tableam_scan_end(scan);
    ExecDropSingleTupleTableSlot(slot);
    pfree(cols);

    MemoryContextSwitchTo(oldctx);

    *out_ctids = ctids;
    return nrows;
}

/* ════════════════════════════════════════════════════════════════════
 * 辅助函数：删除 delta 行
 * ════════════════════════════════════════════════════════════════════ */

static void DeleteDeltaRows(Relation delta_rel, ItemPointerData* ctids, int nrows)
{
    for (int i = 0; i < nrows; i++) {
        TM_FailureData tmfd;
        TM_Result result = tableam_tuple_delete(delta_rel, &ctids[i],
                                                GetCurrentCommandId(false),
                                                InvalidSnapshot, GetActiveSnapshot(),
                                                true, NULL, &tmfd, false);
        if (result != TM_Ok && result != TM_Deleted) {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("failed to delete flushed row from delta table")));
        }
    }

    /* Advance command counter so subsequent scans see the deletes. */
    CommandCounterIncrement();
}

/* ════════════════════════════════════════════════════════════════════
 * 辅助函数：S3 配置
 * ════════════════════════════════════════════════════════════════════ */

static iceberg_lite::S3Config BuildS3Config(List* options,
                                              const std::string& bucket)
{
    iceberg_lite::S3Config cfg;

    cfg.endpoint = std::string(
        ResolveS3Option(options, ICEBERG_OPT_S3_ENDPOINT, ENV_S3_ENDPOINT, DEFAULT_S3_ENDPOINT));

    /* access_key / secret_access_key 不留硬编码默认值：三级均无则报错 */
    const char* ak = ResolveS3Option(options, ICEBERG_OPT_S3_ACCESS_KEY_ID,
                                     ENV_S3_ACCESS_KEY_ID, NULL);
    if (ak == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                 errmsg("iceberg_delta: S3 access key not provided; set foreign table option "
                        "'s3_access_key_id' or environment variable ICEBERG_S3_ACCESS_KEY_ID")));
    }
    cfg.access_key = std::string(ak);

    const char* sk = ResolveS3Option(options, ICEBERG_OPT_S3_SECRET_ACCESS,
                                     ENV_S3_SECRET_ACCESS_KEY, NULL);
    if (sk == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                 errmsg("iceberg_delta: S3 secret access key not provided; set foreign table option "
                        "'s3_secret_access_key' or environment variable ICEBERG_S3_SECRET_ACCESS_KEY")));
    }
    cfg.secret_key = std::string(sk);

    cfg.region = std::string(
        ResolveS3Option(options, ICEBERG_OPT_S3_REGION, ENV_S3_REGION, DEFAULT_S3_REGION));

    cfg.bucket = bucket;

    const char* path_style = GetOptionString(options, ICEBERG_OPT_S3_PATH_STYLE);
    cfg.path_style = path_style ? (strcmp(path_style, "true") == 0) : DEFAULT_S3_PATH_STYLE;

    const char* ssl_enabled = GetOptionString(options, ICEBERG_OPT_S3_SSL_ENABLED);
    cfg.use_ssl = ssl_enabled ? (strcmp(ssl_enabled, "true") == 0) : DEFAULT_S3_USE_SSL;

    return cfg;
}

/* ════════════════════════════════════════════════════════════════════
 * 主函数：iceberg_delta_flush
 * ════════════════════════════════════════════════════════════════════ */

Datum iceberg_delta_flush(PG_FUNCTION_ARGS)
{
    Oid foreign_relid = PG_GETARG_OID(0);
    Relation foreign_rel = NULL;
    Relation delta_rel = NULL;
    Oid delta_relid = InvalidOid;

    ItemPointerData* ctids = NULL;
    int64_t total_flushed = 0;

    /* 创建专用的 flush 内存上下文 */
    MemoryContext oldctx;
    MemoryContext flush_ctx = AllocSetContextCreate(CurrentMemoryContext,
                                                     "iceberg_delta flush context",
                                                     ALLOCSET_DEFAULT_MINSIZE,
                                                     ALLOCSET_DEFAULT_INITSIZE,
                                                     ALLOCSET_DEFAULT_MAXSIZE);

    PG_TRY();
    {
        oldctx = MemoryContextSwitchTo(flush_ctx);

        /* ── 1. 打开外表，提取选项 ── */
        foreign_rel = relation_open(foreign_relid, AccessShareLock);
        ForeignTable* ft = GetForeignTable(foreign_relid);

        /* 解析 location 获取 warehouse + table_name */
        const char* location = GetOptionString(ft->options, ICEBERG_OPT_LOCATION);
        if (location == NULL) {
            /* 回退：warehouse + table_name */
            const char* warehouse = GetOptionString(ft->options, ICEBERG_OPT_WAREHOUSE);
            const char* table_name = GetOptionString(ft->options, ICEBERG_OPT_TABLE_NAME);
            if (warehouse == NULL || table_name == NULL) {
                ereport(ERROR,
                        (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                         errmsg("iceberg_delta foreign table requires 'location' option "
                                "or 'warehouse' + 'table_name' options")));
            }
            /* 仓库路径可能以 s3:// 开头，也可能不是 */
            StringInfoData locbuf;
            initStringInfo(&locbuf);
            if (strncmp(warehouse, "s3://", 5) == 0) {
                appendStringInfo(&locbuf, "%s/%s", warehouse, table_name);
            } else {
                /* 本地路径模式：构造 s3:// URL */
                ereport(ERROR,
                        (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                         errmsg("iceberg_delta flush requires an S3 location; "
                                "local warehouse paths are not yet supported")));
            }
            location = locbuf.data;
        }

        std::string bucket, table_path;
        if (!ParseS3Location(location, bucket, table_path) || bucket.empty()) {
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                     errmsg("invalid iceberg_delta location: \"%s\"; "
                            "expected s3://bucket/table_path format", location)));
        }

        /* ── 2. 获取 delta_relid ── */
        delta_relid = GetDeltaRelidFromOptions(foreign_relid);
        if (!OidIsValid(delta_relid)) {
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("iceberg_delta foreign table has no associated delta table")));
        }

        /* ── 3. 打开 delta 表，读取所有行 ── */
        delta_rel = relation_open(delta_relid, RowExclusiveLock);
        TupleDesc tupdesc = RelationGetDescr(delta_rel);

        std::vector<iceberg_lite::Record> records;
        int nrows = ReadDeltaTable(delta_rel, tupdesc, flush_ctx,
                                    records, &ctids);

        if (nrows == 0) {
            total_flushed = 0;
        } else {
            /* ── 4. 构建 S3 客户端 ── */
            iceberg_lite::S3Config s3_conf = BuildS3Config(ft->options, bucket);
            iceberg_lite::S3Client s3_client(s3_conf);

            /* ── 5. 打开或创建 Iceberg 表 ── */
            iceberg_lite::IcebergTable iceberg_table = [&]() {
                try {
                    return iceberg_lite::IcebergTable::Open(s3_client, table_path);
                } catch (const std::exception&) {
                    /* 首次 flush：用 delta 表的 schema 创建 Iceberg 表 */
                    iceberg_lite::Schema schema =
                        BuildIcebergSchemaFromTupleDesc(tupdesc);
                    return iceberg_lite::IcebergTable::Create(
                        s3_client, table_path, schema);
                }
            }();

            /* ── 6. 追加记录到 Iceberg ── */
            try {
                iceberg_table.Append(records);
            } catch (const std::exception& e) {
                ereport(ERROR,
                        (errcode(ERRCODE_FDW_ERROR),
                         errmsg("failed to append records to iceberg table: %s",
                                e.what())));
            }

            /* ── 7. Iceberg 提交成功后，删除 delta 行 ──
             * 如果后续 OpenGauss 事务回滚，delta 删除也会回滚，
             * Iceberg 的更改依赖 PyIceberg 的原子性。 */
            DeleteDeltaRows(delta_rel, ctids, nrows);

            total_flushed = nrows;
        }

        MemoryContextSwitchTo(oldctx);
    }
    PG_CATCH();
    {
        /* 清理资源 */
        if (delta_rel != NULL)
            relation_close(delta_rel, RowExclusiveLock);
        if (foreign_rel != NULL)
            relation_close(foreign_rel, AccessShareLock);
        MemoryContextDelete(flush_ctx);

        PG_RE_THROW();
    }
    PG_END_TRY();

    /* 正常清理路径 */
    if (delta_rel != NULL)
        relation_close(delta_rel, RowExclusiveLock);
    if (foreign_rel != NULL)
        relation_close(foreign_rel, AccessShareLock);
    MemoryContextDelete(flush_ctx);

    PG_RETURN_INT64(total_flushed);
}
