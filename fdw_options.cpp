#include "postgres.h"
#include "fmgr.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "foreign/foreign.h"
#include "foreign/fdwapi.h"
#include "utils/builtins.h"

#include "iceberg_delta/fdw_storage_options.h"

PG_FUNCTION_INFO_V1(iceberg_fdw_validator);

extern "C" Datum iceberg_fdw_validator(PG_FUNCTION_ARGS);

Datum iceberg_fdw_validator(PG_FUNCTION_ARGS)
{
    List* options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid catalog = PG_GETARG_OID(1);

    if (catalog == ForeignDataWrapperRelationId ||
        catalog == ForeignServerRelationId) {
        PG_RETURN_VOID();
    }

    if (catalog != ForeignTableRelationId) {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                 errmsg("iceberg_fdw options are only supported for foreign tables")));
    }

    ListCell* lc;

    foreach (lc, options_list) {
        DefElem* def = (DefElem*)lfirst(lc);
        const char* name = def->defname;

        if (strcmp(name, ICEBERG_OPT_LOCATION) == 0 ||
            strcmp(name, ICEBERG_OPT_WAREHOUSE) == 0 ||
            strcmp(name, ICEBERG_OPT_TABLE_NAME) == 0 ||
            strcmp(name, ICEBERG_OPT_FOLDERNAME) == 0) {
            /* Valid user options */
        } else if (strcmp(name, ICEBERG_OPT_DELTA_RELID) == 0 ||
                   strcmp(name, ICEBERG_OPT_DELTA_SCHEMA) == 0 ||
                   strcmp(name, ICEBERG_OPT_DELTA_NAME) == 0) {
            /* Internal options: written by DDL hook, allowed but not validated */
        } else if (strcmp(name, ICEBERG_OPT_S3_ENDPOINT) == 0 ||
                   strcmp(name, ICEBERG_OPT_S3_ACCESS_KEY_ID) == 0 ||
                   strcmp(name, ICEBERG_OPT_S3_SECRET_ACCESS) == 0 ||
                   strcmp(name, ICEBERG_OPT_S3_REGION) == 0 ||
                   strcmp(name, ICEBERG_OPT_S3_PATH_STYLE) == 0 ||
                   strcmp(name, ICEBERG_OPT_S3_SSL_ENABLED) == 0) {
            /* S3 options (future) */
        } else {
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                     errmsg("unrecognized option for iceberg_fdw foreign table: \"%s\"", name)));
        }
    }

    PG_RETURN_VOID();
}
