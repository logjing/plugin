#include "postgres.h"
#include "catalog/namespace.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "iceberg_delta/catalog.h"

Oid IcebergCatalogGetDeltaRelid(Oid foreign_relid)
{
    Oid delta_relid = InvalidOid;
    int rc = SPI_connect();
    if (rc != SPI_OK_CONNECT) return InvalidOid;

    const char* sql =
        "SELECT delta_relid, delta_schema, delta_name "
        "FROM iceberg_delta.mapping WHERE foreign_relid = $1";
    Oid argtypes[1] = {OIDOID};
    Datum values[1] = {ObjectIdGetDatum(foreign_relid)};

    rc = SPI_execute_with_args(sql, 1, argtypes, values, NULL, true, 1, NULL);
    if (rc == SPI_OK_SELECT && SPI_processed > 0 && SPI_tuptable != NULL) {
        HeapTuple tup = SPI_tuptable->vals[0];
        TupleDesc tupdesc = SPI_tuptable->tupdesc;

        bool isnull1 = false;
        Oid cached_oid = DatumGetObjectId(
            SPI_getbinval(tup, tupdesc, 1, &isnull1));

        if (!isnull1 && OidIsValid(cached_oid) &&
            get_rel_relkind(cached_oid) != '\0') {
            delta_relid = cached_oid;
        } else {
            bool isnull2 = false, isnull3 = false;
            char* delta_schema = TextDatumGetCString(
                SPI_getbinval(tup, tupdesc, 2, &isnull2));
            char* delta_name = TextDatumGetCString(
                SPI_getbinval(tup, tupdesc, 3, &isnull3));

            if (!isnull2 && !isnull3) {
                Oid namespace_oid = get_namespace_oid(delta_schema, true);
                if (OidIsValid(namespace_oid)) {
                    delta_relid = get_relname_relid(delta_name, namespace_oid);
                    if (OidIsValid(delta_relid)) {
                        IcebergCatalogUpdateDeltaRelid(foreign_relid, delta_relid);
                    }
                }
            }
            pfree(delta_schema);
            pfree(delta_name);
        }
    }

    SPI_finish();
    return delta_relid;
}

bool IcebergCatalogInsertDeltaTableMapping(Oid foreign_relid, Oid delta_relid,
                                            const char* delta_schema,
                                            const char* delta_name)
{
    int rc = SPI_connect();
    if (rc != SPI_OK_CONNECT) return false;

    const char* sql =
        "INSERT INTO iceberg_delta.mapping"
        "(foreign_relid, delta_relid, delta_schema, delta_name)"
        " VALUES ($1, $2, $3, $4)";
    Oid argtypes[4] = {OIDOID, OIDOID, TEXTOID, TEXTOID};
    Datum values[4] = {
        ObjectIdGetDatum(foreign_relid),
        ObjectIdGetDatum(delta_relid),
        CStringGetTextDatum(delta_schema),
        CStringGetTextDatum(delta_name),
    };

    rc = SPI_execute_with_args(sql, 4, argtypes, values, NULL, false, 0, NULL);
    bool ok = (rc == SPI_OK_INSERT);

    SPI_finish();
    return ok;
}

bool IcebergCatalogDeleteDeltaTableMapping(Oid foreign_relid)
{
    int rc = SPI_connect();
    if (rc != SPI_OK_CONNECT) return false;

    const char* sql =
        "DELETE FROM iceberg_delta.mapping WHERE foreign_relid = $1";
    Oid argtypes[1] = {OIDOID};
    Datum values[1] = {ObjectIdGetDatum(foreign_relid)};

    rc = SPI_execute_with_args(sql, 1, argtypes, values, NULL, false, 0, NULL);
    bool ok = (rc == SPI_OK_DELETE);

    SPI_finish();
    return ok;
}

bool IcebergCatalogUpdateDeltaRelid(Oid foreign_relid, Oid new_delta_relid)
{
    int rc = SPI_connect();
    if (rc != SPI_OK_CONNECT) return false;

    const char* sql =
        "UPDATE iceberg_delta.mapping SET delta_relid = $2 "
        "WHERE foreign_relid = $1";
    Oid argtypes[2] = {OIDOID, OIDOID};
    Datum values[2] = {
        ObjectIdGetDatum(foreign_relid),
        ObjectIdGetDatum(new_delta_relid),
    };

    rc = SPI_execute_with_args(sql, 2, argtypes, values, NULL, false, 0, NULL);
    bool ok = (rc == SPI_OK_UPDATE);

    SPI_finish();
    return ok;
}
