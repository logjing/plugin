#include "postgres.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "iceberg_delta/delta_table.h"

Oid IcebergDeltaTableCreate(Oid foreign_relid, const char* schema_name,
                             const char* table_name, TupleDesc tupdesc)
{
    int rc = SPI_connect();
    if (rc != SPI_OK_CONNECT) {
        return InvalidOid;
    }

    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE TABLE %s.%s (",
                     quote_identifier(schema_name),
                     quote_identifier(table_name));

    bool first = true;
    for (int i = 0; i < tupdesc->natts; i++) {
        Form_pg_attribute attr = &tupdesc->attrs[i];
        if (attr->attisdropped) continue;
        if (!first) appendStringInfoString(&buf, ", ");
        first = false;
        appendStringInfo(&buf, "%s %s",
                         quote_identifier(NameStr(attr->attname)),
                         format_type_be(attr->atttypid));
    }
    appendStringInfoChar(&buf, ')');
    appendStringInfoString(&buf, " WITH (STORAGE_TYPE = USTORE)");

    rc = SPI_execute(buf.data, false, 0);
    if (rc != SPI_OK_UTILITY) {
        SPI_finish();
        return InvalidOid;
    }

    Oid namespace_oid = get_namespace_oid(schema_name, false);
    Oid delta_relid = get_relname_relid(table_name, namespace_oid);

    SPI_finish();
    return delta_relid;
}
