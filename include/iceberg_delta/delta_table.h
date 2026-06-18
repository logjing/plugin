#ifndef ICEBERG_DELTA_DELTA_TABLE_H
#define ICEBERG_DELTA_DELTA_TABLE_H

#include "postgres.h"
#include "utils/rel.h"

Oid IcebergDeltaTableCreate(Oid foreign_relid, const char *schema_name,
                             const char *table_name, TupleDesc tupdesc);

#endif /* ICEBERG_DELTA_DELTA_TABLE_H */
