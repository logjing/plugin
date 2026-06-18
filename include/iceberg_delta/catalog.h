#ifndef ICEBERG_DELTA_CATALOG_H
#define ICEBERG_DELTA_CATALOG_H

#include "postgres.h"

Oid IcebergCatalogGetDeltaRelid(Oid foreign_relid);
bool IcebergCatalogInsertDeltaTableMapping(Oid foreign_relid, Oid delta_relid,
                                            const char *delta_schema,
                                            const char *delta_name);
bool IcebergCatalogDeleteDeltaTableMapping(Oid foreign_relid);
bool IcebergCatalogUpdateDeltaRelid(Oid foreign_relid, Oid new_delta_relid);

#endif /* ICEBERG_DELTA_CATALOG_H */
