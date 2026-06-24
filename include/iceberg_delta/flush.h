#ifndef ICEBERG_DELTA_FLUSH_H
#define ICEBERG_DELTA_FLUSH_H

#include "postgres.h"
#include "fmgr.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Flush delta rows to the Iceberg data lake.
 *
 * Usage: SELECT iceberg_delta_flush('<foreign_table_name>'::regclass);
 *        SELECT iceberg_delta_flush(foreign_table_oid);
 *
 * Returns the number of rows flushed (written to Iceberg and deleted from
 * the delta table).  Returns 0 if the delta table is empty.
 *
 * The function is transactional: if the Iceberg commit fails, the PG
 * transaction rolls back, restoring the delta rows. */
extern Datum iceberg_delta_flush(PG_FUNCTION_ARGS);

/* Delete matching rows from the Iceberg data lake.
 *
 * Called from EndForeignModify when processing DELETE FROM <foreign table>.
 * filter is a PyIceberg-compatible SQL expression string (e.g. "id == 1").
 * The function opens the foreign table to extract S3 options, connects to
 * MinIO/S3, and calls IcebergTable::Delete(filter).
 *
 * This function is not a PG function — it is called internally from the
 * FDW modify path. Errors are raised via ereport. */
extern void IcebergDeltaDeleteFromLake(Oid foreign_relid, const char* filter);

#ifdef __cplusplus
}
#endif

#endif /* ICEBERG_DELTA_FLUSH_H */
