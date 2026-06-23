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

#ifdef __cplusplus
}
#endif

#endif /* ICEBERG_DELTA_FLUSH_H */
