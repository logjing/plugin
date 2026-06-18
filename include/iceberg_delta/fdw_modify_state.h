#ifndef ICEBERG_DELTA_FDW_MODIFY_STATE_H
#define ICEBERG_DELTA_FDW_MODIFY_STATE_H

#include "postgres.h"
#include "utils/rel.h"

typedef struct IcebergDeltaFdwModifyState {
    Relation    delta_rel;      /* delta heap relation opened with RowExclusiveLock */
    TupleDesc   delta_tupdesc;  /* tuple descriptor of the delta relation */
} IcebergDeltaFdwModifyState;

#endif /* ICEBERG_DELTA_FDW_MODIFY_STATE_H */
