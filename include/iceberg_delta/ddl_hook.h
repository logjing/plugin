#ifndef ICEBERG_DELTA_DDL_HOOK_H
#define ICEBERG_DELTA_DDL_HOOK_H

#include "postgres.h"
#include "tcop/utility.h"

extern ProcessUtility_hook_type prev_ProcessUtility;

void IcebergDeltaDDLHook(processutility_context *pucontext,
                         DestReceiver *dest,
#ifdef PGXC
                         bool sentToRemote,
#endif
                         char *completionTag,
                         ProcessUtilityContext context,
                         bool isCTAS);

#endif /* ICEBERG_DELTA_DDL_HOOK_H */
