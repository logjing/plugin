#include "postgres.h"
#include "fmgr.h"
#include "tcop/utility.h"

#include "iceberg_delta/ddl_hook.h"

PG_MODULE_MAGIC;

extern "C" void _PG_init(void);
extern "C" void _PG_fini(void);

ProcessUtility_hook_type prev_ProcessUtility = NULL;

static void IcebergDeltaProcessUtility(processutility_context* pucontext,
                                       DestReceiver* dest,
#ifdef PGXC
                                       bool sentToRemote,
#endif
                                       char* completionTag,
                                       ProcessUtilityContext context,
                                       bool isCTAS)
{
    IcebergDeltaDDLHook(pucontext, dest,
#ifdef PGXC
                        sentToRemote,
#endif
                        completionTag, context, isCTAS);
}

void _PG_init(void)
{
    prev_ProcessUtility = ProcessUtility_hook;
    ProcessUtility_hook = IcebergDeltaProcessUtility;
}

void _PG_fini(void)
{
    ProcessUtility_hook = prev_ProcessUtility;
}
