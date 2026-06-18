#include "postgres.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "executor/executor.h"

#include "iceberg_delta/fdw_modify.h"

PG_FUNCTION_INFO_V1(iceberg_fdw_handler);

extern "C" Datum iceberg_fdw_handler(PG_FUNCTION_ARGS);

/* Scan placeholder callbacks */
static void IcebergDeltaGetForeignRelSize(PlannerInfo* root,
                                           RelOptInfo* baserel,
                                           Oid foreigntableid)
{
    baserel->rows = 0;
}

static void IcebergDeltaGetForeignPaths(PlannerInfo* root,
                                         RelOptInfo* baserel,
                                         Oid foreigntableid)
{
    add_path(root, baserel, (Path*)
        create_foreignscan_path(root, baserel,
                                100000000,
                                100000000,
                                NIL,
                                NULL,
                                NULL,
                                NIL,
                                1));
}

static ForeignScan* IcebergDeltaGetForeignPlan(PlannerInfo* root, RelOptInfo* baserel,
                                                Oid foreigntableid, ForeignPath* best_path,
                                                List* tlist, List* scan_clauses,
                                                Plan* outer_plan)
{
    scan_clauses = extract_actual_clauses(scan_clauses, false);
    return make_foreignscan(tlist, scan_clauses, baserel->relid, NIL, NIL,
                            NIL, NIL, outer_plan, EXEC_ON_ALL_NODES);
}

static void IcebergDeltaBeginForeignScan(ForeignScanState* node, int eflags) {}

static TupleTableSlot* IcebergDeltaIterateForeignScan(ForeignScanState* node)
{
    return ExecClearTuple(node->ss.ss_ScanTupleSlot);
}

static void IcebergDeltaReScanForeignScan(ForeignScanState* node) {}
static void IcebergDeltaEndForeignScan(ForeignScanState* node) {}

Datum iceberg_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine* routine = makeNode(FdwRoutine);

    routine->IsForeignRelUpdatable = IcebergDeltaIsForeignRelUpdatable;
    routine->PlanForeignModify = IcebergDeltaPlanForeignModify;
    routine->BeginForeignModify = IcebergDeltaBeginForeignModify;
    routine->ExecForeignInsert = IcebergDeltaExecForeignInsert;
    routine->EndForeignModify = IcebergDeltaEndForeignModify;

    routine->GetForeignRelSize = IcebergDeltaGetForeignRelSize;
    routine->GetForeignPaths = IcebergDeltaGetForeignPaths;
    routine->GetForeignPlan = IcebergDeltaGetForeignPlan;
    routine->BeginForeignScan = IcebergDeltaBeginForeignScan;
    routine->IterateForeignScan = IcebergDeltaIterateForeignScan;
    routine->ReScanForeignScan = IcebergDeltaReScanForeignScan;
    routine->EndForeignScan = IcebergDeltaEndForeignScan;

    routine->AddForeignUpdateTargets = NULL;
    routine->ExecForeignUpdate = NULL;
    routine->ExecForeignDelete = NULL;

    PG_RETURN_POINTER(routine);
}
