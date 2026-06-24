#include "postgres.h"
#include "fmgr.h"
#include "access/tableam.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "iceberg_delta/fdw_modify.h"
#include "iceberg_delta/fdw_modify_state.h"
#include "iceberg_delta/catalog.h"
#include "iceberg_delta/fdw_storage_options.h"

PG_FUNCTION_INFO_V1(iceberg_fdw_handler);

extern "C" Datum iceberg_fdw_handler(PG_FUNCTION_ARGS);

/* ──── delta scan state ──── */

typedef struct IcebergDeltaScanState {
    Relation    delta_rel;       /* opened delta table (AccessShareLock for scan) */
    TupleDesc   delta_tupdesc;   /* tuple descriptor of the delta relation */
    TableScanDesc delta_scan;    /* tableam scan descriptor */
    TupleTableSlot* delta_slot;  /* temporary slot with delta's descriptor */
    AttrNumber  ctid_attno;      /* position of "ctid" junk column in scan slot, or 0 */
} IcebergDeltaScanState;

/* ════════════════════════════════════════════════════════════════════
 * Planner callbacks
 * ════════════════════════════════════════════════════════════════════ */

static void IcebergDeltaGetForeignRelSize(PlannerInfo* root,
                                           RelOptInfo* baserel,
                                           Oid foreigntableid)
{
    /*
     * For DML operations, set a realistic row count so the planner creates
     * scan paths. For plain SELECT, rows=0 keeps the placeholder behaviour.
     */
    if (root->parse->commandType == CMD_DELETE ||
        root->parse->commandType == CMD_UPDATE) {
        Oid delta_relid = IcebergDeltaGetDeltaRelidFromOptions(foreigntableid);
        if (OidIsValid(delta_relid)) {
            Relation delta = relation_open(delta_relid, AccessShareLock);
            BlockNumber nblocks = RelationGetNumberOfBlocks(delta);
            baserel->rows = nblocks * 10;  /* rough estimate */
            baserel->pages = nblocks;
            relation_close(delta, AccessShareLock);
        } else {
            baserel->rows = 0;
        }
    } else {
        baserel->rows = 0;
    }
}

static void IcebergDeltaGetForeignPaths(PlannerInfo* root,
                                         RelOptInfo* baserel,
                                         Oid foreigntableid)
{
    add_path(root, baserel, (Path*)
        create_foreignscan_path(root, baserel,
                                baserel->rows > 0 ? baserel->rows : 100000000,
                                baserel->rows > 0 ? baserel->rows : 100000000,
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

/* ════════════════════════════════════════════════════════════════════
 * Executor scan callbacks — real delta table scan
 * ════════════════════════════════════════════════════════════════════ */

static void IcebergDeltaBeginForeignScan(ForeignScanState* node, int eflags)
{
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY) return;

    ForeignScan* fsplan = (ForeignScan*)node->ss.ps.plan;
    Oid foreigntableid = fsplan->scan.scanrelid;

    Oid delta_relid = IcebergDeltaGetDeltaRelidFromOptions(foreigntableid);
    if (!OidIsValid(delta_relid)) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("iceberg_fdw foreign table has no associated delta table for scan")));
    }

    IcebergDeltaScanState* scan_state =
        (IcebergDeltaScanState*)palloc0(sizeof(IcebergDeltaScanState));

    scan_state->delta_rel = relation_open(delta_relid, AccessShareLock);
    scan_state->delta_tupdesc = RelationGetDescr(scan_state->delta_rel);

    /* Start a tableam scan on the delta table */
    RangeScanInRedis range_scan = {false, 0, 0};
    scan_state->delta_scan = tableam_scan_begin(scan_state->delta_rel,
                                                 GetActiveSnapshot(), 0, NULL,
                                                 range_scan);

    /* Temp slot with delta's descriptor for deconstructing tuples */
    scan_state->delta_slot = MakeSingleTupleTableSlot(
        scan_state->delta_tupdesc, false,
        scan_state->delta_rel->rd_tam_ops);

    /*
     * Locate the "ctid" junk column position in the scan's output. This was
     * added by AddForeignUpdateTargets for DELETE plans. For SELECT plans
     * the attno will be InvalidAttrNumber (0).
     */
    scan_state->ctid_attno = ExecFindJunkAttributeInTlist(
        fsplan->scan.plan.targetlist, "ctid");

    node->fdw_state = (void*)scan_state;
}

static TupleTableSlot* IcebergDeltaIterateForeignScan(ForeignScanState* node)
{
    IcebergDeltaScanState* scan_state = (IcebergDeltaScanState*)node->fdw_state;
    TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;

    ExecClearTuple(slot);

    if (scan_state == NULL) {
        /* EXPLAIN only — no state, return empty */
        return NULL;
    }

    /* Get next row from the delta table */
    Tuple tuple = tableam_scan_getnexttuple(scan_state->delta_scan,
                                             ForwardScanDirection, NULL);
    if (tuple == NULL) {
        /* EOF — clear slot so executor sees TupIsNull */
        return NULL;
    }

    /* Store the delta tuple into the temp slot to deconstruct it */
    tableam_tslot_store_tuple(tuple, scan_state->delta_slot, InvalidBuffer,
                               false, false);
    tableam_tslot_getallattrs(scan_state->delta_slot, false);

    /*
     * Copy data columns from the temp slot into the scan slot. The scan
     * slot's descriptor (from fdw_scan_tlist) may have a subset of the
     * delta's columns plus a ctid junk column at the end. We fill the data
     * columns first, then manually set the ctid junk column if present.
     */
    int scan_natts = slot->tts_tupleDescriptor->natts;
    int delta_natts = scan_state->delta_tupdesc->natts;

    for (int i = 0; i < scan_natts; i++) {
        bool is_junk = (scan_state->ctid_attno > 0 &&
                        i == scan_state->ctid_attno - 1);
        if (is_junk) {
            /*
             * ctid junk column — fill with the delta tuple's physical ctid.
             * Stored as a pointer Datum so the executor can extract it via
             * ExecGetJunkAttribute/DatumGetPointer.
             */
            ItemPointer ctid = tableam_tops_get_t_self(scan_state->delta_rel,
                                                        tuple);
            ItemPointerData* ctid_copy =
                (ItemPointerData*)palloc(sizeof(ItemPointerData));
            *ctid_copy = *ctid;
            slot->tts_isnull[i] = false;
            slot->tts_values[i] = PointerGetDatum(ctid_copy);
        } else if (i < delta_natts) {
            /*
             * Data column — copy from the delta tuple. The column order of
             * the delta table matches the foreign table (same schema), so
             * the positions align.
             */
            slot->tts_values[i] = scan_state->delta_slot->tts_values[i];
            slot->tts_isnull[i] = scan_state->delta_slot->tts_isnull[i];
        } else {
            /* extra columns beyond delta's scope — shouldn't happen */
            slot->tts_isnull[i] = true;
        }
    }

    return slot;
}

static void IcebergDeltaReScanForeignScan(ForeignScanState* node)
{
    IcebergDeltaScanState* scan_state = (IcebergDeltaScanState*)node->fdw_state;
    if (scan_state == NULL || scan_state->delta_scan == NULL) return;

    tableam_scan_end(scan_state->delta_scan);

    RangeScanInRedis range_scan = {false, 0, 0};
    scan_state->delta_scan = tableam_scan_begin(scan_state->delta_rel,
                                                 GetActiveSnapshot(), 0, NULL,
                                                 range_scan);
}

static void IcebergDeltaEndForeignScan(ForeignScanState* node)
{
    IcebergDeltaScanState* scan_state = (IcebergDeltaScanState*)node->fdw_state;
    if (scan_state == NULL) return;

    if (scan_state->delta_scan != NULL) {
        tableam_scan_end(scan_state->delta_scan);
        scan_state->delta_scan = NULL;
    }
    if (scan_state->delta_slot != NULL) {
        ExecDropSingleTupleTableSlot(scan_state->delta_slot);
        scan_state->delta_slot = NULL;
    }
    if (scan_state->delta_rel != NULL) {
        relation_close(scan_state->delta_rel, AccessShareLock);
        scan_state->delta_rel = NULL;
    }

    node->fdw_state = NULL;
}

/* ════════════════════════════════════════════════════════════════════
 * FDW handler — register all callbacks
 * ════════════════════════════════════════════════════════════════════ */

Datum iceberg_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine* routine = makeNode(FdwRoutine);

    /* Modify callbacks */
    routine->IsForeignRelUpdatable = IcebergDeltaIsForeignRelUpdatable;
    routine->PlanForeignModify = IcebergDeltaPlanForeignModify;
    routine->BeginForeignModify = IcebergDeltaBeginForeignModify;
    routine->ExecForeignInsert = IcebergDeltaExecForeignInsert;
    routine->ExecForeignDelete = IcebergDeltaExecForeignDelete;
    routine->EndForeignModify = IcebergDeltaEndForeignModify;
    routine->AddForeignUpdateTargets = IcebergDeltaAddForeignUpdateTargets;

    /* Scan callbacks — real delta scan */
    routine->GetForeignRelSize = IcebergDeltaGetForeignRelSize;
    routine->GetForeignPaths = IcebergDeltaGetForeignPaths;
    routine->GetForeignPlan = IcebergDeltaGetForeignPlan;
    routine->BeginForeignScan = IcebergDeltaBeginForeignScan;
    routine->IterateForeignScan = IcebergDeltaIterateForeignScan;
    routine->ReScanForeignScan = IcebergDeltaReScanForeignScan;
    routine->EndForeignScan = IcebergDeltaEndForeignScan;

    routine->ExecForeignUpdate = NULL;

    PG_RETURN_POINTER(routine);
}
