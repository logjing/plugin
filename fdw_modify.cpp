#include "postgres.h"
#include "fmgr.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "catalog/namespace.h"
#include "foreign/foreign.h"
#include "foreign/fdwapi.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "optimizer/planner.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "iceberg_delta/fdw_modify.h"
#include "iceberg_delta/fdw_modify_state.h"
#include "iceberg_delta/catalog.h"
#include "iceberg_delta/fdw_storage_options.h"

static Oid IcebergDeltaGetDeltaRelidFromOptions(Oid foreign_relid)
{
    ForeignTable* ft = GetForeignTable(foreign_relid);
    ListCell* lc;

    foreach (lc, ft->options) {
        DefElem* def = (DefElem*)lfirst(lc);
        if (strcmp(def->defname, ICEBERG_OPT_DELTA_RELID) == 0) {
            const char* value = defGetString(def);
            Oid oid = (Oid)strtoul(value, NULL, 10);
            if (OidIsValid(oid) && get_rel_relkind(oid) != '\0') {
                return oid;
            }
            const char* delta_schema = NULL;
            const char* delta_name = NULL;
            ListCell* lc2;
            foreach (lc2, ft->options) {
                DefElem* def2 = (DefElem*)lfirst(lc2);
                if (strcmp(def2->defname, ICEBERG_OPT_DELTA_SCHEMA) == 0)
                    delta_schema = defGetString(def2);
                if (strcmp(def2->defname, ICEBERG_OPT_DELTA_NAME) == 0)
                    delta_name = defGetString(def2);
            }
            if (delta_schema != NULL && delta_name != NULL) {
                Oid namespace_oid = get_namespace_oid(delta_schema, true);
                if (OidIsValid(namespace_oid)) {
                    return get_relname_relid(delta_name, namespace_oid);
                }
            }
            return InvalidOid;
        }
    }
    return IcebergCatalogGetDeltaRelid(foreign_relid);
}

int IcebergDeltaIsForeignRelUpdatable(Relation rel)
{
    return (1 << CMD_INSERT);
}

List* IcebergDeltaPlanForeignModify(PlannerInfo* root, ModifyTable* plan,
                                    Index resultRelation, int subplan_index)
{
    RangeTblEntry* rte = planner_rt_fetch(resultRelation, root);
    Oid delta_relid;

    if (root->parse->commandType != CMD_INSERT) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("iceberg_fdw foreign table only supports INSERT")));
    }

    delta_relid = IcebergDeltaGetDeltaRelidFromOptions(rte->relid);
    if (!OidIsValid(delta_relid)) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("iceberg_fdw foreign table has no associated delta table")));
    }

    return list_make1_oid(delta_relid);
}

void IcebergDeltaBeginForeignModify(ModifyTableState* mtstate,
                                    ResultRelInfo* rinfo, List* fdw_private,
                                    int subplan_index, int eflags)
{
    IcebergDeltaFdwModifyState* modify_state;
    Oid delta_relid;

    if (eflags & EXEC_FLAG_EXPLAIN_ONLY) return;

    if (fdw_private == NIL) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("iceberg_fdw foreign table has no delta relid in fdw_private")));
    }
    delta_relid = linitial_oid(fdw_private);

    if (!OidIsValid(delta_relid)) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("iceberg_fdw foreign table has no associated delta table")));
    }

    modify_state = (IcebergDeltaFdwModifyState*)palloc0(sizeof(IcebergDeltaFdwModifyState));
    modify_state->delta_rel = relation_open(delta_relid, RowExclusiveLock);
    modify_state->delta_tupdesc = RelationGetDescr(modify_state->delta_rel);

    rinfo->ri_FdwState = modify_state;
}

TupleTableSlot* IcebergDeltaExecForeignInsert(EState* estate,
                                               ResultRelInfo* rinfo,
                                               TupleTableSlot* slot,
                                               TupleTableSlot* planSlot)
{
    IcebergDeltaFdwModifyState* modify_state =
        (IcebergDeltaFdwModifyState*)rinfo->ri_FdwState;
    Tuple tuple;

    if (modify_state == NULL || modify_state->delta_rel == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("iceberg_fdw foreign table modify state is not initialized")));
    }

    heap_slot_getallattrs(slot, false);
    tuple = tableam_tslot_get_tuple_from_slot(modify_state->delta_rel, slot);
    if (tuple == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("failed to form tuple for iceberg delta table")));
    }

    (void)tableam_tuple_insert(modify_state->delta_rel, tuple,
                               estate->es_output_cid, 0, NULL);

    return slot;
}

void IcebergDeltaEndForeignModify(EState* estate, ResultRelInfo* rinfo)
{
    IcebergDeltaFdwModifyState* modify_state =
        (IcebergDeltaFdwModifyState*)rinfo->ri_FdwState;

    if (modify_state == NULL) return;

    if (modify_state->delta_rel != NULL) {
        relation_close(modify_state->delta_rel, RowExclusiveLock);
        modify_state->delta_rel = NULL;
    }

    rinfo->ri_FdwState = NULL;
}
