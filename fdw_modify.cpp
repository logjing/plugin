#include "postgres.h"
#include "fmgr.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "catalog/namespace.h"
#include "catalog/pg_operator.h"
#include "foreign/foreign.h"
#include "foreign/fdwapi.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "nodes/primnodes.h"
#include "nodes/makefuncs.h"
#include "optimizer/planner.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "iceberg_delta/fdw_modify.h"
#include "iceberg_delta/fdw_modify_state.h"
#include "iceberg_delta/catalog.h"
#include "iceberg_delta/fdw_storage_options.h"
#include "iceberg_delta/flush.h"

Oid IcebergDeltaGetDeltaRelidFromOptions(Oid foreign_relid)
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
    return (1 << CMD_INSERT) | (1 << CMD_DELETE);
}

/* ════════════════════════════════════════════════════════════════════
 * WHERE Expr → PyIceberg filter 字符串转换器
 *
 * 遍历 openGauss 的 Expr 树，重建为 PyIceberg 可解析的 SQL 过滤字符串。
 * 第一版支持：=, <>, <, <=, >, >=, AND/OR, 字面量。
 * 不支持：IN, LIKE, BETWEEN, 函数, 子查询, IS NULL（后续版本）。
 * ════════════════════════════════════════════════════════════════════ */

static void AppendFilterExpr(StringInfo buf, Node* node, PlannerInfo* root,
                              int targetRti, bool* hasError);

static void AppendFilterConst(StringInfo buf, Const* con)
{
    if (con->constisnull) {
        appendStringInfoString(buf, "None");
        return;
    }
    switch (con->consttype) {
        case INT2OID:
        case INT4OID:
            appendStringInfo(buf, "%d", DatumGetInt32(con->constvalue));
            break;
        case INT8OID:
            appendStringInfo(buf, "%ld", (long)DatumGetInt64(con->constvalue));
            break;
        case FLOAT4OID:
            appendStringInfo(buf, "%f", DatumGetFloat4(con->constvalue));
            break;
        case FLOAT8OID:
            appendStringInfo(buf, "%f", DatumGetFloat8(con->constvalue));
            break;
        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID: {
            char* str = TextDatumGetCString(con->constvalue);
            appendStringInfoChar(buf, '\'');
            /* escape embedded single quotes */
            for (char* p = str; *p; p++) {
                if (*p == '\'') appendStringInfoChar(buf, '\'');
                appendStringInfoChar(buf, *p);
            }
            appendStringInfoChar(buf, '\'');
            pfree(str);
            break;
        }
        default: {
            char* str = OidOutputFunctionCall(con->consttype, con->constvalue);
            appendStringInfoChar(buf, '\'');
            appendStringInfoString(buf, str);
            appendStringInfoChar(buf, '\'');
            pfree(str);
            break;
        }
    }
}

static void AppendFilterOpExpr(StringInfo buf, OpExpr* opexpr, PlannerInfo* root,
                                int targetRti, bool* hasError)
{
    if (list_length(opexpr->args) != 2) {
        *hasError = true;
        return;
    }
    Node* left  = (Node*)linitial(opexpr->args);
    Node* right = (Node*)lsecond(opexpr->args);

    appendStringInfoChar(buf, '(');
    AppendFilterExpr(buf, left, root, targetRti, hasError);
    if (*hasError) return;

    const char* opname = get_opname(opexpr->opno);
    if (opname == NULL) {
        *hasError = true;
        return;
    }
    /* map PG operator names to PyIceberg expression syntax */
    if (strcmp(opname, "=") == 0)
        appendStringInfoString(buf, " == ");
    else if (strcmp(opname, "<>") == 0)
        appendStringInfoString(buf, " != ");
    else if (strcmp(opname, "<") == 0)
        appendStringInfoString(buf, " < ");
    else if (strcmp(opname, "<=") == 0)
        appendStringInfoString(buf, " <= ");
    else if (strcmp(opname, ">") == 0)
        appendStringInfoString(buf, " > ");
    else if (strcmp(opname, ">=") == 0)
        appendStringInfoString(buf, " >= ");
    else {
        *hasError = true;
        return;
    }

    AppendFilterExpr(buf, right, root, targetRti, hasError);
    if (*hasError) return;
    appendStringInfoChar(buf, ')');
}

static void AppendFilterExpr(StringInfo buf, Node* node, PlannerInfo* root,
                              int targetRti, bool* hasError)
{
    if (node == NULL || *hasError) return;

    switch (nodeTag(node)) {
        case T_OpExpr:
            AppendFilterOpExpr(buf, (OpExpr*)node, root, targetRti, hasError);
            break;
        case T_BoolExpr: {
            BoolExpr* bexpr = (BoolExpr*)node;
            const char* joiner = (bexpr->boolop == OR_EXPR) ? " OR " : " AND ";
            appendStringInfoChar(buf, '(');
            ListCell* lc;
            bool first = true;
            foreach (lc, bexpr->args) {
                if (!first) appendStringInfoString(buf, joiner);
                first = false;
                AppendFilterExpr(buf, (Node*)lfirst(lc), root, targetRti, hasError);
                if (*hasError) return;
            }
            appendStringInfoChar(buf, ')');
            break;
        }
        case T_Var: {
            Var* var = (Var*)node;
            if ((int)var->varno != targetRti) {
                *hasError = true;
                return;
            }
            RangeTblEntry* rte = rt_fetch(targetRti, root->parse->rtable);
            char* colname = get_attname(rte->relid, var->varattno, false);
            if (colname == NULL) {
                *hasError = true;
                return;
            }
            appendStringInfoString(buf, quote_identifier(colname));
            pfree(colname);
            break;
        }
        case T_Const:
            AppendFilterConst(buf, (Const*)node);
            break;
        case T_RelabelType:
            AppendFilterExpr(buf, (Node*)((RelabelType*)node)->arg, root, targetRti, hasError);
            break;
        default:
            *hasError = true;
            break;
    }
}

static char* BuildFilterFromWhere(PlannerInfo* root, int targetRti,
                                   bool* out_is_empty)
{
    Query* parse = root->parse;
    *out_is_empty = false;

    if (parse->jointree == NULL) {
        *out_is_empty = true;
        return NULL;
    }
    FromExpr* from = (FromExpr*)parse->jointree;
    if (from->quals == NULL || from->quals == (Node*)NIL) {
        *out_is_empty = true;
        return NULL;  /* no WHERE → delete all */
    }

    StringInfoData buf;
    initStringInfo(&buf);
    bool hasError = false;

    ListCell* lc;
    bool first = true;
    foreach (lc, (List*)from->quals) {
        Node* qual = (Node*)lfirst(lc);
        if (!first) appendStringInfoString(&buf, " AND ");
        first = false;
        AppendFilterExpr(&buf, qual, root, targetRti, &hasError);
        if (hasError) {
            pfree(buf.data);
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("iceberg_fdw DELETE: unsupported WHERE expression"),
                     errdetail("Only =, <>, <, <=, >, >=, AND, OR, and simple "
                               "literals are supported in DELETE WHERE clauses")));
        }
    }

    return buf.data;
}

List* IcebergDeltaPlanForeignModify(PlannerInfo* root, ModifyTable* plan,
                                    Index resultRelation, int subplan_index)
{
    RangeTblEntry* rte = planner_rt_fetch(resultRelation, root);
    Oid delta_relid;

    if (root->parse->commandType != CMD_INSERT &&
        root->parse->commandType != CMD_DELETE) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("iceberg_fdw foreign table only supports INSERT and DELETE")));
    }

    delta_relid = IcebergDeltaGetDeltaRelidFromOptions(rte->relid);
    if (!OidIsValid(delta_relid)) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("iceberg_fdw foreign table has no associated delta table")));
    }

    List* result = list_make1_oid(delta_relid);

    if (root->parse->commandType == CMD_DELETE) {
        bool is_empty = false;
        int targetRti = (int)resultRelation;  /* planner_rt_fetch uses 1-based */
        char* filter = BuildFilterFromWhere(root, targetRti, &is_empty);

        if (is_empty) {
            /* no WHERE → delete all: empty filter triggers full delete */
            result = lappend(result, makeString(pstrdup("")));
        } else {
            result = lappend(result, makeString(filter));
        }
    }

    return result;
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

    /* DELETE: extract filter string from fdw_private (second element) */
    if (list_length(fdw_private) >= 2) {
        Value* filter_val = (Value*)lsecond(fdw_private);
        if (filter_val != NULL && filter_val->type == T_String)
            modify_state->delete_filter = pstrdup(strVal(filter_val));
        else
            modify_state->delete_filter = NULL;

        /* Locate "ctid" junk column position for ExecForeignDelete */
        PlanState* subplan_state = mtstate->mt_plans[subplan_index];
        modify_state->ctid_attno = ExecFindJunkAttributeInTlist(
            subplan_state->plan->targetlist, "ctid");
        if (!AttributeNumberIsValid(modify_state->ctid_attno))
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("could not find junk ctid column in delete plan")));
    } else {
        modify_state->delete_filter = NULL;
        modify_state->ctid_attno = InvalidAttrNumber;
    }

    rinfo->ri_FdwState = modify_state;
}

void IcebergDeltaAddForeignUpdateTargets(Query* parsetree,
                                          RangeTblEntry* target_rte,
                                          Relation target_relation)
{
    /*
     * Add a resjunk ctid column to the query's target list. The scan will
     * fill it with the delta table's ctid so that ExecForeignDelete can
     * delete the correct delta row.
     *
     * We use SelfItemPointerAttributeNumber, same as postgres_fdw for a
     * regular table's ctid.
     */
    Var* var = makeVar((Index)linitial_int(parsetree->resultRelations),
                       SelfItemPointerAttributeNumber,
                       TIDOID, -1, InvalidOid, 0);

    TargetEntry* tle = makeTargetEntry((Expr*)var,
        list_length(parsetree->targetList) + 1,
        pstrdup("ctid"), true);

    parsetree->targetList = lappend(parsetree->targetList, tle);
}

TupleTableSlot* IcebergDeltaExecForeignDelete(EState* estate,
                                               ResultRelInfo* rinfo,
                                               TupleTableSlot* slot,
                                               TupleTableSlot* planSlot)
{
    IcebergDeltaFdwModifyState* modify_state =
        (IcebergDeltaFdwModifyState*)rinfo->ri_FdwState;

    if (modify_state == NULL || modify_state->delta_rel == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("iceberg_fdw foreign table modify state is not initialized")));
    }

    /* Extract the delta ctid from the planSlot's junk column */
    if (!AttributeNumberIsValid(modify_state->ctid_attno)) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("could not find junk ctid column in delete plan")));
    }

    bool isNull = false;
    Datum ctid_datum = ExecGetJunkAttribute(planSlot,
        modify_state->ctid_attno, &isNull);
    if (isNull) {
        ereport(ERROR,
                (errcode(ERRCODE_NULL_JUNK_ATTRIBUTE),
                 errmsg("ctid is NULL in delete plan")));
    }

    ItemPointer ctid = (ItemPointer)DatumGetPointer(ctid_datum);

    /* Delete the delta row by ctid */
    TM_FailureData tmfd;
    TM_Result result = tableam_tuple_delete(modify_state->delta_rel, ctid,
                                             GetCurrentCommandId(false),
                                             InvalidSnapshot, GetActiveSnapshot(),
                                             true, NULL, &tmfd, false);
    if (result != TM_Ok && result != TM_Deleted) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("failed to delete row from iceberg delta table: result=%d",
                        (int)result)));
    }

    return slot;
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

    /* DELETE: flush the WHERE filter to the Iceberg data lake */
    if (modify_state->delete_filter != NULL) {
        /* Get foreign table OID from the result relation */
        Oid foreign_relid = rinfo->ri_RelationDesc->rd_id;
        IcebergDeltaDeleteFromLake(foreign_relid, modify_state->delete_filter);
    }

    if (modify_state->delta_rel != NULL) {
        relation_close(modify_state->delta_rel, RowExclusiveLock);
        modify_state->delta_rel = NULL;
    }

    rinfo->ri_FdwState = NULL;
}
