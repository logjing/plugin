#ifndef ICEBERG_DELTA_FDW_MODIFY_H
#define ICEBERG_DELTA_FDW_MODIFY_H

#include "postgres.h"
#include "executor/exec/execdesc.h"
#include "nodes/execnodes.h"

Oid IcebergDeltaGetDeltaRelidFromOptions(Oid foreign_relid);
int IcebergDeltaIsForeignRelUpdatable(Relation rel);
List *IcebergDeltaPlanForeignModify(PlannerInfo *root, ModifyTable *plan,
                                    Index resultRelation, int subplan_index);
void IcebergDeltaBeginForeignModify(ModifyTableState *mtstate,
                                    ResultRelInfo *rinfo, List *fdw_private,
                                    int subplan_index, int eflags);
TupleTableSlot *IcebergDeltaExecForeignInsert(EState *estate,
                                               ResultRelInfo *rinfo,
                                               TupleTableSlot *slot,
                                               TupleTableSlot *planSlot);
TupleTableSlot *IcebergDeltaExecForeignDelete(EState *estate,
                                               ResultRelInfo *rinfo,
                                               TupleTableSlot *slot,
                                               TupleTableSlot *planSlot);
void IcebergDeltaEndForeignModify(EState *estate, ResultRelInfo *rinfo);
void IcebergDeltaAddForeignUpdateTargets(Query *parsetree,
                                          RangeTblEntry *target_rte,
                                          Relation target_relation);

#endif /* ICEBERG_DELTA_FDW_MODIFY_H */
