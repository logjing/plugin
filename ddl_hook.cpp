#include "postgres.h"
#include "tcop/utility.h"
#include "nodes/parsenodes.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "foreign/foreign.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "executor/spi.h"

#include "iceberg_delta/ddl_hook.h"
#include "iceberg_delta/delta_table.h"
#include "iceberg_delta/catalog.h"

static const char* kIcebergServerName = "iceberg_server";
static const char* kDeltaSchemaName = "iceberg_delta";

static void CallNextUtility(processutility_context* pucontext,
                            DestReceiver* dest,
#ifdef PGXC
                            bool sentToRemote,
#endif
                            char* completionTag,
                            ProcessUtilityContext context,
                            bool isCTAS)
{
    if (prev_ProcessUtility) {
        prev_ProcessUtility(pucontext, dest,
#ifdef PGXC
                            sentToRemote,
#endif
                            completionTag, context, isCTAS);
    } else {
        standard_ProcessUtility(pucontext, dest,
#ifdef PGXC
                                sentToRemote,
#endif
                                completionTag, context, isCTAS);
    }
}

static bool IsIcebergServerByName(const char* servername)
{
    return servername != NULL && strcmp(servername, kIcebergServerName) == 0;
}

static bool IsIcebergServerByRelid(Oid relid)
{
    ForeignTable* ft = NULL;
    ForeignServer* server = NULL;
    bool result = false;

    PG_TRY();
    {
        ft = GetForeignTable(relid);
        server = GetForeignServer(ft->serverid);
        result = (server != NULL &&
                  strcmp(server->servername, kIcebergServerName) == 0);
    }
    PG_CATCH();
    {
        FlushErrorState();
        result = false;
    }
    PG_END_TRY();

    return result;
}

static void IcebergDeltaUpdateForeignTableOptions(Oid foreign_relid, Oid delta_relid,
                                                  const char* delta_schema,
                                                  const char* delta_name)
{
    int rc = SPI_connect();
    if (rc != SPI_OK_CONNECT) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("could not connect to SPI for foreign table options update")));
    }

    char relid_str[32];
    snprintf(relid_str, sizeof(relid_str), "%u", delta_relid);

    char* foreign_qualname = get_namespace_name(get_rel_namespace(foreign_relid));
    char* foreign_relname = get_rel_name(foreign_relid);

    StringInfoData sql;
    initStringInfo(&sql);
    appendStringInfo(&sql,
        "ALTER FOREIGN TABLE %s.%s OPTIONS (ADD delta_relid '%s', "
        "ADD delta_schema '%s', ADD delta_name '%s')",
        quote_identifier(foreign_qualname),
        quote_identifier(foreign_relname),
        relid_str,
        delta_schema,
        delta_name);

    rc = SPI_execute(sql.data, false, 0);
    if (rc != SPI_OK_UTILITY) {
        SPI_finish();
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("failed to update foreign table options for delta cache")));
    }

    SPI_finish();
}

static void IcebergDeltaHandleCreateForeignTable(CreateForeignTableStmt* stmt)
{
    Oid foreign_relid = RangeVarGetRelid(stmt->base.relation, NoLock, false);
    if (!OidIsValid(foreign_relid)) {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("could not determine relid for created iceberg foreign table")));
    }

    Relation rel = relation_open(foreign_relid, AccessShareLock);
    TupleDesc tupdesc = RelationGetDescr(rel);

    char delta_name[NAMEDATALEN];
    snprintf(delta_name, sizeof(delta_name), "delta_%u", foreign_relid);

    Oid delta_relid = IcebergDeltaTableCreate(foreign_relid,
                                               kDeltaSchemaName,
                                               delta_name, tupdesc);

    /* Close relation BEFORE SPI ALTER to avoid lock conflict */
    relation_close(rel, AccessShareLock);

    if (!OidIsValid(delta_relid)) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("failed to create delta heap table for iceberg foreign table")));
    }

    ObjectAddress delta_addr;
    ObjectAddress ft_addr;
    ObjectAddressSet(delta_addr, RelationRelationId, delta_relid);
    ObjectAddressSet(ft_addr, RelationRelationId, foreign_relid);
    recordDependencyOn(&delta_addr, &ft_addr, DEPENDENCY_AUTO);

    if (!IcebergCatalogInsertDeltaTableMapping(foreign_relid, delta_relid,
                                                kDeltaSchemaName, delta_name)) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("failed to insert iceberg delta table mapping")));
    }

    IcebergDeltaUpdateForeignTableOptions(foreign_relid, delta_relid,
                                          kDeltaSchemaName, delta_name);
}

static void IcebergDeltaHandleDropForeignTable(DropStmt* stmt)
{
    ListCell* lc;
    foreach (lc, stmt->objects) {
        List* names = (List*)lfirst(lc);
        RangeVar* rv = makeRangeVarFromNameList(names);
        Oid relid = RangeVarGetRelid(rv, NoLock, stmt->missing_ok);
        if (!OidIsValid(relid)) continue;
        if (get_rel_relkind(relid) != RELKIND_FOREIGN_TABLE) continue;
        if (!IsIcebergServerByRelid(relid)) continue;

        IcebergCatalogDeleteDeltaTableMapping(relid);
    }
}

void IcebergDeltaDDLHook(processutility_context* pucontext,
                         DestReceiver* dest,
#ifdef PGXC
                         bool sentToRemote,
#endif
                         char* completionTag,
                         ProcessUtilityContext context,
                         bool isCTAS)
{
    Node* parsetree = pucontext->parse_tree;

    if (IsA(parsetree, CreateForeignTableStmt)) {
        CreateForeignTableStmt* stmt = (CreateForeignTableStmt*)parsetree;
        if (IsIcebergServerByName(stmt->servername)) {
            CallNextUtility(pucontext, dest,
#ifdef PGXC
                            sentToRemote,
#endif
                            completionTag, context, isCTAS);
            IcebergDeltaHandleCreateForeignTable(stmt);
            return;
        }
    } else if (IsA(parsetree, DropStmt)) {
        DropStmt* stmt = (DropStmt*)parsetree;
        if (stmt->removeType == OBJECT_FOREIGN_TABLE) {
            bool has_iceberg = false;
            ListCell* lc;
            foreach (lc, stmt->objects) {
                List* names = (List*)lfirst(lc);
                RangeVar* rv = makeRangeVarFromNameList(names);
                Oid relid = RangeVarGetRelid(rv, NoLock, stmt->missing_ok);
                if (OidIsValid(relid) && IsIcebergServerByRelid(relid)) {
                    has_iceberg = true;
                    break;
                }
            }
            if (has_iceberg) {
                IcebergDeltaHandleDropForeignTable(stmt);
                CallNextUtility(pucontext, dest,
#ifdef PGXC
                                sentToRemote,
#endif
                                completionTag, context, isCTAS);
                return;
            }
        }
    }

    CallNextUtility(pucontext, dest,
#ifdef PGXC
                    sentToRemote,
#endif
                    completionTag, context, isCTAS);
}
