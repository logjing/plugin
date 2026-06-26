/*
 * Bridge ABI declarations for iceberg-rust-bridge.
 * Minimal subset needed by the iceberg_delta FDW plugin.
 */

#ifndef ICEBERG_DELTA_BRIDGE_ABI_H
#define ICEBERG_DELTA_BRIDGE_ABI_H

/* Avoid dngettext macro conflict with PGXC c.h when included
 * after postgres.h in C++17 mode. PG defines dngettext as a
 * macro that shadows the GNU libintl declaration. */
#ifdef dngettext
#undef dngettext
#endif
#ifdef dgettext
#undef dgettext
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct IcebergBridgeString      IcebergBridgeString;
typedef struct IcebergBridgeError       IcebergBridgeError;
typedef struct IcebergBridgeStorage     IcebergBridgeStorage;
typedef struct IcebergBridgeTable       IcebergBridgeTable;
typedef struct IcebergBridgeTransaction IcebergBridgeTransaction;
typedef struct IcebergBridgeCatalog     IcebergBridgeCatalog;

typedef enum IcebergBridgeStatus {
  ICEBERG_BRIDGE_OK               = 0,
  ICEBERG_BRIDGE_INVALID_ARGUMENT = 1,
  ICEBERG_BRIDGE_NOT_FOUND        = 2,
  ICEBERG_BRIDGE_UNSUPPORTED      = 4,
  ICEBERG_BRIDGE_IO_ERROR         = 5,
  ICEBERG_BRIDGE_COMMIT_CONFLICT  = 7,
  ICEBERG_BRIDGE_RUNTIME_ERROR    = 8,
  ICEBERG_BRIDGE_INVALID_STATE    = 9,
  ICEBERG_BRIDGE_INTERNAL         = 100
} IcebergBridgeStatus;

typedef struct IcebergBridgeNamespaceIdent {
  const char* const* levels; size_t level_count;
} IcebergBridgeNamespaceIdent;

typedef struct IcebergBridgeTableIdent {
  IcebergBridgeNamespaceIdent namespace_ident; const char* name;
} IcebergBridgeTableIdent;

/* lifecycle */
const char* iceberg_bridge_string_data(const IcebergBridgeString* value);
void iceberg_bridge_string_free(IcebergBridgeString* value);
IcebergBridgeStatus iceberg_bridge_error_code(const IcebergBridgeError* err);
const char* iceberg_bridge_error_message(const IcebergBridgeError* err);
void iceberg_bridge_error_free(IcebergBridgeError* err);

/* storage */
IcebergBridgeStatus iceberg_bridge_storage_open(
    const char* storage_config_json, IcebergBridgeStorage** out, IcebergBridgeError** err);
void iceberg_bridge_storage_release(IcebergBridgeStorage* storage);

/* catalog */
IcebergBridgeStatus iceberg_bridge_catalog_open(
    const char* catalog_type, const char* props_json,
    IcebergBridgeCatalog** out, IcebergBridgeError** err);
void iceberg_bridge_catalog_release(IcebergBridgeCatalog* catalog);
IcebergBridgeStatus iceberg_bridge_catalog_register_table(
    IcebergBridgeCatalog* catalog, const char* table_ident_json,
    const char* metadata_location, IcebergBridgeString** out, IcebergBridgeError** err);
IcebergBridgeStatus iceberg_bridge_catalog_load_table(
    IcebergBridgeCatalog* catalog, const char* table_ident_json,
    IcebergBridgeTable** out, IcebergBridgeError** err);

/* table */
IcebergBridgeStatus iceberg_bridge_table_metadata_location(
    IcebergBridgeTable* table, IcebergBridgeString** out, IcebergBridgeError** err);
void iceberg_bridge_table_free(IcebergBridgeTable* table);

/* scan */
IcebergBridgeStatus iceberg_bridge_scan_positions(
    IcebergBridgeStorage* storage, const char* metadata_location,
    const IcebergBridgeTableIdent* table_ident, const char* filter_json,
    IcebergBridgeString** out_positions_json, IcebergBridgeError** err);

/* delete vector */
IcebergBridgeStatus iceberg_bridge_write_delete_vector(
    IcebergBridgeStorage* storage, const uint64_t* positions, size_t positions_count,
    const char* dv_path, const char* referenced_data_file,
    IcebergBridgeString** out_data_file_json, IcebergBridgeError** err);

/* transaction */
IcebergBridgeStatus iceberg_bridge_transaction_new(
    IcebergBridgeTable* table, IcebergBridgeTransaction** out, IcebergBridgeError** err);
IcebergBridgeStatus iceberg_bridge_transaction_row_delta(
    IcebergBridgeTransaction* tx, const char* add_data_files_json,
    const char* add_delete_files_json, const char* remove_data_files_json,
    IcebergBridgeError** err);
IcebergBridgeStatus iceberg_bridge_transaction_commit(
    IcebergBridgeCatalog* catalog, IcebergBridgeTransaction* tx,
    IcebergBridgeTable** out_new_table, IcebergBridgeError** err);
void iceberg_bridge_transaction_free(IcebergBridgeTransaction* tx);

#ifdef __cplusplus
}
#endif
#endif
