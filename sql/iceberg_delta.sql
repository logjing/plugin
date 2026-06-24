-- Test: iceberg_delta extension basic functionality

-- Install extension
CREATE EXTENSION iceberg_delta;

-- Verify schema and mapping table exist
SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'iceberg_delta';
SELECT tablename FROM pg_tables WHERE tablename = 'mapping' AND schemaname = 'iceberg_delta';

-- Verify FDW and server exist
SELECT fdwname FROM pg_foreign_data_wrapper WHERE fdwname = 'iceberg_fdw';
SELECT srvname FROM pg_foreign_server WHERE srvname = 'iceberg_server';

-- Create a foreign table (DDL hook should auto-create Delta internal table)
CREATE FOREIGN TABLE t_test_insert (
    id INTEGER,
    name TEXT,
    score FLOAT
) SERVER iceberg_server OPTIONS (warehouse '/data/iceberg', table_name 'test_insert');

-- Verify mapping record was created
SELECT foreign_relid, delta_schema, delta_name FROM iceberg_delta.mapping;

-- Verify delta_relid option was written to foreign table
SELECT pg_options_to_table(ftoptions) FROM pg_foreign_table ft
JOIN pg_class c ON c.oid = ft.ftrelid
WHERE c.relname = 't_test_insert';

-- Insert data (should be intercepted into Delta internal table)
INSERT INTO t_test_insert VALUES (1, 'alice', 95.5);
INSERT INTO t_test_insert VALUES (2, 'bob', 88.0);
INSERT INTO t_test_insert VALUES (3, 'charlie', 76.5);

-- Query delta table directly to verify data was written
SELECT delta_schema, delta_name FROM iceberg_delta.mapping WHERE foreign_relid = (
    SELECT oid FROM pg_class WHERE relname = 't_test_insert'
);

-- Verify DEPENDENCY_AUTO was registered
SELECT deptype FROM pg_depend WHERE objid = (
    SELECT oid FROM pg_class WHERE relname LIKE '%_delta' AND relnamespace = (
        SELECT oid FROM pg_namespace WHERE nspname = 'iceberg_delta'
    )
) AND refobjid = (
    SELECT oid FROM pg_class WHERE relname = 't_test_insert'
);

-- Drop foreign table (should cascade delete Delta internal table + mapping)
DROP FOREIGN TABLE t_test_insert;

-- Verify mapping record was cleaned up
SELECT count(*) FROM iceberg_delta.mapping;

-- Verify delta internal table was auto-cascaded
SELECT count(*) FROM pg_class WHERE relname LIKE '%_delta' AND relnamespace = (
    SELECT oid FROM pg_namespace WHERE nspname = 'iceberg_delta'
);

-- Test with multiple inserts in a transaction
CREATE FOREIGN TABLE t_tx_test (
    id INTEGER,
    value TEXT
) SERVER iceberg_server OPTIONS (warehouse '/data/iceberg', table_name 'tx_test');

BEGIN;
INSERT INTO t_tx_test VALUES (10, 'in_tx');
INSERT INTO t_tx_test VALUES (20, 'also_in_tx');
COMMIT;

-- Verify both rows in delta table
SELECT delta_schema, delta_name FROM iceberg_delta.mapping WHERE foreign_relid = (
    SELECT oid FROM pg_class WHERE relname = 't_tx_test'
);

-- Test rollback
BEGIN;
INSERT INTO t_tx_test VALUES (30, 'should_rollback');
ROLLBACK;

-- Row 30 should not exist in delta table after rollback

-- Cleanup
DROP FOREIGN TABLE t_tx_test;

-- Uninstall extension
DROP EXTENSION iceberg_delta CASCADE;
