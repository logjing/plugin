\echo Use "CREATE EXTENSION iceberg_delta" to load this file. \quit

-- 创建插件 schema
CREATE SCHEMA IF NOT EXISTS iceberg_delta;
GRANT USAGE ON SCHEMA iceberg_delta TO PUBLIC;

-- 映射表：替代 pg_delta_table 系统 catalog
CREATE TABLE iceberg_delta.mapping (
    foreign_relid   OID PRIMARY KEY,
    delta_relid     OID NOT NULL UNIQUE,
    delta_schema    TEXT NOT NULL,
    delta_name      TEXT NOT NULL,
    created_at      timestamptz DEFAULT now()
) WITH (STORAGE_TYPE = USTORE);

-- FDW handler 函数
CREATE OR REPLACE FUNCTION iceberg_delta.iceberg_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME', 'iceberg_fdw_handler'
LANGUAGE C STRICT NOT FENCED;

-- FDW validator 函数
CREATE OR REPLACE FUNCTION iceberg_delta.iceberg_fdw_validator(
    options text[], catalog_oid oid)
RETURNS void
AS 'MODULE_PATHNAME', 'iceberg_fdw_validator'
LANGUAGE C STRICT NOT FENCED;

-- 注册 FDW
CREATE FOREIGN DATA WRAPPER iceberg_fdw
    HANDLER iceberg_delta.iceberg_fdw_handler
    VALIDATOR iceberg_delta.iceberg_fdw_validator;

-- 默认 server
CREATE SERVER iceberg_server FOREIGN DATA WRAPPER iceberg_fdw;
