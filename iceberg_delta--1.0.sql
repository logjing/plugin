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

-- Flush 函数：将 delta 表数据刷写到 Iceberg 数据湖
CREATE OR REPLACE FUNCTION iceberg_delta.iceberg_delta_flush(
    foreign_table regclass)
RETURNS int8
AS 'MODULE_PATHNAME', 'iceberg_delta_flush'
LANGUAGE C STRICT NOT FENCED;

-- 注册 FDW
CREATE FOREIGN DATA WRAPPER iceberg_fdw
    HANDLER iceberg_delta.iceberg_fdw_handler
    VALIDATOR iceberg_delta.iceberg_fdw_validator;

-- 默认 server
CREATE SERVER iceberg_server FOREIGN DATA WRAPPER iceberg_fdw;
