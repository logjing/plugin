#ifndef ICEBERG_DELTA_FDW_STORAGE_OPTIONS_H
#define ICEBERG_DELTA_FDW_STORAGE_OPTIONS_H

#include "postgres.h"

#define ICEBERG_OPT_LOCATION        "location"
#define ICEBERG_OPT_WAREHOUSE       "warehouse"
#define ICEBERG_OPT_TABLE_NAME      "table_name"
#define ICEBERG_OPT_FOLDERNAME      "foldername"

/* Internal options written by DDL hook */
#define ICEBERG_OPT_DELTA_RELID     "delta_relid"
#define ICEBERG_OPT_DELTA_SCHEMA    "delta_schema"
#define ICEBERG_OPT_DELTA_NAME      "delta_name"

/* S3 options (future) */
#define ICEBERG_OPT_S3_ENDPOINT         "s3_endpoint"
#define ICEBERG_OPT_S3_ACCESS_KEY_ID    "s3_access_key_id"
#define ICEBERG_OPT_S3_SECRET_ACCESS    "s3_secret_access_key"
#define ICEBERG_OPT_S3_REGION           "s3_region"
#define ICEBERG_OPT_S3_PATH_STYLE       "s3_path_style_access"
#define ICEBERG_OPT_S3_SSL_ENABLED      "s3_ssl_enabled"

#endif /* ICEBERG_DELTA_FDW_STORAGE_OPTIONS_H */
