
-- Creates only the operational file registry; leaves existing STG tables intact.
USE ROLE SYSADMIN;
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;

CREATE TABLE IF NOT EXISTS RETAIL_DWH.CONTROL.FILE_MANIFEST (
    SOURCE_FILE_ID        VARCHAR(64) NOT NULL,
    SOURCE_FILE_NAME      VARCHAR(255) NOT NULL,
    SOURCE_FILE_SHA256    VARCHAR(64) NOT NULL,
    SOURCE_FILE_DATE      DATE NOT NULL,
    SOURCE_TYPE           VARCHAR(40) NOT NULL,
    STG_TABLE             VARCHAR(255) NOT NULL,
    ROW_COUNT             NUMBER(38, 0) NOT NULL,
    LOAD_STATUS           VARCHAR(20) NOT NULL,
    BATCH_ID              VARCHAR(100) NOT NULL,
    LOADED_AT             TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- In standard Snowflake tables, declared UNIQUE/PRIMARY KEY constraints are
-- informational, not enforced. This DAG uses deterministic identities and a
-- serial initial load instead of relying on a constraint to prevent duplicates.

DESCRIBE TABLE RETAIL_DWH.CONTROL.FILE_MANIFEST;
