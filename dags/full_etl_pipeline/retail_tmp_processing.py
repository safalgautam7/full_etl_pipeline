"""STG -> TMP (Silver) and DQ quarantine for retail ETL.
Run AFTER retail_stg_ingestion has loaded your CSVs.
Execute 01_create_tmp_dq_tables.sql once in Snowflake before triggering.

 Each source is replaced in TMP/DQ within one explicit Snowflake transaction, making
retries and identical reruns safe. Only one TMP DAG run is allowed at a time.
"""

import json
from pathlib import Path
import re
from contextlib import closing
from dags.full_etl_pipeline.stg_ingestion import CONFIG_PATH
import pendulum 
from airflow.sdk import dag, task

#configuration
CONFIG_PATH = Path("/opt/airflow/config/retail_tmp.json")
SQL_DIR = Path(__file__).resolve().parent / "sql"

#valid snowflake names
OBJECT_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*(\.[A-Z][A-Z0-9_]*){2}$") # 3 .names 

#SQL decision table + accepted typed business columns.
SOURCES = {
    "item_master": {
        "sql": "validate_item_master.sql",
        "cols": "SNAPSHOT_DATE, ITEM_ID, ITEM_NAME, SUBCLASS_ID, SUBCLASS_NAME, BRAND, UNIT_OF_MEASURE, LIST_PRICE, IS_ACTIVE",
    },
    "store_master": {
        "sql": "validate_store_master.sql",
        "cols": "SNAPSHOT_DATE, STORE_ID, STORE_NAME, DISTRICT_ID, DISTRICT_NAME, CITY, REGION, IS_ACTIVE",
    },
    "pos_sales": {
        "sql": "validate_pos_sales.sql",
        "cols": "SALE_DATE, TRANSACTION_ID, LINE_NUMBER, STORE_ID, ITEM_ID, QUANTITY, UNIT_PRICE, DISCOUNT_AMOUNT, NET_SALES, PAYMENT_METHOD",
    },
    "inventory_snapshot": {
        "sql": "validate_inventory_snapshot.sql",
        "cols": "SNAPSHOT_DATE, STORE_ID, ITEM_ID, QUANTITY_ON_HAND, UNIT_COST, INVENTORY_VALUE",
    },
}

LINEAGE_COLS = (
    "SOURCE_ROW_ID, SOURCE_FILE_ID, SOURCE_FILE_NAME, "
    "SOURCE_FILE_DATE, SOURCE_ROW_NUMBER, BATCH_ID"
)


