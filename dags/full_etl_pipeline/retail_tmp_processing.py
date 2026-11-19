"""STG -> TMP (Silver) and DQ quarantine for retail ETL.
Run AFTER retail_stg_ingestion has loaded your CSVs.
Execute 01_create_tmp_dq_tables.sql once in Snowflake before triggering.

 Each source is replaced in TMP/DQ within one explicit Snowflake transaction, making
retries and identical reruns safe. Only one TMP DAG run is allowed at a time.
"""

import json
from math import e
from pathlib import Path
import re
from contextlib import closing
from full_etl_pipeline.stg_ingestion import CONFIG_PATH

import pendulum 
from airflow.sdk import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

#configuration
CONFIG_PATH = Path("/opt/airflow/config/retail_tmp.json")
SQL_DIR = Path("/opt/airflow/include/sql/silver")

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


def load_config():
    cfg = json.loads(CONFIG_PATH.read_text(encoding = "utf-8"))
    for source in SOURCES:
        if source not in cfg["sources"]:
            raise ValueError(f"Missing source {source} in config file {CONFIG_PATH}")

        for key in ("stg","tmp", "work"):
            name = cfg["sources"][source][key]
            if not OBJECT_PATTERN.fullmatch(name):
                raise ValueError(f"Unsafe configured snowflake identifer: {name}")

        if not OBJECT_PATTERN.fullmatch(cfg["quarantine_table"]):
            raise ValueError("Invalid quarantine table identifier")
        
        if cfg["snowflake_conn_id"] != "retail_snowflake":
            raise ValueError("Use the verified retail_snowflake connection")

        return cfg

@dag(
    dag_id = "retail_tmp_processing",
    start_date = pendulum.datetime(2026,11,19, tz = "UTC"),
    schedule = None,
    catchup = False,
    max_active_runs = 1,
    max_active_tasks = 1,
    tags = ["retail", "tmp", "silver", "dq"],
)

def retail_tmp_processing():
    @task(retries = 2)
    def process_source(source: str):
        cfg = load_config()
        names = cfg["sources"][source]
        stage_table, target, work = names["stg"], names["tmp"], names["work"]
        quarantine = cfg["quarantine_table"]
        business_cols = SOURCES[source]["cols"] #item_master, store_master ......
        sql_file = SQL_DIR / SOURCES[source]["sql"]
        sql = sql_file.read_text(encoding = "utf-8").strip().rstrip(";")

        #The sql file create only a temporary decision table for the source, and then executes a single transaction to move data from STG -> TMP and DQ quarantine.
        hook = SnowflakeHook(snowflake_conn_id = cfg["snowflake_conn_id"])
        with closing(hook.get_conn()) as conn:
            conn.autocommit(True)

            with closing(conn.cursor()) as cur:
                cur.execute(f"SELECT COUNT(*) FROM {stage_table}")
                stg_count = int(cur.fetchone()[0])

                if not stg_count:
                    raise RuntimeError(f"{stage_table} is empty: ingest STG first")

                cur.execute(sql)
                cur.execute(f"""
                    SELECT COUNT(*), COUNT(DISTINCT SOURCE_ROW_ID),
                           COUNT_IF(REJECT_REASON IS NULL),
                           COUNT_IF(REJECT_REASON IS NOT NULL)
                    FROM {work}
                """)
                actual , distinct , accepted , rejected = (int(v or 0) for v in cur.fetchone())
                if actual != stg_count or distinct != stg_count:
                    raise RuntimeError(
                        f"{source}: work table row identity mismatch: "
                        f"STG={stg_count}, work={actual}, distinct={distinct}"
                    )

                if accepted + rejected != stg_count:
                    raise RuntimeError(f"{source}: rows lost during validation")

                # Atomic replacement of the source's current decision results:
                # identical reruns cannot create duplicated TMP/DQ rows.
                cur.execute("BEGIN")
                try:
                    cur.execute(f"""
                        DELETE FROM {target}
                        WHERE SOURCE_ROW_ID IN
                        (SELECT SOURCE_ROW_ID FROM {stage_table})
                    """)
                    cur.execute(f"""
                        DELETE FROM {quarantine}
                        WHERE SOURCE_TYPE = %s
                          AND SOURCE_ROW_ID IN
                              (SELECT SOURCE_ROW_ID FROM {stage_table})
                    """, (source,))
                    cur.execute(f"""
                        INSERT INTO {target} ({LINEAGE_COLS}, {business_cols})
                        SELECT {LINEAGE_COLS}, {business_cols}
                        FROM {work}
                        WHERE REJECT_REASON IS NULL
                    """)
                    cur.execute(f"""
                        INSERT INTO {quarantine}
                          (SOURCE_ROW_ID, SOURCE_FILE_ID, SOURCE_FILE_NAME,
                           SOURCE_FILE_DATE, SOURCE_ROW_NUMBER, SOURCE_TYPE,
                           BATCH_ID, REASON_CODE, REASON_DETAIL, RAW_PAYLOAD)
                        SELECT SOURCE_ROW_ID, SOURCE_FILE_ID, SOURCE_FILE_NAME,
                               SOURCE_FILE_DATE, SOURCE_ROW_NUMBER, %s,
                               BATCH_ID, REJECT_REASON,
                               'Validation failed: ' || REJECT_REASON,
                               RAW_PAYLOAD
                        FROM {work}
                        WHERE REJECT_REASON IS NOT NULL
                    """, (source,))

                    # Validate INSIDE the transaction before committing.
                    cur.execute(f"""
                        SELECT COUNT(*), COUNT(DISTINCT SOURCE_ROW_ID)
                        FROM {target}
                        WHERE SOURCE_ROW_ID IN
                            (SELECT SOURCE_ROW_ID FROM {stage_table})
                    """)
                    tmp_count, distinct_tmp = (int(v) for v in cur.fetchone())
                    cur.execute(f"""
                        SELECT COUNT(*), COUNT(DISTINCT SOURCE_ROW_ID)
                        FROM {quarantine}
                        WHERE SOURCE_TYPE = %s AND SOURCE_ROW_ID IN
                            (SELECT SOURCE_ROW_ID FROM {stage_table})
                    """, (source,))
                    dq_count, distinct_dq = (int(v) for v in cur.fetchone())
                    if ((tmp_count, distinct_tmp, dq_count, distinct_dq)
                            != (accepted, accepted, rejected, rejected)):
                        raise RuntimeError(
                            f"{source} counts wrong: "
                            f"expected TMP/DQ={accepted}/{rejected}, "
                            f"got={tmp_count}/{dq_count}"
                        )
                    cur.execute("COMMIT")
                except Exception:
                    cur.execute("ROLLBACK")
                    raise

                print(
                    f"TMP PASSED {source}: STG={stg_count} "
                    f"accepted={accepted} quarantined={rejected}"
                )
                return {"source": source, "raw": stg_count,
                        "accepted": accepted, "quarantined": rejected}

    items = process_source.override(task_id="load_tmp_item_master")("item_master")
    stores = process_source.override(task_id="load_tmp_store_master")("store_master")
    sales = process_source.override(task_id="load_tmp_pos_sales")("pos_sales")
    inventory = process_source.override(task_id="load_tmp_inventory_snapshot")("inventory_snapshot")

    # Master snapshots must be validated before fact references can be checked.
    items >> stores >> sales >> inventory


retail_tmp_processing()
