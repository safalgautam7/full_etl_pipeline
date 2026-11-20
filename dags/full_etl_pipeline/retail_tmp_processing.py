"""STG -> TMP (Silver) and current DQ dispositions.

Run after STG ingestion. Validation rules are unchanged; conditional publication
preserves PROCESSED_AT/REJECTED_AT on identical replay. The session-local decision
table is built before the atomic TMP/DQ DML transaction.
"""

from contextlib import closing
from pathlib import Path

import pendulum
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task, get_current_context
from full_etl_pipeline import silver_processing

CONFIG_PATH = Path("/opt/airflow/config/retail_tmp.json")
SQL_DIR = Path("/opt/airflow/include/sql/silver")


def load_config():
    return silver_processing.load_config(CONFIG_PATH)


@dag(
    dag_id="retail_tmp_processing",
    start_date=pendulum.datetime(2026, 11, 19, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["retail", "tmp", "silver", "dq"],
)
def retail_tmp_processing():
    @task(retries=2)
    def process_source(source: str):
        cfg = load_config()
        hook = SnowflakeHook(snowflake_conn_id=cfg["snowflake_conn_id"])
        with closing(hook.get_conn()) as conn:
            conn.autocommit(True)
            with closing(conn.cursor()) as cur:
                context = get_current_context()
                from full_etl_pipeline.daily_processing import processing_day, process_daily_source

                day = processing_day(context)
                if day:
                    result = process_daily_source(cur, cfg, source, day, SQL_DIR)
                    if result is None:
                        return
                else:
                    result = silver_processing.process_source(cur, cfg, source, SQL_DIR)
                print(
                    f"TMP PASSED {source}: STG={result['raw']} "
                    f"accepted={result['accepted']} quarantined={result['quarantined']}"
                )
                return result

    items = process_source.override(task_id="load_tmp_item_master")("item_master")
    stores = process_source.override(task_id="load_tmp_store_master")("store_master")
    sales = process_source.override(task_id="load_tmp_pos_sales")("pos_sales")
    inventory = process_source.override(task_id="load_tmp_inventory_snapshot")("inventory_snapshot")

    # Master snapshots must be validated before fact references can be checked.
    items >> stores >> sales >> inventory


retail_tmp_processing()
