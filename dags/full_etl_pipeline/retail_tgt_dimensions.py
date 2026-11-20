""" load gold lookup and historical dimensions from validated TMP."""

import json
from contextlib import closing
from multiprocessing import Value
from pathlib import Path

from full_etl_pipeline.retail_tmp_processing import SQL_DIR
from full_etl_pipeline.stg_ingestion import CONFIG_PATH
import pendulum
from airflow.sdk import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

SQL_DIR = Path('/opt/airflow/include/sql/gold')
CONFIG = Path('/opt/airflow/config/retail_tmp.json')

@dag(
    dag_id="retail_tgt_dimensions",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["retail", "gold", "tgt", "scd"],
)
def retail_tgt_dimensions():
    @task(retries=2, pool="retail_writer")
    def run_sql_files(sql_filenames: list[str]) -> None:
        config = json.loads(CONFIG.read_text(encoding="utf-8"))
        conn_id = config["snowflake_conn_id"]
        if conn_id != "retail_snowflake":
            raise ValueError("use the existing retail_snowflake connection")

        hook = SnowflakeHook(snowflake_conn_id = conn_id)
        with closing(hook.get_conn()) as conn:
            conn.autocommit(True)
            with closing(conn.cursor()) as cursor:
                for name in sql_filenames:
                    if Path(name).name != name or not name.endswith(".sql"):
                        raise ValueError(f"Unexpected SQL filename : {name}")
                    path = SQL_DIR / name
                    sql = path.read_text(encoding= "utf-8").strip().rstrip(";")
                    print(f"Executing {path}")
                    cursor.execute(sql)

                    print(f"Completed {name}; affected rows: {cursor.rowcount}")

    @task(retries=2, pool="retail_writer")
    def load_dim_date():
        from full_etl_pipeline.retail_gold_pipeline import get_config
        from full_etl_pipeline.reporting import load_calendar

        cfg = get_config()
        with closing(SnowflakeHook(snowflake_conn_id=cfg["snowflake_conn_id"]).get_conn()) as conn:
            conn.autocommit(True)
            with closing(conn.cursor()) as cursor:
                load_calendar(cursor, cfg)

    calendar = load_dim_date()
    subclass = run_sql_files.override(task_id="load_dim_subclass")(
        ["load_dim_subclass.sql"]
    )
    district = run_sql_files.override(task_id="load_dim_district")(
        ["load_dim_district.sql"]
    )
    item = run_sql_files.override(task_id="load_dim_item")(
        ["prepare_dim_item.sql", "merge_dim_item.sql"]
    )
    location = run_sql_files.override(task_id="load_dim_locn")(
        ["prepare_dim_locn.sql", "merge_dim_locn.sql"]
    )

    # Complete lookup dimensions before assigning related surrogate keys.
    calendar >> subclass >> district >> item >> location


retail_tgt_dimensions()
