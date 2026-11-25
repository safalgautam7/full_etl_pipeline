"""Gold calendar, dimensions, versioned/daily facts and reconciliation.

Reused by retail_full_etl; standalone triggering rebuilds from all accepted
history. Requires the additive reporting migration and one-slot writer pool.
"""

from contextlib import closing
from datetime import timedelta
from pathlib import Path
import re

import pendulum
from airflow.sdk import Variable, dag, task, get_current_context

OBJECT = re.compile(r"^[A-Z][A-Z0-9_]*(\.[A-Z][A-Z0-9_]*){2}$")


def get_config() -> dict:
    """Read from Airflow's task-time Variable API, not at DAG parse time."""
    cfg = Variable.get("retail_gold_config", deserialize_json=True)
    needed = (
        "snowflake_conn_id", "gold_sql_dir", "reconcile_sql_dir",
        "sales_work_table", "inventory_observations_table",
        "inventory_work_table", "dimension_tables",
    )
    for key in needed:
        if key not in cfg:
            raise ValueError(f"Missing Airflow variable field: {key}")
    for key in ("sales_work_table", "inventory_observations_table",
                "inventory_work_table"):
        if not OBJECT.fullmatch(cfg[key]):
            raise ValueError(f"Invalid configured Snowflake object: {key}")
    if len(cfg["dimension_tables"]) != 5 or "RETAIL_DWH.TGT.DIM_DATE" not in cfg["dimension_tables"]:
        raise ValueError("Expected five configured dimensions including DIM_DATE; update retail_gold_config")
    for name in cfg["dimension_tables"]:
        if not OBJECT.fullmatch(name):
            raise ValueError("Invalid dimension table in configuration")
    return cfg


def execute_file(cursor, directory: str, name: str) -> None:
    """Execute one trusted SQL file in the existing Snowflake session."""
    if Path(name).name != name or not name.endswith(".sql"):
        raise ValueError("Unexpected SQL filename")
    path = Path(directory) / name
    sql = path.read_text(encoding="utf-8").strip().rstrip(";")
    if not sql:
        raise ValueError(f"Empty SQL file: {path}")
    context = get_current_context()
    from full_etl_pipeline.daily_processing import processing_day, scoped_gold_sql

    day = processing_day(context)
    if day:
        sql = scoped_gold_sql(sql, name, day)
    print(f"Running SQL: {path.name}")
    cursor.execute(sql)
    print(f"Completed: {path.name}; cursor rowcount={cursor.rowcount}")


def require_no_failure(label: str, stats: tuple, names: tuple) -> None:
    actual = dict(zip(names, (int(n or 0) for n in stats)))
    print(label, actual)
    if actual["rows"] <= 0 or actual["rows"] != actual["unique_rows"]:
        raise RuntimeError(f"{label}: duplicate/missing source row IDs: {actual}")
    if any(v != 0 for k, v in actual.items() if k not in ("rows", "unique_rows")):
        raise RuntimeError(f"{label}: source quality check failed: {actual}")


def require_calendar_matches(cursor, table, column):
    # Only trusted code supplies identifiers. Fail BEFORE publishing facts.
    cursor.execute(f"SELECT COUNT(*) FROM {table} f WHERE "
                   f"(SELECT COUNT(*) FROM RETAIL_DWH.TGT.DIM_DATE d "
                   f"WHERE d.CALENDAR_DATE=f.{column}) <> 1")
    if int(cursor.fetchone()[0]):
        raise RuntimeError("Missing/duplicate calendar dates; extend calendar coverage before loading facts")


@dag(
    dag_id="retail_gold_pipeline",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["retail", "gold", "tgt", "facts", "reconciliation"],
)
def retail_gold_pipeline():
    @task(retries=2, retry_delay=timedelta(minutes=1), pool="retail_writer")
    def load_dimension(sql_names: list[str]) -> None:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        cfg = get_config()
        hook = SnowflakeHook(snowflake_conn_id=cfg["snowflake_conn_id"])
        with closing(hook.get_conn()) as conn:
            conn.autocommit(True)
            with closing(conn.cursor()) as cur:
                for name in sql_names:
                    execute_file(cur, cfg["gold_sql_dir"], name)

    @task(retries=2, pool="retail_writer")
    def load_dim_date() -> None:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from full_etl_pipeline.reporting import load_calendar

        cfg = get_config()
        with closing(SnowflakeHook(snowflake_conn_id=cfg["snowflake_conn_id"]).get_conn()) as conn:
            conn.autocommit(True)
            with closing(conn.cursor()) as cur:
                load_calendar(cur, cfg)

    @task(retries=2, retry_delay=timedelta(minutes=1), pool="retail_writer")
    def load_sales() -> None:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        cfg = get_config()
        work = cfg["sales_work_table"]
        hook = SnowflakeHook(snowflake_conn_id=cfg["snowflake_conn_id"])
        with closing(hook.get_conn()) as conn:
            conn.autocommit(True)
            with closing(conn.cursor()) as cur:
                execute_file(cur, cfg["gold_sql_dir"], "prepare_fct_sales.sql")
                require_calendar_matches(cur, work, "SALE_DATE")
                cur.execute(f"""
                    SELECT COUNT(*), COUNT(DISTINCT SOURCE_ROW_ID),
                           COALESCE(COUNT_IF(ITEM_MATCH_COUNT <> 1),0),
                           COALESCE(COUNT_IF(LOCN_MATCH_COUNT <> 1),0),
                           COALESCE(COUNT_IF(BUSINESS_KEY_OCCURRENCES <> 1),0)
                    FROM {work}
                """)
                require_no_failure(
                    "Sales preflight", cur.fetchone(),
                    ("rows", "unique_rows", "wrong_item_matches",
                     "wrong_location_matches", "duplicate_business_keys"),
                )
                execute_file(cur, cfg["gold_sql_dir"], "merge_fct_sales.sql")

    @task(retries=2, retry_delay=timedelta(minutes=1), pool="retail_writer")
    def load_inventory() -> None:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        cfg = get_config()
        obs = cfg["inventory_observations_table"]
        versions = cfg["inventory_work_table"]
        hook = SnowflakeHook(snowflake_conn_id=cfg["snowflake_conn_id"])
        with closing(hook.get_conn()) as conn:
            conn.autocommit(True)
            with closing(conn.cursor()) as cur:
                execute_file(cur, cfg["gold_sql_dir"],
                              "resolve_inventory_observations.sql")
                require_calendar_matches(cur, obs, "SNAPSHOT_DATE")
                # Validate every daily observation *before* compressing it.
                cur.execute(f"""
                    SELECT COUNT(*), COUNT(DISTINCT SOURCE_ROW_ID),
                           COALESCE(COUNT_IF(ITEM_MATCH_COUNT <> 1),0),
                           COALESCE(COUNT_IF(LOCN_MATCH_COUNT <> 1),0),
                           COALESCE(COUNT_IF(DUPLICATE_POSITION_DAY <> 1),0)
                    FROM {obs}
                """)
                require_no_failure(
                    "Inventory observations preflight", cur.fetchone(),
                    ("rows", "unique_rows", "wrong_item_matches",
                     "wrong_location_matches", "duplicate_business_date_keys"),
                )
                execute_file(cur, cfg["gold_sql_dir"],
                             "prepare_fct_inventory.sql")
                cur.execute(f"SELECT COUNT(*) FROM {versions}")
                version_count = int(cur.fetchone()[0])
                if version_count <= 0:
                    raise RuntimeError("No inventory versions were produced")
                cur.execute(f"""
                    SELECT COUNT(*) FROM (
                        SELECT STORE_ID, ITEM_ID, EFFECTIVE_FROM
                        FROM {versions}
                        GROUP BY STORE_ID, ITEM_ID, EFFECTIVE_FROM
                        HAVING COUNT(*) > 1
                    )
                """)
                if int(cur.fetchone()[0]) != 0:
                    raise RuntimeError("Duplicate inventory version start")
                print("Inventory compressed versions:", version_count)
                execute_file(cur, cfg["gold_sql_dir"],
                              "merge_fct_inventory.sql")
                execute_file(cur, cfg["gold_sql_dir"],
                             "merge_fct_inventory_daily.sql")

    @task(retries=1, pool="retail_writer")
    def reconcile_gold() -> None:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        cfg = get_config()
        hook = SnowflakeHook(snowflake_conn_id=cfg["snowflake_conn_id"])
        with closing(hook.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                execute_file(cur, cfg["reconcile_sql_dir"],
                             "reconcile_gold.sql")
                reconciliation_rows = cur.fetchall()
                if not reconciliation_rows:
                    raise RuntimeError("Reconciliation found no business dates")
                for row in reconciliation_rows:
                    print("SALES_RECONCILIATION:", row)
                failed = [r for r in reconciliation_rows if r[-1] != "PASS"]
                if failed:
                    raise RuntimeError(f"Gold sales reconciliation failed: {failed}")

                execute_file(cur, cfg["reconcile_sql_dir"],
                             "check_gold_integrity.sql")
                checks = cur.fetchall()
                if not checks:
                    raise RuntimeError("Gold integrity query returned no checks")
                for name, failures in checks:
                    print("GOLD_INTEGRITY:", name, failures)
                broken = [(name, failures) for name, failures in checks
                          if int(failures or 0) != 0]
                if broken:
                    raise RuntimeError(f"Gold integrity checks failed: {broken}")
                execute_file(cur, cfg["reconcile_sql_dir"], "check_reporting_integrity.sql")
                reporting = cur.fetchall()
                if not reporting or any(int(n or 0) for _, n in reporting):
                    raise RuntimeError(f"Reporting integrity checks failed: {reporting}")
                if cfg.get("sales_mart_enabled", False):
                    execute_file(cur, cfg["reconcile_sql_dir"], "check_sales_mart_integrity.sql")
                    mart = cur.fetchall()
                    if not mart or any(int(n or 0) for _, n in mart):
                        raise RuntimeError(f"Sales mart integrity checks failed: {mart}")
                print("GOLD RECONCILIATION PASSED")

    calendar = load_dim_date()
    subclass = load_dimension.override(task_id="load_dim_subclass")(
        ["load_dim_subclass.sql"])
    district = load_dimension.override(task_id="load_dim_district")(
        ["load_dim_district.sql"])
    item = load_dimension.override(task_id="load_dim_item")(
        ["prepare_dim_item.sql", "merge_dim_item.sql"])
    location = load_dimension.override(task_id="load_dim_locn")(
        ["prepare_dim_locn.sql", "merge_dim_locn.sql"])

    sales = load_sales()
    inventory = load_inventory()
    reconcile = reconcile_gold()
    calendar >> subclass >> district >> item >> location >> sales >> inventory >> reconcile


retail_gold_pipeline()
