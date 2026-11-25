"""Explicit, guarded development reset. Run INSIDE the Airflow scheduler.

Default is read-only preflight. --apply --confirm RESET_RETAIL_DWH creates a
timestamped database clone, applies reporting DDL, and deletes only the named
pipeline table rows in one transaction. Other schemas/tables, stage files and
Airflow metadata are not cleared. Retail DAGs must be paused and drained.
Credentials are read internally and never printed.
"""

import argparse
import json
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

TABLES = [
    "TGT.FCT_SALES", "TGT.FCT_INVENTORY", "TGT.FCT_INVENTORY_DAILY",
    "TGT.DIM_ITEM", "TGT.DIM_LOCN", "TGT.DIM_SUBCLASS", "TGT.DIM_DISTRICT", "TGT.DIM_DATE",
    "TMP.ITEM_MASTER", "TMP.STORE_MASTER", "TMP.POS_SALES", "TMP.INVENTORY_SNAPSHOT",
    "DQ.QUARANTINE_RECORDS", "STG.ITEM_MASTER", "STG.STORE_MASTER", "STG.POS_SALES", "STG.INVENTORY_SNAPSHOT",
    "CONTROL.FILE_MANIFEST", "CONTROL.CORRECTION_RUNS", "CONTROL.RAW_VERSION_ARCHIVE",
    "CONTROL.MANIFEST_VERSION_ARCHIVE", "CONTROL.DQ_DECISION_ARCHIVE",
]
DAGS = ["retail_full_etl", "retail_stg_ingestion", "retail_tmp_processing", "retail_gold_pipeline",
        "retail_tgt_dimensions", "retail_historical_correction"]


def assert_idle():
    from airflow.models import DagModel, DagRun, TaskInstance
    from airflow.utils.session import create_session
    from airflow.configuration import conf
    from sqlalchemy import select

    with create_session() as session:
        models = list(session.scalars(select(DagModel).where(DagModel.dag_id.in_(DAGS))))
        if not models or any(not model.is_paused for model in models):
            raise RuntimeError("Pause all retail DAGs before reset")
        if session.scalar(select(DagRun.run_id).where(DagRun.dag_id.in_(DAGS),
                                                     DagRun.state.in_(["queued", "running"])).limit(1)):
            raise RuntimeError("Retail runs still queued/running; reset refused")
        if session.scalar(select(TaskInstance.task_id).where(TaskInstance.dag_id.in_(DAGS),
            TaskInstance.state.in_(["running", "queued", "scheduled", "up_for_retry", "up_for_reschedule"])).limit(1)):
            raise RuntimeError("Retail tasks are not drained; reset refused")
    data = Path("/opt/airflow/data")
    manifest = json.loads((data / "file_manifest.json").read_text())
    if int(conf.get("core", "max_map_length")) < manifest["file_count"]:
        raise RuntimeError("max_map_length is below input file count; fix/restart before resetting")
    import hashlib
    for entry in manifest["files"]:
        path = data / entry["file"]
        if hashlib.sha256(path.read_bytes()).hexdigest() != entry["sha256"]:
            raise RuntimeError(f"Input manifest checksum mismatch: {path.name}")
    return manifest


def sql_statements(path):
    text = "\n".join(line for line in path.read_text().splitlines() if not line.lstrip().startswith("--"))
    return [statement.strip() for statement in text.split(";") if statement.strip()]


def main(apply=False, confirmation=None):
    if apply and confirmation != "RESET_RETAIL_DWH":
        raise ValueError("Require --confirm RESET_RETAIL_DWH")
    manifest = assert_idle()
    from airflow.models.connection import Connection
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    connection = Connection.get_connection_from_secrets("retail_snowflake")
    hook = SnowflakeHook(snowflake_conn_id="retail_snowflake")
    # CLI maintenance is outside a TaskFlow supervisor. Use the existing
    # connection resolved through Airflow's server-side secrets API.
    hook.get_connection = lambda _: connection
    with closing(hook.get_conn()) as conn:
        conn.autocommit(True)
        with closing(conn.cursor()) as cursor:
            cursor.execute("SELECT CURRENT_DATABASE()")
            if cursor.fetchone()[0] != "RETAIL_DWH":
                raise RuntimeError("Unexpected database context; reset refused")
            counts = {}
            for table in TABLES:
                cursor.execute(f"SELECT COUNT(*) FROM RETAIL_DWH.{table}")
                counts[table] = int(cursor.fetchone()[0])
            report = {"input_files": manifest["file_count"], "before_rows": counts, "applied": False}
            if not apply:
                print(json.dumps(report, indent=2))
                return
            backup = "RETAIL_DWH_BACKUP_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            cursor.execute(f"CREATE DATABASE {backup} CLONE RETAIL_DWH")
            # Cloning may change the current database; every later object is
            # explicitly qualified, and session context is restored here.
            cursor.execute("USE DATABASE RETAIL_DWH")
            report["backup_database"] = backup
            print("Created recoverable Snowflake clone:", backup, flush=True)
            ddl_root = Path("/opt/airflow/include/sql/ddl")
            for filename in ("02_sales_reporting_attributes.sql", "03_create_sales_mart.sql"):
                for statement in sql_statements(ddl_root / filename):
                    cursor.execute(statement)
            cursor.execute("BEGIN")
            try:
                for table in TABLES:
                    cursor.execute(f"DELETE FROM RETAIL_DWH.{table}")
                for table in TABLES:
                    cursor.execute(f"SELECT COUNT(*) FROM RETAIL_DWH.{table}")
                    if int(cursor.fetchone()[0]):
                        raise RuntimeError(f"Reset coverage failed: {table}")
                cursor.execute("COMMIT")
            except Exception:
                cursor.execute("ROLLBACK")
                raise
            report["applied"] = True
            print(json.dumps(report, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args()
    main(args.apply, args.confirm)
