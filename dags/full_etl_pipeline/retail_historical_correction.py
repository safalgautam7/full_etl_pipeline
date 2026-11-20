"""Explicit approved full-file replacement; one pool slot for the entire repair."""

import hashlib
import json
import tempfile
from contextlib import closing
from pathlib import Path

import pendulum
from airflow.sdk import dag, task, get_current_context

from full_etl_pipeline.daily_processing import inspect_filename


@dag(
    dag_id="retail_historical_correction",
    start_date=pendulum.datetime(2026, 11, 9, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["retail", "correction", "maintenance"],
)
def retail_historical_correction():
    @task(retries=0, pool="retail_writer")
    def replace_file():
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from airflow.providers.standard.hooks.filesystem import FSHook
        from full_etl_pipeline.corrections import correct_file
        from full_etl_pipeline.reporting import load_calendar
        from full_etl_pipeline.retail_gold_pipeline import get_config
        from full_etl_pipeline.silver_processing import load_config
        from full_etl_pipeline.stg_ingestion import checksum, make_jsonl, put_and_check, settings

        context = get_current_context()
        request = context["dag_run"].conf or {}
        filename = request.get("filename", "")
        source, file_date = inspect_filename(filename)  # Reject paths/traversal.
        reason = request.get("reason", "")
        approver = request.get("approved_by", "")
        expected = request.get("expected_old_sha256", "")
        if (request.get("approved") is not True or not isinstance(reason, str)
                or not reason.strip() or len(reason) > 2000 or not isinstance(approver, str)
                or not approver.strip() or len(approver) > 255
                or not isinstance(expected, str) or len(expected) != 64
                or any(c not in "0123456789abcdef" for c in expected)):
            raise ValueError("Require approved=true, reason, approved_by, and expected_old_sha256")
        ingestion = settings()
        cfg = load_config("/opt/airflow/config/retail_tmp.json")
        gold = get_config()
        root = Path(FSHook(fs_conn_id=ingestion["file_conn_id"]).get_path()).resolve()
        csv_path = root / "corrections" / filename
        if not csv_path.is_file() or not csv_path.resolve().is_relative_to(root):
            raise ValueError("Replacement must exist in data/corrections inside the source root")
        digest = checksum(csv_path)
        file_id = hashlib.sha256(f"{filename}:{digest}".encode()).hexdigest()
        attempt = f"{context['run_id']}:{context['ti'].try_number}"
        correction_id = hashlib.sha256(attempt.encode()).hexdigest()
        hook = SnowflakeHook(snowflake_conn_id=cfg["snowflake_conn_id"])
        with closing(hook.get_conn()) as conn:
            conn.autocommit(True)
            with closing(conn.cursor()) as cur:
                # Calendar extension is independent of business publication.
                load_calendar(cur, gold)
                cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE RETAIL_DWH.STG.REPLACEMENT_INPUT "
                            f"LIKE {cfg['sources'][source]['stg']}")
                with tempfile.TemporaryDirectory(prefix="retail_correction_") as tmp:
                    jsonl = Path(tmp) / "replacement.jsonl"
                    count = make_jsonl(csv_path, jsonl, file_id)
                    base = f"@{ingestion['stage']}/_stg_ingest/{source}/{file_id}/"
                    put_and_check(cur, csv_path, base + "source/")
                    put_and_check(cur, jsonl, base + "json/")
                    with jsonl.open(encoding="utf-8") as stream:
                        for line in stream:
                            row = json.loads(line)
                            cur.execute("INSERT INTO RETAIL_DWH.STG.REPLACEMENT_INPUT "
                                        "(SOURCE_ROW_ID,SOURCE_FILE_ID,SOURCE_FILE_NAME,SOURCE_FILE_DATE,"
                                        "SOURCE_ROW_NUMBER,RAW_PAYLOAD,BATCH_ID) "
                                        "SELECT %s,%s,%s,TO_DATE(%s),%s,PARSE_JSON(%s),%s",
                                        (row["source_row_id"], file_id, filename, file_date.isoformat(),
                                         row["source_row_number"], json.dumps(row["raw_payload"]), correction_id))
                    if checksum(csv_path) != digest:
                        raise RuntimeError("Replacement changed while being staged")
                print(f"Staged {count} replacement rows; correction={correction_id}")
                return correct_file(
                    cur, cfg, "/opt/airflow/include/sql", filename=filename, source=source,
                    old_sha=expected, new_sha=digest, new_file_id=file_id,
                    correction_id=correction_id, reason=reason, approved_by=approver,
                )

    replace_file()


retail_historical_correction()
