""" idempotent CSV -> JSONL -> Snowflake STG ingestion.
Airflow 3.3.2. Manual trigger: {"file_limit": 1} for smoke test,
{"file_limit": 0} for all CSVs. Optional start_date/end_date (YYYY-MM-DD)
select an inclusive filename range and earlier master dependencies; use limit 0.
Config in /opt/airflow/config/retail_etl.json.

"""

import csv
import hashlib
import json
import re
import tempfile
from contextlib import closing
from pathlib import Path

import pendulum
from airflow.sdk import dag, get_current_context, task
from full_etl_pipeline.file_selection import FILE_PATTERN, inspect_filename, select_files

CONFIG_PATH = Path("/opt/airflow/config/retail_etl.json")
# Identifiers are read from external config, but must not allow arbitrary SQL.
SQL_OBJECT = re.compile(r"^[A-Z][A-Z0-9_]*(\.[A-Z][A-Z0-9_]*){2}$") # to allow execution of 


def settings():
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    for name in [cfg["stage"], cfg["file_manifest_table"], *cfg["stg_tables"].values()]:
        if not SQL_OBJECT.fullmatch(name):
            raise ValueError(f"Unsafe Snowflake object name in config: {name}")
    return cfg


def checksum(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def make_jsonl(csv_path, jsonl_path, file_id):
    """Keep exact header names and source text values (including empty strings).

    File-level structural errors fail the load, not silently skip a raw row.
    Business validation (NULL item, negative qty, duplicate sale) is in TMP.
    """
    count = 0
    with csv_path.open("r", encoding="utf-8-sig", newline="") as source:
        with jsonl_path.open("w", encoding="utf-8", newline="\n") as output:
            reader = csv.reader(source, strict=True)
            headers = next(reader, None)
            if not headers or any(not h.strip() for h in headers):
                raise ValueError(f"Blank/missing header in {csv_path.name}")
            if len(headers) != len(set(headers)):
                raise ValueError(f"Duplicate column name in {csv_path.name}")
            for row_number, values in enumerate(reader, start=1):
                if len(values) != len(headers):
                    raise ValueError(
                        f"Malformed row {row_number} in {csv_path.name}: "
                        f"expected {len(headers)} fields, found {len(values)}"
                    )
                payload = dict(zip(headers, values))
                row_id = hashlib.sha256(
                    f"{file_id}:{row_number}".encode("utf-8")
                ).hexdigest()
                envelope = {
                    "source_row_id": row_id,
                    "source_row_number": row_number,
                    "raw_payload": payload,
                }
                output.write(json.dumps(envelope, ensure_ascii=False, separators=(",", ":")))
                output.write("\n")
                count += 1
    return count


def put_and_check(cursor, local_path, stage_prefix):
    # Inputs here are only locally generated paths and validated stage IDs.
    cursor.execute(
        f"PUT '{local_path.resolve().as_uri()}' {stage_prefix} "
        "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )
    results = cursor.fetchall()
    if not results or any(str(row[6]).upper() not in {"UPLOADED", "SKIPPED"} for row in results):
        raise RuntimeError(f"Snowflake PUT unsuccessful: {results}")


@dag(
    dag_id="retail_stg_ingestion",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,  # Serial for initial safety; map creates separate tasks.
    params={"file_limit": 1, "start_date": None, "end_date": None},
    tags=["retail", "stg", "idempotent"],
)
def retail_stg_ingestion():
    @task
    def discover_files():
        from airflow.providers.standard.hooks.filesystem import FSHook

        cfg = settings()
        root = Path(FSHook(fs_conn_id=cfg["file_conn_id"]).get_path())
        if not root.is_dir():
            raise FileNotFoundError(root)
        names = sorted(p.name for p in root.glob("*.csv") if FILE_PATTERN.fullmatch(p.name))
        params = get_current_context()["params"]
        chosen, dependencies = select_files(
            names, params["file_limit"], params.get("start_date"), params.get("end_date")
        )
        print(f"Selection range: {params.get('start_date')} to {params.get('end_date')}")
        print(f"Additional earlier master snapshots: {dependencies}")
        print(f"Discovered {len(names)} recognized files; selecting {len(chosen)}: {chosen}")
        return chosen

    @task(retries=2)
    def ingest_one(filename: str):
        from airflow.providers.standard.hooks.filesystem import FSHook
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        cfg = settings()
        source_type, file_date = inspect_filename(filename)
        root = Path(FSHook(fs_conn_id=cfg["file_conn_id"]).get_path())
        csv_path = root / filename
        if not csv_path.is_file():
            raise FileNotFoundError(csv_path)
        file_sha = checksum(csv_path)
        file_id = hashlib.sha256(f"{filename}:{file_sha}".encode()).hexdigest()
        manifest = cfg["file_manifest_table"]
        target = cfg["stg_tables"][source_type]
        stage = cfg["stage"]
        batch_id = get_current_context()["run_id"]

        hook = SnowflakeHook(snowflake_conn_id=cfg["snowflake_conn_id"])
        with closing(hook.get_conn()) as conn:
            conn.autocommit(True)
            with closing(conn.cursor()) as cur:
                cur.execute(
                    f"SELECT SOURCE_FILE_ID, SOURCE_FILE_SHA256, ROW_COUNT, LOAD_STATUS "
                    f"FROM {manifest} WHERE SOURCE_FILE_NAME=%s",
                    (filename,),
                )
                registered = cur.fetchall()
                if len(registered) > 1:
                    raise RuntimeError(f"Duplicate manifest identities for {filename}; stop and investigate")
                if registered and registered[0][1] != file_sha:
                    raise RuntimeError(
                        f"File correction detected: {filename} has a different SHA-256. "
                        "Do not append it automatically; use a controlled correction workflow."
                    )
                if registered and registered[0][3] == "LOADED":
                    cur.execute(
                        f"SELECT COUNT(*) FROM {target} WHERE SOURCE_FILE_ID=%s", (file_id,)
                    )
                    existing_count = cur.fetchone()[0]
                    if existing_count == registered[0][2]:
                        print(f"SKIP {filename}: already loaded, {existing_count} rows")
                        return {"file": filename, "status": "SKIPPED", "rows": existing_count}
                    print(f"Recovering incomplete target for {filename}")

                # Generated files are temporary on the worker; original CSV remains unchanged.
                with tempfile.TemporaryDirectory(prefix="retail_stg_") as tmp:
                    jsonl_path = Path(tmp) / f"{Path(filename).stem}_{file_id}.jsonl"
                    expected = make_jsonl(csv_path, jsonl_path, file_id)
                    base_stage = f"@{stage}/_stg_ingest/{source_type}/{file_id}/"
                    csv_prefix = base_stage + "source/"
                    json_prefix = base_stage + "json/"
                    put_and_check(cur, csv_path, csv_prefix)
                    put_and_check(cur, jsonl_path, json_prefix)

                    # Session-local buffer, created before the STG+manifest transaction.
                    cur.execute(
                        "CREATE OR REPLACE TEMPORARY TABLE RETAIL_DWH.STG.STG_LOAD_BUFFER ("
                        "SOURCE_ROW_ID VARCHAR(64), SOURCE_ROW_NUMBER NUMBER(38,0), "
                        "RAW_PAYLOAD VARIANT)"
                    )
                    staged_json = json_prefix + jsonl_path.name
                    cur.execute(f"""
                        COPY INTO RETAIL_DWH.STG.STG_LOAD_BUFFER
                        (SOURCE_ROW_ID, SOURCE_ROW_NUMBER, RAW_PAYLOAD)
                        FROM (
                          SELECT t.$1:source_row_id::VARCHAR,
                                 t.$1:source_row_number::NUMBER(38,0),
                                 t.$1:raw_payload
                          FROM {staged_json} t
                        )
                        FILE_FORMAT = (TYPE = JSON COMPRESSION = NONE)
                        ON_ERROR = ABORT_STATEMENT
                        FORCE = TRUE
                    """)
                    cur.execute(
                        "SELECT COUNT(*), COUNT(DISTINCT SOURCE_ROW_ID) "
                        "FROM RETAIL_DWH.STG.STG_LOAD_BUFFER"
                    )
                    loaded, distinct_rows = cur.fetchone()
                    if loaded != expected or distinct_rows != expected:
                        raise RuntimeError(
                            f"Stage-to-buffer mismatch for {filename}: "
                            f"CSV={expected} buffer={loaded} unique={distinct_rows}"
                        )

                    # MERGE STG rows and manifest atomically. No DDL inside this transaction.
                    cur.execute("BEGIN")
                    try:
                        cur.execute(f"""
                            MERGE INTO {target} dst USING
                              (SELECT SOURCE_ROW_ID, SOURCE_ROW_NUMBER, RAW_PAYLOAD
                               FROM RETAIL_DWH.STG.STG_LOAD_BUFFER) src
                            ON dst.SOURCE_ROW_ID = src.SOURCE_ROW_ID
                            WHEN NOT MATCHED THEN INSERT
                              (SOURCE_ROW_ID, SOURCE_FILE_ID, SOURCE_FILE_NAME,
                               SOURCE_FILE_DATE, SOURCE_ROW_NUMBER, RAW_PAYLOAD,
                               BATCH_ID)
                            VALUES
                              (src.SOURCE_ROW_ID, %s, %s, TO_DATE(%s),
                               src.SOURCE_ROW_NUMBER, src.RAW_PAYLOAD, %s)
                        """, (file_id, filename, file_date.isoformat(), batch_id))
                        cur.execute(f"""
                            MERGE INTO {manifest} dst USING
                            (SELECT %s AS SOURCE_FILE_ID, %s AS SOURCE_FILE_NAME,
                                    %s AS SOURCE_FILE_SHA256, TO_DATE(%s) AS SOURCE_FILE_DATE,
                                    %s AS SOURCE_TYPE, %s AS STG_TABLE,
                                    %s AS ROW_COUNT, 'LOADED' AS LOAD_STATUS,
                                    %s AS BATCH_ID) src
                            ON dst.SOURCE_FILE_NAME = src.SOURCE_FILE_NAME
                            WHEN MATCHED THEN UPDATE SET
                               ROW_COUNT=src.ROW_COUNT, LOAD_STATUS=src.LOAD_STATUS,
                               BATCH_ID=src.BATCH_ID, LOADED_AT=CURRENT_TIMESTAMP()
                            WHEN NOT MATCHED THEN INSERT
                              (SOURCE_FILE_ID, SOURCE_FILE_NAME, SOURCE_FILE_SHA256,
                               SOURCE_FILE_DATE, SOURCE_TYPE, STG_TABLE,
                               ROW_COUNT, LOAD_STATUS, BATCH_ID)
                            VALUES (src.SOURCE_FILE_ID, src.SOURCE_FILE_NAME,
                                    src.SOURCE_FILE_SHA256, src.SOURCE_FILE_DATE,
                                    src.SOURCE_TYPE, src.STG_TABLE, src.ROW_COUNT,
                                    src.LOAD_STATUS, src.BATCH_ID)
                        """, (
                            file_id, filename, file_sha, file_date.isoformat(),
                            source_type, target, expected, batch_id,
                        ))
                        cur.execute("COMMIT")
                    except Exception:
                        cur.execute("ROLLBACK")
                        raise

                    cur.execute(
                        f"SELECT COUNT(*) FROM {target} WHERE SOURCE_FILE_ID=%s", (file_id,)
                    )
                    target_count = cur.fetchone()[0]
                    if target_count != expected:
                        raise RuntimeError(
                            f"STG target count mismatch for {filename}: {target_count} != {expected}"
                        )
                    print(f"LOADED {filename}: {expected} rows into {target}")
                    return {"file": filename, "status": "LOADED", "rows": expected}

    ingest_one.expand(filename=discover_files())


retail_stg_ingestion()
