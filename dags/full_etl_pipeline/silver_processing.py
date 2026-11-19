"""Silver publication helpers, independent of Airflow and connection credentials.

Validation creates a session-local WORK table before BEGIN. Publication keeps
one current disposition per physical source row and only updates changed values.
Source-file corrections and downstream Gold reclassification are separate work.
"""

import json
import re
from pathlib import Path

OBJECT_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*(\.[A-Z][A-Z0-9_]*){2}$")
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


def validate_config(cfg):
    """Validate every source, not just the first dictionary entry."""
    for source in SOURCES:
        if source not in cfg["sources"]:
            raise ValueError(f"Missing source {source} in Silver config")
        for key in ("stg", "tmp", "work"):
            name = cfg["sources"][source][key]
            if not isinstance(name, str) or not OBJECT_PATTERN.fullmatch(name):
                raise ValueError(f"Unsafe configured Snowflake identifier: {name}")
    name = cfg["quarantine_table"]
    if not isinstance(name, str) or not OBJECT_PATTERN.fullmatch(name):
        raise ValueError("Invalid quarantine table identifier")
    if cfg["snowflake_conn_id"] != "retail_snowflake":
        raise ValueError("Use the verified retail_snowflake connection")
    return cfg


def load_config(path):
    return validate_config(json.loads(Path(path).read_text(encoding="utf-8")))


def read_sql(directory, filename, names):
    """Only validated object identifiers are interpolated; data stays bound."""
    return (Path(directory) / filename).read_text(encoding="utf-8").strip().rstrip(";").format(**names)


def publish_work(cursor, cfg, source, sql_dir):
    """Validate and publish an already-built WORK table in this same session.

No DDL runs inside the transaction. On identical replay both MERGEs and the
targeted transition deletes are no-ops, preserving original timestamps.
"""
    validate_config(cfg)
    if source not in SOURCES:
        raise ValueError(f"Unsupported source: {source}")
    names = {**cfg["sources"][source], "quarantine": cfg["quarantine_table"]}

    def execute(filename, parameters=None):
        sql = read_sql(sql_dir, filename, names)
        if parameters is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, parameters)

    execute("check_work.sql")
    raw, raw_distinct, actual, distinct, accepted, rejected, foreign_rows = (
        int(v or 0) for v in cursor.fetchone()
    )
    if not raw:
        raise RuntimeError(f"{names['stg']} is empty: ingest STG first")
    if (raw != raw_distinct or actual != raw or distinct != raw or foreign_rows):
        raise RuntimeError(
            f"{source}: work table row identity mismatch: STG={raw}, "
            f"STG distinct={raw_distinct}, work={actual}, distinct={distinct}, "
            f"foreign rows={foreign_rows}"
        )
    if accepted + rejected != raw:
        raise RuntimeError(f"{source}: rows lost during validation")

    cursor.execute("BEGIN")
    try:
        execute("remove_reclassified_tmp.sql")
        execute("remove_reclassified_dq.sql", (source,))
        execute(f"merge_{source}.sql")
        execute("merge_quarantine.sql", (source,))

        execute("check_dispositions.sql", (source, source))
        result = tuple(int(v or 0) for v in cursor.fetchone())
        expected = (accepted, accepted, rejected, rejected, 0, 0, 0)
        if result != expected:
            raise RuntimeError(
                f"{source}: disposition mismatch: expected={expected}, actual={result}"
            )
        cursor.execute("COMMIT")
    except Exception:
        cursor.execute("ROLLBACK")
        raise

    return {"source": source, "raw": raw, "accepted": accepted, "quarantined": rejected}


def process_source(cursor, cfg, source, sql_dir):
    """Run the existing classifier, then atomically publish its decisions."""
    validate_config(cfg)
    if source not in SOURCES:
        raise ValueError(f"Unsupported source: {source}")
    cursor.execute(read_sql(sql_dir, SOURCES[source]["sql"], {}))
    return publish_work(cursor, cfg, source, sql_dir)
