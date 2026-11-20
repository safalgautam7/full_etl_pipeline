# Retail ETL with Airflow and Snowflake

This pipeline loads item master, store master, POS sales, and inventory snapshot CSVs into Snowflake. It preserves raw input, quarantines invalid rows, and builds historical dimensions and facts.

The fixture covers **2026-11-09 through 2026-11-13**: five files each for items, sales, and inventory, plus two store snapshots. Use native Airflow Backfill to run the complete ETL one date at a time, or a manual single trigger for the original bulk load.

## Architecture

`retail_full_etl` uses Airflow's built-in daily scheduling/backfill. Every scheduled/backfill run executes the same three TaskGroups for its own business date:

```text
CSV files -> run_stg -> run_tmp -> run_gold -> reconciliation
             Bronze    Silver     Gold
```

- **STG (Bronze):** `discover_files` finds CSVs; `ingest_one.expand` maps one task per file. Each task checks the manifest, converts rows to JSONL, uploads both files to `RETAIL_DWH.STG.CSV_LANDING`, and copies JSONL into a temporary buffer. Raw rows and manifest entries merge transactionally. Four STG tables preserve `RAW_PAYLOAD VARIANT` and lineage, including business defects.
- **TMP (Silver):** Four tasks validate item, store, sales, and inventory rows in that order. Accepted rows go to the corresponding typed TMP tables; rejected rows go to DQ.
- **TGT (Gold):** Dimensions load before `FCT_SALES` and `FCT_INVENTORY`. Reconciliation is the final task and fails the DAG if checks do not pass.

The existing DAG reuses layer tasks directly. There is no custom controller, helper DAG, API connection, or backfill audit table. Manual single triggers retain the original bulk behavior. Standalone layer DAGs remain available. **Do not run them or external writers concurrently against shared tables.**

Aggregate means Gold roll-ups built from TGT. No Aggregate mart is implemented.

## Data quality gates

Ingestion rejects structurally malformed CSVs, such as duplicate headers or incorrect row widths, rather than silently dropping rows. Business validation happens in `include/sql/silver/`:

| SQL file | Checks |
| --- | --- |
| `validate_item_master.sql` | Required item/subclass values, snapshot dates, prices, active flags, duplicate item snapshots |
| `validate_store_master.sql` | Required store/district values, snapshot dates, active flags, duplicate store snapshots |
| `validate_pos_sales.sql` | Missing transaction/store/item keys, invalid or negative/zero quantities, prices, discounts, sales amounts, unknown references, duplicate transaction lines |
| `validate_inventory_snapshot.sql` | Required keys, nonnegative quantities/costs/values, value arithmetic, unknown references, duplicate daily positions |

Sales duplicates use `(TRANSACTION_ID, LINE_NUMBER)`. Reference checks use the latest accepted master snapshot on or before the business date, so a later store snapshot is not applied backward.

Each `load_tmp_*` task builds a decision table and publishes through `silver_processing.py`. `DQ.QUARANTINE_RECORDS` retains lineage, raw payload, rejection timestamp, and a primary reason such as `NULL_ITEM_ID`, `NEGATIVE_QUANTITY`, `UNKNOWN_ITEM_ID`, or `DUPLICATE_SALES_LINE`. DQ stores current dispositions, not decision history. Rejected rows remain in STG but are excluded from TMP and normal Gold loading.

Gold fact preflight checks require exactly one historical item and location match and unique business keys before publication.

## History and replay behavior

`DIM_SUBCLASS` and `DIM_DISTRICT` use SCD1: changed descriptions update in place. `DIM_ITEM` and `DIM_LOCN` use SCD2: changes create effective-dated versions with generated surrogate keys. Facts resolve `ITEM_SK` and `LOCN_SK` using the sale or snapshot date, not simply the current dimension row.

Inventory is positional history. Unchanged daily observations collapse into one version. A change in measures or historical dimension keys starts a new version and closes the previous interval. `merge_fct_inventory.sql` preserves prior positions rather than deleting them.

Replay protection works at each layer:

- A file's SHA-256 and filename determine `SOURCE_FILE_ID`; its physical row number determines `SOURCE_ROW_ID` within that file version.
- `CONTROL.FILE_MANIFEST` skips an already loaded file when its hash and stored row count match. STG inserts only missing source-row IDs.
- Conditional TMP/DQ MERGEs update only changed values. Identical replay preserves `PROCESSED_AT` and `REJECTED_AT`; only disposition changes remove rows from the previous destination. Coverage and exclusivity checks run before commit, with rollback on failure.
- Gold MERGEs use stable business/version keys and changed-value checks to retain existing surrogate keys on unchanged replay.

Same-name files with changed bytes fail deliberately. Reordering or correcting a file is not handled automatically by source-row MERGE.

## How to run

### Prerequisites

Use Docker Engine and Compose v2. The stack defaults to Airflow **3.3.2**, CeleryExecutor, PostgreSQL, and Redis. Configure local `.env` values including `AIRFLOW_UID`, `FERNET_KEY`, and authentication settings. Ensure Snowflake, standard, Celery, and FAB providers are installed; Compose supports `_PIP_ADDITIONAL_REQUIREMENTS` for extra dependencies. Never commit credentials.

Snowflake setup requires:

1. An existing `RETAIL_DWH` database with `STG`, `TMP`, `DQ`, `TGT`, and `CONTROL` schemas, the four raw STG tables, and the internal stage `RETAIL_DWH.STG.CSV_LANDING`. Their initialization DDL is **not included** in this repository.
2. Run `include/sql/control/01_file_manifest.sql`, then `include/sql/ddl/create_tmp_dq_tables.sql`, `create_tgt_dimensions.sql`, and `create_tgt_facts.sql` in Snowflake. These create missing tables without replacing populated ones. Review their explicit `SYSADMIN` role and `SNOWFLAKE_LEARNING_WH` warehouse for your environment.
3. A runtime role with warehouse, stage, table read/write, and temporary-table creation permissions.

### Start and configure Airflow

Run from the project root:

```bash
docker compose up airflow-init
docker compose up -d
```

Open `http://localhost:8080` and configure these Airflow connections:

- `file_con`: filesystem connection with Extra `{"path":"/opt/airflow/data"}`.
- `retail_snowflake`: Snowflake connection with your account, authentication, role, warehouse, and database settings.

STG and TMP read `config/retail_etl.json` and `config/retail_tmp.json`. Create the Airflow Variable **`retail_gold_config`** with the JSON contents of `config/retail_gold_config.json`. Compose mounts `config/`, `data/`, `dags/`, and `include/` under `/opt/airflow/`.

Place source files in `data/` using `<source>_YYYYMMDD.csv`, where source is `item_master`, `store_master`, `pos_sales`, or `inventory_snapshot`. Check DAG imports, unpause the controller, and trigger the full run:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors --output json
docker compose exec airflow-scheduler airflow dags unpause retail_full_etl
docker compose exec airflow-scheduler airflow dags trigger retail_full_etl --conf '{"file_limit": 0}'
```

Follow `run_stg`, `run_tmp`, and `run_gold` in the Airflow graph. In a manual bulk run, `file_limit=0` selects every recognized CSV; standalone ingestion defaults to one file. Scheduled/native backfill runs always select their own date's complete daily inputs, regardless of the smoke-test limit.

## How to backfill and verify

### Sequential daily backfill

Use the **Backfill option in the Trigger menu** on `retail_full_etl` at `http://localhost:8080`:

1. Choose November 9–13, 2026; confirm the preview includes those five daily intervals.
2. Set backfill **Max active runs = 1** and keep **Run backwards disabled**. Airflow's backfill concurrency setting is separate from the DAG setting.
3. Select **Reprocess failed**. Airflow requires failed/completed reprocessing when tasks use `depends_on_past=True`; **None produces a 422 validation error**, even for a new range. Failed reprocessing also creates runs for dates with no existing run, but does not rerun successful dates. Select completed reprocessing only when deliberately replaying successful dates with unchanged inputs.
4. Start the backfill. Airflow runs the original full ETL for Nov 9, then Nov 10, Nov 11, Nov 12, and Nov 13.

No Trigger JSON date range, new connections, or extra setup SQL is needed. Each run uses `data_interval_start` as its business date. The existing discovery task has `depends_on_past`/`wait_for_downstream`, including a direct dependency on final reconciliation: the next day cannot process until the previous full day succeeds. A failed day must be fixed/retried before later dates can proceed.

**Run-ID dates:** `backfill__2026-11-10T00:00:00+00:00` processes the interval starting **Nov 9**, not Nov 10. For Nov 9–13 the displayed run-ID dates are Nov 10–14. Confirm **Logical Date / Data Interval Start** in the run details. Daily runs have 16 task instances on Nov 9/11 (four files) and 15 on the other days (three files); manual bulk runs over all 17 files have 29 instances.

Required item/sales/inventory files must exist each day. Store files are loaded only on their own dates; other days reuse previously accepted store snapshots. Start with Nov 9 to load its initial store master. If starting later, applicable earlier masters must already be loaded. There is no separate bootstrap workflow.

Silver and sales publication are day-scoped; SCD/inventory preparation reads accepted history and closes affected intervals. **A previous bulk load can be replayed day by day without resetting data**: earlier-day files must match their loaded manifest checksums and complete raw counts, and Silver decisions must remain unchanged. SCD1 keeps the latest accepted descriptions, while SCD2/inventory retain later interval ends/current flags. New or changed historical inputs and changed validation decisions still require explicit repair; do not delete shared data to bypass these checks.

The DAG now has a daily UTC timetable starting November 9 and `catchup=False`, which is necessary for native backfill. **Unpausing also enables ordinary automatic daily runs**; only the explicit Backfill dialog requests the historical range. Those automatic runs require that day's inputs too.

### Verify results

Fixture totals below describe all 17 supplied files, not every narrower daily range. Use Airflow's native run history for the five run dates/statuses and `evidence/verify_etl_stages.sql` for actual business results. Existing data is never deleted merely to match fixture totals.

`evidence/verify_etl_stages.sql` remains the read-only Snowflake screenshot worksheet for Bronze, Silver/DQ, Gold, reconciliation, SCD1/SCD2, and replay. Use `evidence/scd_evidence_guide.md` for capture order. SQL verifies the final warehouse state; pair it with five successful native backfill runs and daily discovery logs to prove day-by-day execution. Original manifest/STG `BATCH_ID` values remain from the first ingestion during an unchanged backfill replay, even when that was a manual bulk run.

The `reconcile_gold` task executes `include/sql/reconcile/reconcile_gold.sql` and `check_gold_integrity.sql`. For each observed sales date it compares both row counts and net sales amounts:

```text
Raw sales = Accepted sales + Rejected sales
Gold sales = Accepted sales
```

Unparseable sales amounts fail reconciliation. Integrity checks cover intervals, current versions, historical keys, sales-line matching, quarantined sales in Gold, and inventory observation coverage. Inventory versions are not expected to equal TMP observation counts. Results are logged, not persisted.

Inspect the final task log or execute those SQL files in Snowflake. Check file status and rejection reasons with:

```sql
SELECT SOURCE_FILE_NAME, ROW_COUNT, LOAD_STATUS
FROM RETAIL_DWH.CONTROL.FILE_MANIFEST ORDER BY SOURCE_FILE_NAME;

SELECT SOURCE_TYPE, REASON_CODE, COUNT(*) AS ROWS_REJECTED
FROM RETAIL_DWH.DQ.QUARANTINE_RECORDS GROUP BY SOURCE_TYPE, REASON_CODE;
```

For the supplied fixture only, expected results are 510 STG rows, 505 accepted TMP rows, five rejected sales rows, and 145 Gold sales lines totaling **1566.15**, with **61.70** rejected sales amount. The current inventory design produces 98 versions from 150 accepted daily observations. These are test examples, not runtime assumptions. `tests/fixtures/expected_results.json` is used only by tests.

Run local checks without Snowflake credentials:

```bash
PYTHONPATH=dags python3 -m unittest discover -s tests/unit -v
PYTHONPATH=dags uv run --with 'duckdb>=1.4,<2' --no-project python -m unittest discover -s tests/integration -v
```

The second command requires `uv`; alternatively install `requirements-test.txt` in a test environment. Local SQL tests cover Silver publication and replay, not Snowflake behavior or Gold acceptance. `tests/integration/test_snowflake_silver.py` is opt-in and requires a disposable test database and private connector configuration.

## Known limitations

- Native backfill uses the original DAG and Airflow's run history, not custom locks/audit tables. Do not run standalone layer DAGs or external writers concurrently. Arbitrary historical rebuild/repair is unsupported; reconciliation results are logged rather than persisted.
- Changed-file versioning and correction workflows are unsupported. Changing validation rules also needs downstream repair: the current Gold MERGE does not remove sales lines that become rejected.
- Sales keys assume transaction IDs are globally unique. Store-scoped IDs would require a consistent key change across validation, loading, and reconciliation.
- Some SQL object names remain hardcoded to `RETAIL_DWH`. Structural CSV failures fail the task; there is no separate structural quarantine workflow.
- Full Snowflake acceptance and Gold replay evidence are pending. The conditional Silver MERGE has local regression coverage, but its opt-in Snowflake verification has not yet run.

## Project files and evidence

- `dags/full_etl_pipeline/`: controller, standalone DAGs, and Silver publication helper.
- `dags/full_etl_pipeline/daily_processing.py`: native data-interval date selection and day-scoped Silver/Gold adapters; covered by `tests/unit/test_native_backfill.py`.
- `include/sql/`: initialization DDL, validation, MERGE, and reconciliation SQL.
- `tests/`: unit tests, local SQL regression tests, isolated Snowflake test, and fixture expectations.
- `scripts/validate_retail_dags.py`: DAG structure and configuration checks in Airflow.
- `evidence/silver_merge_checkpoint.md`: verification status, local test results, and historical run observations. Execution logs are under `logs/dag_id=retail_full_etl/` when retained locally.
- `retail_etl_architecture.mmd`: architecture diagram; `requirements_and_implementation.md`: assignment contract and remaining work.
- `architecture notes.md`: developer walkthrough of discovery, intermediate tables, loading, history, and transaction boundaries.
