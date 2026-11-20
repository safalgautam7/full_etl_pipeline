# Retail ETL with Airflow and Snowflake

This pipeline loads item master, store master, POS sales, and inventory snapshot CSVs into Snowflake. It preserves raw input, quarantines invalid rows, and builds historical dimensions and facts.

The fixture covers **2026-11-09 through 2026-11-13**: five files each for items, sales, and inventory, plus two store snapshots. Load all 17 files or select an inclusive filename-date range with applicable earlier master snapshots.

## Architecture

`retail_full_etl` runs three expandable TaskGroups in order:

```text
CSV files -> run_stg -> run_tmp -> run_gold -> reconciliation
             Bronze    Silver     Gold
```

- **STG (Bronze):** `discover_files` finds CSVs; `ingest_one.expand` maps one task per file. Each task checks the manifest, converts rows to JSONL, uploads both files to `RETAIL_DWH.STG.CSV_LANDING`, and copies JSONL into a temporary buffer. Raw rows and manifest entries merge transactionally. Four STG tables preserve `RAW_PAYLOAD VARIANT` and lineage, including business defects.
- **TMP (Silver):** Four tasks validate item, store, sales, and inventory rows in that order. Accepted rows go to the corresponding typed TMP tables; rejected rows go to DQ.
- **TGT (Gold):** Dimensions load before `FCT_SALES` and `FCT_INVENTORY`. Reconciliation is the final task and fails the DAG if checks do not pass.

The controller reuses tasks, not child-DAG triggers. Standalone DAGs are `retail_stg_ingestion`, `retail_tmp_processing`, and `retail_gold_pipeline`; `retail_tgt_dimensions` is dimensions-only. **Do not run these concurrently against shared tables.**

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

Follow `run_stg`, `run_tmp`, and `run_gold` in the Airflow graph. Without dates, `file_limit=0` selects every recognized CSV; the standalone ingestion DAG defaults to one file for smoke testing. Date-range selection is described below.

## How to backfill and verify

Place historical files and applicable earlier master snapshots in `data/`, then trigger an inclusive range:

```bash
docker compose exec airflow-scheduler airflow dags trigger retail_full_etl --conf '{"start_date":"2026-11-10","end_date":"2026-11-13","file_limit":0}'
```

- Supply both dates as `YYYY-MM-DD`, start on or before end. Omit both (or use JSON null) to retain all-file/limit behavior.
- Ranges require `file_limit=0`, including standalone `retail_stg_ingestion`, whose default is 1.
- Discovery selects in-range files and the latest available item/store snapshot on or before the start. Extra earlier dependencies are logged; future snapshots are never substituted.
- Missing days/sources are allowed; no in-range matches fails discovery. If files begin November 10, requesting November 9–13 selects what exists. Missing applicable masters can cause unknown-reference quarantine.
- Manifest updates are automatic; already-loaded files are skipped. The one-file-per-source-per-date naming convention is unchanged.

For the supplied fixture, November 10–13 selects **14 files**: four each for item, sales, and inventory, plus the November 9 and November 11 store snapshots. November 9 supplies the initial store reference; November 11 supplies the later change. No November 9 sales or inventory files are selected.

To select a single day, set both dates to that day. In the Airflow UI, pass the same JSON in the trigger configuration. For ingestion only, use the same CLI command with DAG ID `retail_stg_ingestion`; this does not run Silver or Gold.

This scopes **file ingestion only**. Silver and Gold still read all warehouse inputs to derive history. The range does not delete existing data, filter Gold output, or enable scheduled catchup. Filename sorting is not day-by-day orchestration. Fixture totals below apply to the complete fixture, not necessarily to a fresh warehouse loaded with a narrower range.

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

- Date-scoped Silver/Gold publication, complete input-readiness checks, cross-DAG locking, logical batch/day audit, and persisted reconciliation remain incomplete. File ingestion supports date ranges. Per-DAG concurrency is one, but does not protect against a different DAG writing the same tables.
- Changed-file versioning and correction workflows are unsupported. Changing validation rules also needs downstream repair: the current Gold MERGE does not remove sales lines that become rejected.
- Sales keys assume transaction IDs are globally unique. Store-scoped IDs would require a consistent key change across validation, loading, and reconciliation.
- Some SQL object names remain hardcoded to `RETAIL_DWH`. Structural CSV failures fail the task; there is no separate structural quarantine workflow.
- Full Snowflake acceptance and Gold replay evidence are pending. The conditional Silver MERGE has local regression coverage, but its opt-in Snowflake verification has not yet run.

## Project files and evidence

- `dags/full_etl_pipeline/`: controller, standalone DAGs, and Silver publication helper.
- `dags/full_etl_pipeline/file_selection.py`: date validation, filename filtering, and earlier master dependency selection; covered by `tests/unit/test_file_selection.py`.
- `include/sql/`: initialization DDL, validation, MERGE, and reconciliation SQL.
- `tests/`: unit tests, local SQL regression tests, isolated Snowflake test, and fixture expectations.
- `scripts/validate_retail_dags.py`: DAG structure and configuration checks in Airflow.
- `evidence/silver_merge_checkpoint.md`: verification status, local test results, and historical run observations. Execution logs are under `logs/dag_id=retail_full_etl/` when retained locally.
- `retail_etl_architecture.mmd`: architecture diagram; `requirements_and_implementation.md`: assignment contract and remaining work.
- `architecture notes.md`: developer walkthrough of discovery, intermediate tables, loading, history, and transaction boundaries.
