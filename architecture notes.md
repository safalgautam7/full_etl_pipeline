# Retail ETL — Architecture Notes

This guide follows a file from discovery to reporting-ready Snowflake tables. Read it alongside `retail_etl_architecture.mmd`; see `README.md` for environment setup and run commands. Snowflake objects below belong to `RETAIL_DWH` unless stated otherwise.

## 1. Execution model

**Native backfill:** open `retail_full_etl` → Trigger → Backfill. Select Nov 9–13, set backfill Max active runs to 1, and leave Run backwards off. Airflow creates five runs of this existing DAG; each uses `data_interval_start` to select that day's files and scope Silver/Gold writes. The discovery task's past/downstream dependency includes final reconciliation, preventing the next day's processing until the previous day succeeds. No helper DAG, API connection, custom controller, audit tables, or bootstrap workflow is used.

The DAG has a daily UTC timetable, starting Nov 9, with `catchup=False`. Unpausing enables automatic daily runs as well as explicit native backfill. Manual single-run triggers retain the original bulk behavior; all-history publication statements below refer to manual bulk mode, not scheduled/backfill runs.

Displayed native run IDs use interval-end timestamps: `backfill__2026-11-10...` processes **Nov 9**. The five fixture business dates Nov 9–13 therefore appear under run IDs Nov 10–14. Use Logical Date/Data Interval Start to identify the processed date. Daily task-instance counts are 16/15/16/15/15; a manual 17-file bulk run has 29. This reflects dynamic ingestion mapping, not additional DAG definitions.

`retail_full_etl` contains three sequential TaskGroups, reused unchanged for every daily backfill run:

```text
Local CSVs → run_stg (raw ingestion) → run_tmp (validation) → run_gold (dimensions, facts, checks)
```

The controller reuses the task-building functions from the standalone layer DAGs; it does **not** trigger child DAG runs. All tasks share the controller run ID. A layer must succeed before the next starts. The DAG allows one active run and one active task, so mapped ingestion tasks are separate tasks but execute serially.

## 2. File discovery and identity

Implemented in `dags/full_etl_pipeline/stg_ingestion.py`:

1. `discover_files` reads the directory from the Airflow filesystem connection `file_con` (normally `/opt/airflow/data`, mounted from local `data/`).
2. It searches that directory, not subdirectories, for lowercase `.csv` files matching `<source>_YYYYMMDD.csv`. Supported sources are `item_master`, `store_master`, `pos_sales`, and `inventory_snapshot`. Other filenames are ignored; no recognized inputs fails the task. Filename dates are parsed during range selection and checked again during ingestion.
3. `daily_processing.processing_day()` uses native scheduled/backfill `data_interval_start`, never the wall clock. `select_day_files()` selects only that date's files and requires item/sales/inventory inputs; a store file is optional when an earlier accepted snapshot exists. For manual bulk/standalone runs, names are sorted and `file_limit=0` selects all, a positive limit selects the first N; standalone ingestion defaults to one. Date ranges belong in the native Backfill dialog, not Trigger JSON.
4. `ingest_one.expand` creates one task per selected filename. Each task computes a SHA-256 of the file bytes and derives stable identities:

   ```text
   SOURCE_FILE_ID = SHA256(filename + ":" + file checksum)
   SOURCE_ROW_ID  = SHA256(SOURCE_FILE_ID + ":" + data-row number)
   ```

The data-row number starts at 1 after the header. These identify a physical source row, not a business entity such as an item or sales line.

Before loading, the task checks `CONTROL.FILE_MANIFEST` by filename. An already-loaded file with the same checksum and matching STG row count is skipped. A changed checksum under the same filename fails deliberately. A matching file with an incomplete STG count is reprocessed to insert missing rows.

### Native backfill example

The Nov 9–13 UI backfill creates five original full-pipeline runs with **4, 3, 4, 3, 3 files** respectively. Only Nov 9 and Nov 11 include store files. Nov 10 reuses the already accepted Nov 9 store snapshot; no prior-date files are inserted into a daily run. A later starting date needs its earlier master dependencies already loaded. Missing inputs fail the day and block later processing. Existing bulk-loaded dates are replayable if the files match their loaded hashes/counts and validation decisions are unchanged; new/changed historical inputs require repair rather than automatic rebuilding.

Daily Silver classification/publication is restricted to that day's raw identities. SCD1 affects only keys observed that day, selecting their latest accepted descriptions so an older replay cannot undo a later rename. Sales facts are date-scoped. SCD2/inventory preparation reads all accepted history to preserve later boundaries on a bulk-loaded target, but MERGEs only versions starting or ending on the run date. An unchanged replay preserves surrogate keys, timestamps, and interval/current state. Daily monetary reconciliation plus global read-only integrity checks finish each run.

## 3. Bronze: how raw tables are loaded

For a new or recoverable file, ingestion performs these steps:

1. **Validate CSV structure.** Missing/blank headers, duplicate headers, malformed CSV, or incorrect row widths fail the task. Business errors such as negative quantities are retained for Silver validation.
2. **Create local JSONL.** Each CSV data row becomes a JSON object containing `source_row_id`, `source_row_number`, and `raw_payload`. Header names and source text values, including empty strings, are preserved. The original CSV is unchanged.
3. **Upload with `PUT`.** Both the CSV and generated JSONL go to the internal Snowflake stage `STG.CSV_LANDING`, under `_stg_ingest/<source>/<file_id>/source/` and `json/`. A stage stores files; it is not a database table.
4. **Load a buffer.** `COPY INTO` reads the JSONL into session-local temporary table `STG.STG_LOAD_BUFFER`. Copy errors abort the statement. Total rows and distinct source-row IDs must both equal the CSV row count.
5. **Publish transactionally.** Inside one `BEGIN`/`COMMIT`, an insert-only `MERGE` adds missing source-row IDs to the destination STG table, and another `MERGE` records the file as `LOADED` in the manifest. Errors roll back both changes. A final target-count check follows the commit.

| Source prefix | Raw destination | Accepted Silver destination |
| --- | --- | --- |
| `item_master` | `STG.ITEM_MASTER` | `TMP.ITEM_MASTER` |
| `store_master` | `STG.STORE_MASTER` | `TMP.STORE_MASTER` |
| `pos_sales` | `STG.POS_SALES` | `TMP.POS_SALES` |
| `inventory_snapshot` | `STG.INVENTORY_SNAPSHOT` | `TMP.INVENTORY_SNAPSHOT` |

STG stores `RAW_PAYLOAD` as `VARIANT`, plus filename, file date, row number, source IDs, and `BATCH_ID` (the ingestion Airflow run ID). Existing raw rows are never overwritten. The manifest stores file identity, checksum, destination, row count, status, and load lineage; it is not a logical business-day batch ledger.

## 4. Silver: parsing, validation, and quarantine

`retail_tmp_processing.py` runs item → store → sales → inventory tasks. Masters run first because sales and inventory reference accepted master snapshots. Each task uses `silver_processing.py` and SQL in `include/sql/silver/`.

**Build decisions first.** Each `validate_*.sql` creates a temporary `TMP.WORK_<SOURCE>` table from the corresponding STG table. SQL trims/normalizes values and uses `TRY_TO_*` conversions so invalid text can be classified rather than aborting conversion. Every raw row receives typed fields and a `REJECT_REASON`; a null reason means accepted.

Validation covers required keys, business dates matching file dates, valid types and flags, numeric constraints, arithmetic, duplicates, and applicable references. For sales and inventory, reference checks select the latest accepted item/store snapshot date **on or before** the business date, then look for the key in that snapshot. Future master snapshots are not used backward. Sales duplicates use `(TRANSACTION_ID, LINE_NUMBER)`; the earliest eligible physical row wins, while invalid rows keep their primary business-error reason.

**Publish accepted and rejected rows together.** Before publication, checks require exactly one work decision per STG identity, no foreign work rows, and `accepted + rejected = raw`. Then one transaction:

- Removes only rows switching destination: accepted → rejected leaves TMP; rejected → accepted leaves DQ.
- Conditionally merges accepted rows into the typed TMP table on `SOURCE_ROW_ID`.
- Conditionally merges rejected rows into `DQ.QUARANTINE_RECORDS` on `(SOURCE_TYPE, SOURCE_ROW_ID)`, retaining raw payload, lineage, reason code/detail, and rejection timestamp.
- Verifies counts, classification, and that no row is both accepted and rejected; commits only if checks pass.

Unchanged values do not update timestamps. DQ represents the **current decision**, not an append-only rejection history. Rejected rows remain in STG and are excluded from normal Gold inputs. Despite its name, the `TMP` schema contains permanent accepted tables; only the `WORK_*` tables are temporary.

## 5. Gold: dimensions and facts

`retail_gold_pipeline.py` executes this order:

```text
DIM_SUBCLASS → DIM_DISTRICT → DIM_ITEM → DIM_LOCN → FCT_SALES → FCT_INVENTORY → reconcile_gold
```

### Dimensions

- **`DIM_SUBCLASS` / `DIM_DISTRICT`: SCD Type 1.** Select the latest accepted description per business key and merge directly into TGT. Changes update in place; no description history is kept.
- **`DIM_ITEM` / `DIM_LOCN`: SCD Type 2.** `prepare_dim_item.sql` and `prepare_dim_locn.sql` build historical work tables from all accepted snapshots. Attribute hashes and `LAG` find changes; `LEAD` determines the next version's start. Repeated unchanged snapshots do not create versions. Subclass/district description-only changes do not create item/location versions.
- Dimension MERGEs match `(ITEM_ID, EFFECTIVE_FROM)` or `(STORE_ID, EFFECTIVE_FROM)`. New versions receive generated surrogate keys (`ITEM_SK`, `LOCN_SK`); existing versions retain their keys. `IS_CURRENT` identifies the last version.

History uses **half-open intervals**: `EFFECTIVE_FROM <= business_date < EFFECTIVE_TO`; a null end date means open-ended. For example, a version starting November 12 replaces the previous version for transactions on November 12 itself.

### Sales fact

`prepare_fct_sales.sql` creates `TMP.WORK_FCT_SALES` from accepted sales, resolving item and location versions as of `SALE_DATE`. Preflight requires nonempty input, unique source-row IDs and sales-line keys, and exactly one historical match for each dimension. Missing or ambiguous matches fail rather than silently dropping rows.

`merge_fct_sales.sql` publishes one row per `(TRANSACTION_ID, LINE_NUMBER)` into `TGT.FCT_SALES`, with measures, historical surrogate keys, and source-row lineage. It inserts new lines and updates only changed values. This key assumes transaction IDs are globally unique.

### Inventory fact

Inventory is **change-compressed position history**, not one Gold row per daily snapshot:

1. `resolve_inventory_observations.sql` creates `TMP.WORK_INV_OBSERVATIONS`, resolving every accepted daily observation to historical keys. Preflight checks unique `(STORE_ID, ITEM_ID, SNAPSHOT_DATE)` and exactly one item/location match.
2. `prepare_fct_inventory.sql` creates `TMP.WORK_FCT_INVENTORY`. For each store/item, a change in quantity, cost, value, or historical dimension keys starts a version. Unchanged observations collapse into the existing interval.
3. `merge_fct_inventory.sql` merges into `TGT.FCT_INVENTORY` on `(STORE_ID, ITEM_ID, EFFECTIVE_FROM)`, closing intervals and inserting versions without deleting prior history or regenerating existing version keys.

For example, identical Monday/Tuesday positions followed by a Wednesday change produce two versions, not three rows.

## 6. Intermediate objects and transaction scope

| Temporary table | Purpose |
| --- | --- |
| `STG.STG_LOAD_BUFFER` | JSONL copy buffer before raw publication |
| `TMP.WORK_ITEM_MASTER`, `WORK_STORE_MASTER`, `WORK_POS_SALES`, `WORK_INVENTORY_SNAPSHOT` | One validation decision per source row |
| `TMP.WORK_DIM_ITEM`, `WORK_DIM_LOCN` | Derived SCD2 change boundaries and intervals |
| `TMP.WORK_FCT_SALES` | Sales with historical keys and preflight counts |
| `TMP.WORK_INV_OBSERVATIONS` | Every accepted daily inventory observation with resolved keys |
| `TMP.WORK_FCT_INVENTORY` | Compressed inventory versions ready to merge |

These are Snowflake session-local temporary tables, recreated by their tasks and unavailable after the session ends. Preparation, checking, and publication must use the same connection. Temporary DDL is performed before explicit publication transactions. Bronze publication is atomic per file; Silver publication is atomic per source; Gold publishes through individual atomic MERGE statements. **There is no transaction spanning the whole DAG**—earlier successful loads remain if a later task fails.

## 7. Final checks and developer reference

`reconcile_gold` runs `include/sql/reconcile/reconcile_gold.sql` and `check_gold_integrity.sql`. Per sales date, both row counts and net amounts must satisfy:

```text
Raw sales = Accepted sales + Rejected sales
Gold sales = Accepted sales
```

Checks also cover historical intervals/current versions, historical keys, sales-line matching, rejected sales appearing in Gold, and coverage of accepted inventory observations by the correct versions. Inventory observation and version counts need not match. Unparseable raw sales amounts fail reconciliation. Results are logged in Airflow, not persisted; mismatches fail the DAG but do not undo previously committed loads.

**Where to change behavior:**

- `config/retail_etl.json`: file/Snowflake connections, stage, manifest, and STG destinations.
- `config/retail_tmp.json`: Silver destinations and work-table identifiers. Validation SQL currently contains hardcoded object names, so configuration alone does not retarget every query.
- Airflow Variable `retail_gold_config` (template: `config/retail_gold_config.json`): Gold SQL paths, connection, and preflight table names. Gold SQL also contains hardcoded objects.
- `include/sql/silver/`: parsing and validation rules; `silver_processing.py`: publication checks and transaction handling.
- `include/sql/gold/`: history preparation and target MERGEs; `include/sql/ddl/`: TMP/DQ and TGT definitions. STG/schema/stage bootstrap DDL is not supplied; follow `README.md` prerequisites.

**Operational boundaries:** Manual bulk mode reads all available inputs. Native daily runs publish their date's data, with historical reads/affected interval closures allowed. Unchanged replay over an earlier bulk load is supported; changed historical files or Silver/DQ decisions are blocked before publication. Do not run standalone layers or external writers concurrently. Same-name file corrections and automatic downstream removal of sales newly rejected after rule changes remain unsupported.
