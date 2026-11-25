# Retail ETL — Architecture Notes

This document explains the implemented system and its boundaries. See
[README.md](README.md) for reproducible setup, manual DDL, both connections,
the single required Variable, pool, run commands and Power BI setup. The
[Mermaid diagram](retail_etl_architecture.mmd) is a visual companion. Paths
below are relative to the repository root; objects belong to `RETAIL_DWH`.

The sales-dashboard extension adds Department/Class and Location Type end to
end and exposes a separate four-view **MART_SALES star**. Its rollout, current
2,328-file history, backups and verification are documented in
[sales_reporting_reload.md](evidence/sales_reporting_reload.md). Existing TGT
inventory tables remain available but are not imported into the sales-only model.

## 1. System boundary and runtime

```text
CSV files on mounted worker filesystem
  → STG/Bronze: raw records and active file manifest
  → TMP/Silver: accepted typed observations + DQ current dispositions
  → TGT/Gold: calendar, historical dimensions, sales and two inventory grains
  → read-only reconciliation gates
  → Power BI semantic model
```

Airflow 3.3.2 uses CeleryExecutor, Redis as broker and PostgreSQL as metadata/
result backend. Workers execute Python and open Snowflake sessions. Snowflake
stores files in a named internal stage and executes warehouse transformations.
CSV payloads do not pass through XCom; only selected filenames/task results do.
Docker Compose supplies a development environment, not production hardening.

**Initialization is explicit.** `include/sql/ddl/00_bootstrap_warehouse.sql`
creates the warehouse/database/schemas/raw tables/stage; the manifest,
TMP/DQ, dimension, fact and reporting/correction DDL follow in README order.
`airflow-init` initializes Airflow metadata, not Snowflake business objects.
`IF NOT EXISTS` protects existing objects from replacement but does not migrate
incompatible schemas. Several SQL names are hardcoded: JSON edits alone do
not make this a database-agnostic implementation.

## 2. Orchestration and daily scope

**Controller:** `dags/full_etl_pipeline/retail_full_etl.py`.

```text
run_stg.discover_files
  → run_stg.ingest_one[one mapped instance per file]
  → run_tmp.load_tmp_item_master
  → run_tmp.load_tmp_store_master
  → run_tmp.load_tmp_pos_sales
  → run_tmp.load_tmp_inventory_snapshot
  → run_gold.load_dim_date
  → run_gold.load_dim_subclass
  → run_gold.load_dim_district
  → run_gold.load_dim_item
  → run_gold.load_dim_locn
  → run_gold.load_sales
  → run_gold.load_inventory [versioned + daily publication]
  → run_gold.reconcile_gold
```

There are **14 task definitions**, not 14 fixed runtime instances. A four-file
day has 17 instances; a three-file day has 16; all 33 fixture files in one manual
bulk run give 46. TaskGroups reuse unwrapped standalone task-building factories;
they do not trigger child DAGs or define warehouse transactions.

The full DAG has a daily UTC timetable starting November 9, 2026,
`catchup=False`, `max_active_runs=1` and `max_active_tasks=1`. The native
Backfill dialog repeats the complete DAG per interval, oldest first, with
backfill concurrency separately set to one. Discovery's `depends_on_past` and
`wait_for_downstream`, including a direct edge to final reconciliation, gate
the next day's processing on the previous full run's success.

`daily_processing.processing_day()` returns `data_interval_start` only for
scheduled/backfill runs of the full DAG. Manual single triggers and standalone
layers retain bulk behavior. A native run ID dated November 10 at midnight
refers to the business interval beginning **November 9**. `catchup=False`
prevents automatic historical catchup; it does not disable today's schedule.
Unpausing can create ordinary runs requiring inputs beyond the fixture.
Manual runs stay queued while a DAG is paused.

### Per-date publication versus historical reads

- `select_day_files()` requires item/sales/inventory for exactly that date;
  stores are optional when applicable prior accepted masters exist.
- `process_daily_source()` restricts raw decisions to that date. Existing
  accepted sales keys are checked globally, not just within the current day.
- SCD1 restricts affected keys to the day but chooses the latest accepted
  description globally, so old replay cannot undo later labels.
- SCD2 and inventory preparation read all accepted history to preserve later
  intervals. Their normal daily MERGEs publish versions starting/ending that day.
- Sales publishes its business date; daily inventory publishes its snapshot date.
- Monetary reconciliation is date-scoped; structural/reporting integrity remains global.

Unchanged daily replay after a prior bulk load verifies file hashes/raw coverage
and existing Silver decisions. New historical dates, changed files and changed
decisions are not silently accepted by native backfill.

## 3. Configuration and writer coordination

| Resource | Owner and responsibility |
| --- | --- |
| `file_con` connection | Filesystem root, normally `/opt/airflow/data` via FSHook |
| `retail_snowflake` connection | Snowflake account, authentication, database/schema, role and warehouse |
| `config/retail_etl.json` | Direct STG configuration: connections, stage, manifest, source destinations |
| `config/retail_tmp.json` | Direct Silver mappings and quarantine configuration |
| `retail_gold_config` Variable | Task-time Gold configuration, work objects, five dimensions and calendar bounds; JSON template under config/ |
| `retail_writer` pool | Exactly one slot shared by main/layer/correction tasks |

Runtime configuration reads avoid querying Variables/connections merely to
construct the controller graph. SQL identifiers are validated; data values are
bound parameters. Gold filenames are trusted simple SQL basenames.

The pool serializes participating **tasks across DAGs**. A correction holds a
slot for its entire operation. It is not an external SQL lock or a whole-run
lock for ordinary multi-task DAGs. Keep standalone DAGs paused in normal
operation; pause/drain ordinary work and stop external writers for maintenance.
Fernet protects stored connection credentials; `.env`/private keys/tokens never
belong in source control. Approver text in correction JSON is not authentication.

## 4. Bronze: source identity and publication

**Implementation:** `stg_ingestion.py`, with naming/date helpers in
`daily_processing.py`.

Discovery scans only the source root for supported lowercase
`<source>_YYYYMMDD.csv` names, not subdirectories. Thus correction candidates
under `data/corrections/` cannot accidentally enter normal ingestion.

```text
SOURCE_FILE_ID = SHA256(filename + ':' + SHA256(exact file bytes))
SOURCE_ROW_ID  = SHA256(SOURCE_FILE_ID + ':' + CSV data-record number)
```

Record numbering starts after the header; quoted multiline CSV records are
one record, not multiple identities. Source hashes identify provenance, not
business keys. Duplicate transactions can have distinct raw identities.

For each selected file, the worker:

1. Checks `CONTROL.FILE_MANIFEST`. Matching loaded hash/count yields a successful
   SKIPPED return. Changed bytes fail normal ingestion. Matching incomplete
   loads can recover by inserting missing identities.
2. Uses strict CSV parsing to reject structural errors. Business defects remain
   raw strings. JSONL preserves exact headers and text values, including blanks.
3. PUTs CSV and JSONL under content-based prefixes in `STG.CSV_LANDING`.
4. COPYs only JSONL into session-local `STG.STG_LOAD_BUFFER`, aborting on errors
   and checking count plus identity uniqueness against the source record count.
5. In one transaction, insert-only MERGEs STG rows and MERGEs the manifest;
   commits or rolls back both. A target count is rechecked after commit.

Both stage files remain unless retention removes them; local generated JSONL
is temporary. Stage storage is not a table. The raw tables contain lineage,
`RAW_PAYLOAD VARIANT` and ingestion metadata. Normal ingestion never updates
an existing raw record. Approved corrections archive the superseded envelope
before removing it from the active STG view of history.

## 5. Silver: validity is separate from provenance

**Implementation:** `retail_tmp_processing.py`, `silver_processing.py`,
and `include/sql/silver/`.

Item → store → sales → inventory order ensures reference validation can use
accepted masters. Each classifier builds a temporary work table with typed
columns, raw payload/lineage and one primary `REJECT_REASON`. `TRY_TO_*`
conversions classify malformed business values instead of aborting the query.
Ordered CASE rules preserve primary errors; duplicate ranking lets the earliest
eligible record win instead of allowing an invalid duplicate to displace it.

Rules cover required keys, filename/business-date agreement, types/flags,
numeric constraints, arithmetic, duplicate snapshots/positions/transaction
lines, and master references. Sales duplicate keys are global
`(TRANSACTION_ID, LINE_NUMBER)`. Values use fixed-precision money types.

Reference validation chooses the latest **accepted full master snapshot date
on or before** the fact date, then tests key membership in that snapshot. It
does not find an arbitrary older row for an absent entity, apply future masters
backward, or require the entity's active flag to be true.

Before normal publication, `check_work.sql` proves nonempty raw coverage,
unique identities, one decision per raw record and accepted + rejected = raw.
Inside one transaction, the publisher removes disposition flips, conditionally
MERGEs accepted rows into TMP and rejected rows into DQ, and verifies exact
counts/classification/exclusivity before committing. Unchanged rows retain
`PROCESSED_AT`/`REJECTED_AT`.

The accepted TMP tables are **permanent**. `WORK_*` tables alone are temporary.
DQ stores current dispositions, not every validation attempt. Rejected rows
remain in active STG until their file is superseded. Ordinary source publication
does not itself repair already published Gold facts after a rule change.

## 6. Gold: calendar and historical business dimensions

**Orchestrator:** `retail_gold_pipeline.py`; SQL under `include/sql/gold/`.

### Shared calendar

`reporting.calendar_bounds()` validates configured complete-year coverage;
`load_calendar()` runs `load_dim_date.sql` before other Gold tasks. Default
coverage is 2020–2035, independent of observed transactions or wall-clock dates.
`DIM_DATE` has one actual DATE, a deterministic `YYYYMMDD` key, calendar
attributes, ISO weekday/week/**week-year**, sort keys and month boundaries.
Coverage is continuous; fact preflight requires exactly one matching date.
Future dates are not considered evidence of loaded business history.

A sales DATE column can validly join the calendar directly; no redundant
numeric sales date-key migration is required. Fiscal calendars/holidays need
business approval before implementation. SCD validity dates are not substitutes
for a reporting calendar.

### Business dimensions

- `DIM_SUBCLASS` and `DIM_DISTRICT` are **SCD1**: latest accepted descriptions
  update in place. Historical reports intentionally show their latest labels.
- `DIM_ITEM` and `DIM_LOCN` are **SCD2**: preparation hashes tracked attributes,
  uses LAG to find changed states and LEAD over boundaries to derive ends.
  Unchanged observations collapse; description-only SCD1 changes do not create
  extra item/location versions.
- MERGEs match `(natural ID, EFFECTIVE_FROM)`, preserve surviving SKs, and insert
  new identity-generated SKs. Source-identity guards also update corrected
  boundary provenance when values are otherwise unchanged.

Intervals are **half-open**: `EFFECTIVE_FROM <= business_date < EFFECTIVE_TO`;
null means open-ended. A version beginning on November 12 applies on November
12 itself; no ambiguous shared boundary day or subtract-one-day arithmetic.

## 7. Gold facts: distinguish event, interval and observation grains

| Fact | Grain | Important behavior |
| --- | --- | --- |
| `FCT_SALES` | Transaction line | Resolves item/location at SALE_DATE, not IS_CURRENT |
| `FCT_INVENTORY` | Store/item state-change start | Compressed validity intervals |
| `FCT_INVENTORY_DAILY` | Store/item observed snapshot date | Explicit daily reporting balances; no inferred missing days |

`prepare_fct_sales.sql` retains every accepted sale and counts historical
matches/business-key occurrences before `merge_fct_sales.sql`. Missing or
ambiguous matches fail instead of silently dropping rows. Ordinary preflight
also requires nonempty input. Matched updates use null-safe differences,
retaining SK/timestamp on unchanged replay.

Inventory first runs `resolve_inventory_observations.sql` for **every accepted
observation**, counting historical matches and duplicate daily positions.
Only after validation does `prepare_fct_inventory.sql` compress unchanged
quantity/cost/value/historical-key states into intervals. The interval MERGE
preserves earlier versions. `merge_fct_inventory_daily.sql` separately publishes
the same validated observations at their original daily grain.

For observed stock 10, 10, 12 across three days, interval history has two rows
and daily history has three. Joining a calendar to EFFECTIVE_FROM would select
only change starts, not daily balances. Ordinary open intervals may span absent
observations; they do not authorize inventing reporting balances indefinitely.

Sales quantities/net sales are additive. Stock is **semi-additive**: sum across
items/stores on a date, not across dates. Unit prices/costs are not additive
either. Closing/average stock and data completeness need explicit BI measures.

## 8. Reconciliation and evidence

`reconcile_gold()` runs three read-only SQL files:

| File under `include/sql/reconcile/` | Contract |
| --- | --- |
| `reconcile_gold.sql` | Per-observed-date raw = accepted + rejected; Gold = accepted, for sales counts and money |
| `check_gold_integrity.sql` | Historical intervals/current flags, duplicate business/SK keys, correct dated fact keys, accepted coverage and rejected-sales exclusion |
| `check_reporting_integrity.sql` | Calendar uniqueness/keys/continuity/coverage, daily inventory uniqueness, exact accepted-observation coverage and stale-row exclusion |
| `check_sales_mart_integrity.sql` (enabled by Gold config) | Flattened dimension keys/classifications, fact references, gross/net identity and ISO PY date mapping |

Accounting must return PASS per date; every integrity failure count must be
zero. Inventory version counts do not equal observation counts by design.
Unparseable raw sales money cannot be accounted for and intentionally fails
reconciliation even when quarantined. These checks derive expectations from
warehouse inputs, not hardcoded fixture totals.

Ordinary results are logged in Airflow rather than persisted. A final failure
does not roll back earlier tasks. Use `evidence/verify_reporting_corrections.sql`
for read-only inspection and pair warehouse state with actual run dates/logs
to prove chronological execution; a final table alone cannot prove backfill order.

## 9. Approved correction path

**Entry point:** `retail_historical_correction.py`, one task holding the shared
writer pool throughout staging and repair. **Core:** `corrections.py`.

The contract is an approved **complete replacement of one already loaded
source/date file**, staged outside normal discovery. The expected active SHA
prevents stale approval from overwriting another version. A byte-identical
already-active replacement is a NOOP after checking raw coverage. Structural
validation/checksum stability/path containment precede publication.

The core records RUNNING and precreates empty temporary work shapes. Then:

```text
BEGIN
  validate all active manifest/raw identities and coverage
  archive superseded raw + manifest; snapshot existing DQ
  replace active STG file and manifest pointer
  revalidate all active Silver history, masters first
  delete obsolete decisions and conditionally merge current results
  rebuild SCD boundaries; delete obsolete versions/unused lookup keys
  resolve fact keys against repaired history
  remove stale facts/observations; merge sales and both inventory grains
  run accounting + historical + reporting integrity
  record SUCCEEDED
COMMIT
```

Exceptions roll back business changes and write FAILED/error separately.
`RepairSQL` turns preprepared CTAS SELECTs into DELETE/INSERT SELECT during
repair: **no DDL runs inside this transaction**, because Snowflake DDL would
break the intended rollback scope. New versions use permanent identities;
surviving version/business keys keep SKs. Identity gaps after rollback are normal.

All loaded active history is revalidated because master corrections can affect
later accepted **and previously rejected** observations. That correctness-first
choice costs compute, transaction duration and archive growth; affected-key
optimization needs separate design and tests before business-scale deployment.

STG and FILE_MANIFEST remain the active version. `RAW_VERSION_ARCHIVE` and
`MANIFEST_VERSION_ARCHIVE` retain superseded records; `DQ_DECISION_ARCHIVE`
retains prior dispositions at correction time. A separate historical reader
must explicitly read archives. Source files remain content-addressed on the stage.

Operationally, pause/drain ordinary work and BI and stop external writers.
Only after success synchronize the root input to the approved corrected bytes.
Worker termination requires checking audit, active manifest and old session
termination before retry. Calendar extension/stage uploads happen outside
the transaction. Header-only replacements can remove a date's observations,
but missing master observations do not generate automatic inactive/tombstone
events. Partial patches, new historical dates, multi-file atomic repair and
arbitrary rule deployments are not supported by this interface.

## 10. Object lifetime and atomicity

| Object/scope | Lifetime / atomicity |
| --- | --- |
| Raw/accepted/DQ/Gold/control tables | Permanent; manually initialized |
| `STG.STG_LOAD_BUFFER` | Session-local temporary COPY buffer |
| `STG.REPLACEMENT_INPUT` | Session-local approved correction input |
| `TMP.WORK_*` | Session-local validation/history/fact work; producer and consumer share one connection |
| Normal Bronze | One explicit transaction per file: raw + manifest |
| Normal Silver | One explicit transaction per source: accepted/rejected publication + checks |
| Normal Gold | Individual MERGEs; two inventory MERGEs are not jointly transactional |
| Ordinary reconciliation | Read-only failure gate after earlier commits |
| Correction | One DML transaction for active replacement/archives/Silver/Gold/checks/SUCCEEDED |

Session-local temporary tables cannot be inspected later from another
Snowsight session. Uploaded stage files and local worker temporary JSONL have
different lifetimes. No distributed transaction spans PUT, Airflow tasks and
all Snowflake objects in an ordinary run.

## 11. Design decisions and release boundaries

| Decision | Why / cost |
| --- | --- |
| Separate raw and validity layers | Retain rejected evidence instead of silently cleaning it away |
| Native Airflow backfill | Reuse scheduler history/dependencies instead of a parallel custom controller |
| Changed files blocked in normal ingestion | Avoid mistaking changed physical identities for safe replay |
| SCD1 and SCD2 together | Latest descriptive labels versus historically correct entity attributes/assignment |
| Warehouse calendar | One complete governed calendar across reports/tools, not hidden per-report auto-date tables |
| Both inventory facts | Compact changes plus equality-joinable observed daily reporting; extra storage |
| Conditional MERGEs | Stable replay timestamps/SKs; not an implicit deletion/correction contract |
| All-history approved repair | Conservative dependency coverage and rollback; higher operational cost |
| One-slot pool | Simple writer exclusion for participating tasks; lower throughput and no external protection |

`scripts/validate_retail_dags.py` checks actual Airflow graphs without loading
business data. Unit tests cover contracts/scoping; local SQL tests exercise
classifiers, replay, corrections and rollback using dialect adaptations.
Opt-in Snowflake tests require a guarded disposable `RETAIL_ETL_TEST_*`
database and private external connector configuration. Neither local SQL nor
graph construction certifies actual PUT/COPY, Snowflake transactions, worker
permissions or business-scale performance.

The user reported successful deployed Gold bootstrap. Correction-specific
isolated Snowflake acceptance remains a separate gate. Older SCD evidence
tests refer to absent `evidence/verify_etl_stages.sql`; that is a documented
baseline gap, not an available setup dependency. Sales transaction IDs are
assumed globally unique. Production rollout additionally requires secure
deployment, controlled approval, backup/Time Travel, monitoring, retention,
validated BI measures and performance testing.
