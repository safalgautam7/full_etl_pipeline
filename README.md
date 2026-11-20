# Retail ETL: Airflow → Snowflake → Power BI

An executable CSV-to-warehouse pipeline with raw lineage, accepted/rejected
decisions, historical dimensions, sales, daily inventory, and controlled
historical corrections. This guide covers a **fresh checkout and empty
warehouse**, not just an already configured developer machine.

For internals and design decisions, read [architecture notes.md](architecture%20notes.md)
and [the Mermaid diagram](retail_etl_architecture.mmd).

## 1. What you will run

```text
data/*.csv
  → run_stg: raw VARIANT + file manifest
  → run_tmp: typed accepted rows + DQ quarantine
  → run_gold: calendar + dimensions + facts
  → reconciliation: accounting, historical keys, reporting coverage
```

The main DAG is **`retail_full_etl`**, with 14 task definitions and dynamically
mapped file ingestion. Gold contains these **eight** tables:

| Table | Grain / purpose |
| --- | --- |
| `DIM_DATE` | One calendar date; shared reporting calendar |
| `DIM_SUBCLASS`, `DIM_DISTRICT` | One natural key; latest description, SCD1 |
| `DIM_ITEM`, `DIM_LOCN` | One entity/version start; effective-dated SCD2 |
| `FCT_SALES` | One `(TRANSACTION_ID, LINE_NUMBER)` |
| `FCT_INVENTORY` | One `(STORE_ID, ITEM_ID, EFFECTIVE_FROM)` change interval |
| `FCT_INVENTORY_DAILY` | One accepted `(STORE_ID, ITEM_ID, SNAPSHOT_DATE)` observation |

There is no aggregate mart. Snowflake performs transformations; Airflow
orchestrates them. Ordinary runs are **not** one warehouse-wide transaction.

## 2. Prerequisites and checkout

- Git; Docker Engine/Desktop with Compose v2; a Linux/WSL shell for the commands below.
- At least 4 GB RAM, 2 CPUs and 10 GB free disk for the local Compose stack;
  more memory is recommended.
- A Snowflake account, an existing login with supported noninteractive
  authentication, and an administrator/deployment role able to create the
  warehouse/database/objects and grant access. Network policies must allow the worker.
- Optional host Python 3 and `uv` for credential-free local tests.

```bash
git clone https://github.com/safalgautam7/full_etl_pipeline.git
cd full_etl_pipeline
```

If copying rather than cloning, copy tracked project files and `data/`.
**Do not copy another developer's `.env`, credentials, `logs/`, generated
`config/airflow.cfg`, Python caches, or Airflow metadata volume.** Existing
installations must preserve their own secrets and metadata, not replace them.

The runtime mounts `dags/`, `include/`, `config/`, `data/`, `scripts/`, `plugins/`,
and `logs/` under `/opt/airflow/`. Run Compose commands from the repository root.

## 3. Create your local environment file

For a **new checkout only**:

```bash
cp .env.example .env
mkdir -p logs plugins config
id -u
```

Edit `.env`:

1. Set `AIRFLOW_UID` to the printed Linux/WSL user ID (Docker Desktop users can
   normally retain `50000`).
2. Generate a Fernet key and a **different** random JWT secret:

   ```bash
   docker run --rm --entrypoint python apache/airflow:3.3.2 -c 'from cryptography.fernet import Fernet; import secrets; print("FERNET_KEY=" + Fernet.generate_key().decode()); print("AIRFLOW__API_AUTH__JWT_SECRET=" + secrets.token_urlsafe(48))'
   ```

   Replace the corresponding placeholders in `.env`. Do not post this output
   in screenshots, tickets or Git. Keep the Fernet key stable once connections
   are saved; changing it without rotation can make existing credentials unreadable.
3. Set `_AIRFLOW_WWW_USER_USERNAME` and a strong `_AIRFLOW_WWW_USER_PASSWORD`.
4. Retain the Airflow image and provider versions in `.env.example` for this
   tested development setup. `_PIP_ADDITIONAL_REQUIREMENTS` installs providers
   at container startup; production should use a pinned custom image instead.

The supplied Compose file uses Airflow **3.3.2**, CeleryExecutor, PostgreSQL,
Redis and FAB authentication. It is a local-development stack, **not a hardened
production deployment**. Never commit `.env`, private keys, tokens or passwords.
An exposed token must be revoked, not merely removed from a document.

## 4. Create Snowflake objects manually — required

Airflow does **not** run initialization DDL. Open Snowsight SQL worksheets and
execute the following repository files **in this exact order**:

| Order | File | Creates |
| --- | --- | --- |
| 1 | [00_bootstrap_warehouse.sql](include/sql/ddl/00_bootstrap_warehouse.sql) | Warehouse, database, five schemas, internal stage and four raw STG tables |
| 2 | [01_file_manifest.sql](include/sql/control/01_file_manifest.sql) | Active file registry |
| 3 | [create_tmp_dq_tables.sql](include/sql/ddl/create_tmp_dq_tables.sql) | Four typed accepted TMP tables and quarantine |
| 4 | [create_tgt_dimensions.sql](include/sql/ddl/create_tgt_dimensions.sql) | Four business dimensions |
| 5 | [create_tgt_facts.sql](include/sql/ddl/create_tgt_facts.sql) | Sales and versioned inventory |
| 6 | [create_reporting_correction_tables.sql](include/sql/ddl/create_reporting_correction_tables.sql) | Calendar, daily inventory and correction audit/archive tables |

**Review the role/warehouse before executing.** The lab defaults are
`SYSADMIN`, `SNOWFLAKE_LEARNING_WH`, and database `RETAIL_DWH`. Some statements
explicitly select the role/warehouse. If your deployment uses another
warehouse, update those statements and the Airflow connection consistently.
Creating/resuming a warehouse incurs Snowflake charges. `AUTO_SUSPEND=60`
limits idle time on a newly created lab warehouse.

All creation statements use `IF NOT EXISTS`; they do not truncate or replace
populated objects. They also do **not** migrate an existing incompatible schema.
Never use `CREATE OR REPLACE` on populated warehouse tables as a setup shortcut.

SQL currently hardcodes `RETAIL_DWH` in several layers. Changing only an Airflow
connection or JSON database name does not retarget the entire pipeline.

### Runtime role and authentication

For a lab, an authorized existing login can use `SYSADMIN`. For shared/business
use, create a dedicated runtime role after all six scripts. The following is
an administrator-run example for this dedicated database; replace
`YOUR_EXISTING_SNOWFLAKE_USER` with the actual Snowflake login identifier:

```sql
USE ROLE SECURITYADMIN;
CREATE ROLE IF NOT EXISTS RETAIL_ETL_ROLE;
GRANT ROLE RETAIL_ETL_ROLE TO USER YOUR_EXISTING_SNOWFLAKE_USER;
GRANT USAGE ON WAREHOUSE SNOWFLAKE_LEARNING_WH TO ROLE RETAIL_ETL_ROLE;
GRANT USAGE ON DATABASE RETAIL_DWH TO ROLE RETAIL_ETL_ROLE;
GRANT USAGE ON SCHEMA RETAIL_DWH.STG TO ROLE RETAIL_ETL_ROLE;
GRANT USAGE ON SCHEMA RETAIL_DWH.TMP TO ROLE RETAIL_ETL_ROLE;
GRANT USAGE ON SCHEMA RETAIL_DWH.DQ TO ROLE RETAIL_ETL_ROLE;
GRANT USAGE ON SCHEMA RETAIL_DWH.TGT TO ROLE RETAIL_ETL_ROLE;
GRANT USAGE ON SCHEMA RETAIL_DWH.CONTROL TO ROLE RETAIL_ETL_ROLE;
GRANT CREATE TABLE ON SCHEMA RETAIL_DWH.STG TO ROLE RETAIL_ETL_ROLE;
GRANT CREATE TABLE ON SCHEMA RETAIL_DWH.TMP TO ROLE RETAIL_ETL_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE RETAIL_DWH TO ROLE RETAIL_ETL_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN DATABASE RETAIL_DWH TO ROLE RETAIL_ETL_ROLE;
GRANT READ ON STAGE RETAIL_DWH.STG.CSV_LANDING TO ROLE RETAIL_ETL_ROLE;
GRANT WRITE ON STAGE RETAIL_DWH.STG.CSV_LANDING TO ROLE RETAIL_ETL_ROLE;
```

These grants deliberately cover correction writes across this database. Review
and narrow them for your security policy; use a separate read-only role for
Power BI, not the ETL writer role. Provision credentials/key-pair authentication
through your Snowflake administrator; this repository does not create users or secrets.

## 5. Start Airflow

```bash
docker compose up airflow-init
docker compose up -d
docker compose ps
```

Wait for initialization to complete successfully and services to become healthy.
Open **http://localhost:8080** and sign in using your `.env` UI credentials.
Snowflake resources are separate from Airflow's PostgreSQL metadata database.

DAGs default to paused. Keep them paused while creating connections, the
Variable and the pool. If a provider import fails, inspect the relevant
container logs; changing startup provider requirements requires recreating
the affected containers, not restarting an individual task.

## 6. Create every required connection, Variable and pool

### Connection 1: `file_con`

In **Admin → Connections → Add** (or the Connections page in your UI layout):

| Field | Value |
| --- | --- |
| Connection ID | `file_con` |
| Connection type | File (path) / `fs` |
| Extra JSON | `{"path":"/opt/airflow/data"}` |
| Other fields | Leave blank |

Alternatively:

```bash
docker compose exec -T airflow-scheduler airflow connections add file_con --conn-type fs --conn-extra '{"path":"/opt/airflow/data"}'
```

Use the **container path**, not a Windows drive path or host `/mnt/...` path.

### Connection 2: `retail_snowflake`

Create this through the UI so credentials are not put in shell history:

| Field | Value |
| --- | --- |
| Connection ID | `retail_snowflake` |
| Connection type | Snowflake |
| Login | Your Snowflake login name, not your GitHub username |
| Password | Password only if permitted by your account's noninteractive authentication policy; for an encrypted private key, its passphrase |
| Schema | `STG` |
| Extra JSON | Account, database, warehouse, role and authentication options below |

Common Extra (replace the account and choose the role actually granted to you):

```json
{
  "account": "YOUR_ORG-YOUR_ACCOUNT",
  "database": "RETAIL_DWH",
  "warehouse": "SNOWFLAKE_LEARNING_WH",
  "role": "RETAIL_ETL_ROLE"
}
```

Use the Snowflake account identifier supplied by your administrator, **not**
`https://...` or an entire Snowsight URL. Leave Host/Port blank unless your
network setup explicitly requires them. Do not select browser-interactive
authentication for unattended Celery workers.

For key-pair authentication, have the administrator register the public key.
Place the private key locally at `plugins/secrets/retail_user_key.p8` (ignored
by Git), make it readable only by the container's Airflow user, and add:

```json
{
  "account": "YOUR_ORG-YOUR_ACCOUNT",
  "database": "RETAIL_DWH",
  "warehouse": "SNOWFLAKE_LEARNING_WH",
  "role": "RETAIL_ETL_ROLE",
  "authenticator": "SNOWFLAKE_JWT",
  "private_key_file": "/opt/airflow/plugins/secrets/retail_user_key.p8"
}
```

Never paste private-key contents into this README or a committed config file.
Production should use a proper secrets backend and controlled key mounting.

### The only required Airflow Variable: `retail_gold_config`

In **Admin → Variables**, create/update key **`retail_gold_config`** and paste
the **entire JSON object** from [config/retail_gold_config.json](config/retail_gold_config.json).
Do not paste a filename or JSON-encode it a second time. Or run:

```bash
docker compose exec -T airflow-scheduler bash -lc 'airflow variables set retail_gold_config "$(cat /opt/airflow/config/retail_gold_config.json)"'
```

This includes five dimension names, SQL paths, work-table names and the default
calendar range **2020-01-01 to 2035-12-31**. Choose complete years covering your
business history, comparison periods and future horizon before loading facts.

STG reads `config/retail_etl.json` directly; Silver reads `config/retail_tmp.json`
directly. **Do not create extra Airflow Variables for those files.** Editing
the Gold JSON file does not automatically update its deployed Variable.

### Required one-slot pool: `retail_writer`

```bash
docker compose exec -T airflow-scheduler airflow pools set retail_writer 1 "Serialize retail warehouse writers and corrections"
```

Keep **exactly one slot**. All main/standalone layer and correction tasks use
this pool; without it they cannot run. Per-DAG concurrency alone would not
serialize separate DAGs. The pool does not protect against external SQL writers
and does not turn ordinary multi-task runs into a single transaction.

## 7. Validate configuration without loading business data

```bash
docker compose exec -T airflow-scheduler airflow dags list-import-errors --output json
docker compose exec -T -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor python /opt/airflow/scripts/validate_retail_dags.py
docker compose exec -T airflow-scheduler airflow dags list
```

Expect no DAG import errors and PASS messages for the 14-task graph, calendar,
pool assignments, standalone parity, correction DAG and native timetable.
These checks do not validate credentials, execute ETL or query Snowflake.

The `test_snowflake_connection` diagnostic queries session context, but it
explicitly requests `SYSADMIN`; it is **not** a reliable runtime-role check for
a restricted service login unless adapted. `test_snowflake_stage` performs a
real PUT under `_connection_test/`. Neither is required by the main pipeline.
Do **not** use `etl_check_file_connection` as the fresh-install gate: it is an
old 17-file fixture diagnostic and deliberately fails on the current 33-file set.

## 8. Run the complete pipeline

### Input contract

The supplied fixture has **33 CSVs for November 9–18, 2026**: ten each for
item/sales/inventory and store snapshots on November 9, 11 and 15. Inputs live
directly under `data/` and match:

```text
item_master_YYYYMMDD.csv
store_master_YYYYMMDD.csv
pos_sales_YYYYMMDD.csv
inventory_snapshot_YYYYMMDD.csv
```

Keep exact lowercase CSV headers as provided. Every scheduled/backfill date
requires item, sales and inventory files; a store file is optional when an
applicable accepted earlier snapshot exists. Start the fixture at November 9
so its initial masters are loaded. Files under `data/corrections/` are not
ordinary inputs. `data/file_manifest.json` is fixture metadata, not the live
Snowflake registry and not a runtime control input.

### Option A — one manual bulk run (simplest first installation)

This is the full pipeline, not separate layer triggers. Keep standalone DAGs
paused. Queue the manual run, then unpause the full DAG:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger retail_full_etl --conf '{"file_limit": 0}'
docker compose exec -T airflow-scheduler airflow dags unpause retail_full_etl
```

Airflow does not execute a manual run while its DAG remains paused. Follow
the **manual run you just triggered** in the UI:

```text
run_stg → run_tmp → run_gold → reconcile_gold
```

`file_limit=0` selects all recognized files. The run loads the calendar and
daily inventory itself; a fresh warehouse does **not** need a separate Gold
bootstrap. The current 33-file bulk run has 46 runtime task instances.

**Scheduling warning:** `retail_full_etl` has a real midnight-UTC daily
schedule and `catchup=False`. Unpausing can also create a normal current
scheduled run. The fixture does not contain every date after November 18;
such a run can fail for missing input files. It is not the fixture's manual
run. Check run type/date explicitly. After the demonstration finishes, pause
the full DAG again unless you are supplying ongoing daily files:

```bash
docker compose exec -T airflow-scheduler airflow dags pause retail_full_etl
```

Pausing prevents new scheduling but does not cancel existing runs/tasks. Let
active work finish; inspect queued/retry/backfill work before maintenance.
Do not mark a broken business run successful merely to hide an error.

### Option B — native sequential daily backfill

Use this to demonstrate complete day-by-day ETL rather than one bulk load:

1. Open `retail_full_etl` → **Trigger → Backfill**.
2. Select business dates **November 9–18, 2026** (or Nov 9–13 for the original
   subset). Check the preview/data intervals, not just displayed run-ID dates.
3. Set backfill **Max active runs = 1**; leave **Run backwards off**.
4. Choose **Reprocess failed**. With this DAG's past dependencies, `None` is
   rejected by Airflow. Choose completed reprocessing only for intentional
   unchanged replay of successful dates.
5. Ensure the DAG is unpaused to execute, monitor the chronological runs, and
   account for the automatic-schedule warning above. Pause after a fixture-only demo.

Each run uses `data_interval_start`, not wall-clock time. A displayed native
run ID ending `2026-11-10T00:00:00+00:00` processes **November 9**. The next
date's discovery waits for the preceding full run's final reconciliation.
Do not pass `start_date`/`end_date` as custom Trigger JSON.

There are 17 runtime task instances on four-file days and 16 on three-file
days. Previously bulk-loaded history can be replayed without resetting data
when file hashes, raw coverage and Silver decisions are unchanged. New or
changed historical inputs require the explicit correction path below.

## 9. Verify the result in Snowflake

The last task runs all three queries; you can also execute these files
unchanged in Snowsight:

1. [reconcile_gold.sql](include/sql/reconcile/reconcile_gold.sql): every returned
   date must be **PASS** for row counts and sales amounts.
2. [check_gold_integrity.sql](include/sql/reconcile/check_gold_integrity.sql):
   every failure count must be **0**.
3. [check_reporting_integrity.sql](include/sql/reconcile/check_reporting_integrity.sql):
   every failure count must be **0**.
4. [verify_reporting_corrections.sql](evidence/verify_reporting_corrections.sql):
   inspect calendar coverage, daily balances, active checksums and correction audits.

```sql
SELECT SOURCE_FILE_NAME, ROW_COUNT, LOAD_STATUS
FROM RETAIL_DWH.CONTROL.FILE_MANIFEST ORDER BY SOURCE_FILE_DATE, SOURCE_FILE_NAME;

SELECT SOURCE_TYPE, REASON_CODE, COUNT(*) AS REJECTED_ROWS
FROM RETAIL_DWH.DQ.QUARANTINE_RECORDS GROUP BY SOURCE_TYPE, REASON_CODE;

SELECT COUNT(*) AS SALES_LINES, SUM(NET_SALES) AS NET_SALES
FROM RETAIL_DWH.TGT.FCT_SALES;

SELECT
  (SELECT COUNT(*) FROM RETAIL_DWH.TMP.INVENTORY_SNAPSHOT) AS ACCEPTED_OBSERVATIONS,
  (SELECT COUNT(*) FROM RETAIL_DWH.TGT.FCT_INVENTORY_DAILY) AS DAILY_POSITIONS;
```

Accounting identity: **raw sales = accepted + rejected; Gold sales = accepted**,
for both row counts and money. Unparseable raw sales amounts intentionally
fail monetary reconciliation even if quarantined. Versioned inventory counts
need not equal daily observation counts; the daily reporting fact should.

For the unchanged **Nov 9–13 subset only**, the original test oracle is 510 raw
rows, 505 accepted rows, five rejected sales rows, 145 sales lines and 1566.15
net sales; 150 accepted inventory observations become 98 interval versions.
For the full Nov 9–18 fixture, local tests expect 1371 raw, 1033 accepted, 338
rejected, 322 sales lines, 2533.20 net sales and 115 inventory versions.
These are fixture examples, **not production thresholds or reset targets**.
After an approved correction, the old fixture totals need not hold.

## 10. Upgrade an already populated installation

Do not recreate existing secrets or delete tables to match the fresh-install guide.

1. Pause the full DAG, standalone retail DAGs, backfills and Power BI refresh;
   wait for active/queued/retry work to drain and stop external writers.
2. Execute only missing compatible DDL. For the calendar/correction upgrade,
   run `include/sql/ddl/create_reporting_correction_tables.sql`.
3. Create/check the one-slot pool and replace the deployed Gold Variable with
   the current JSON template, preserving your intended calendar bounds.
4. **Unpause and trigger `retail_gold_pipeline` once**, leaving the ordinary
   full/layer DAGs paused. It has `schedule=None`, so unpausing does not create
   automatic daily runs. It populates daily inventory for **all** existing
   accepted observations. Pause this standalone DAG after it finishes.
5. Run all verification queries. Only then resume ordinary ETL/BI refresh.

Do not begin with a narrow daily replay on an upgraded warehouse: global
reporting integrity expects daily rows for all already accepted inventory history.

## 11. Approved historical full-file corrections

Ordinary ingestion intentionally rejects a changed checksum for an existing
filename. Replaying a corrected file is **not** a safe substitute for repair.

Supported correction: one **complete replacement** for an already loaded
source/date file. Omitted records remove those observations. Omission from a
master snapshot does not invent an inactive/tombstone event. Partial patches,
new historical dates, multi-file atomic replacements, and arbitrary rule
deployments are outside this contract.

1. Obtain business approval. Pause ordinary ETL/backfills and BI refresh, wait
   for work to finish, and stop external writers. Restrict this DAG with Airflow RBAC.
2. Query the active checksum:

   ```sql
   SELECT SOURCE_FILE_NAME, SOURCE_FILE_SHA256, SOURCE_FILE_ID, ROW_COUNT
   FROM RETAIL_DWH.CONTROL.FILE_MANIFEST
   WHERE SOURCE_FILE_NAME = 'pos_sales_20261109.csv';
   ```

3. Put the **complete corrected CSV** at
   `data/corrections/pos_sales_20261109.csv`. Keep the root input unchanged for now.
4. Unpause `retail_historical_correction` (it has no schedule) and trigger it with:

   ```json
   {
     "filename": "pos_sales_20261109.csv",
     "expected_old_sha256": "REPLACE_WITH_64_CHARACTER_ACTIVE_SHA256",
     "approved": true,
     "approved_by": "authorized data owner",
     "reason": "Approved correction of discounts and duplicate lines"
   }
   ```

   `approved_by` is an operator-supplied audit field, not independent identity
   verification. Enforce real approval through RBAC and your change process.
5. Verify `CONTROL.CORRECTION_RUNS.STATUS='SUCCEEDED'` and all reconciliation
   checks. Inspect `ERROR_DETAIL` on failure. A byte-identical successful
   resubmission records `NOOP` without rebuilding business data.
6. **Only after success**, replace `data/<filename>` with the exact approved
   bytes from `data/corrections/<filename>`. Otherwise normal replay will
   correctly reject the outdated local root file.
7. Pause the correction DAG, resume normal ETL when inputs are ready, and refresh BI.

The single correction task holds the writer pool slot for staging and the
whole repair. It preserves source CSV/JSONL on the stage, archives superseded
raw/manifest records and existing DQ decisions, replaces the active STG file,
then revalidates **all loaded active history**, masters first. It removes stale
Silver/Gold rows and version boundaries, rebuilds SCD/inventory intervals,
re-resolves fact keys, and validates before committing business changes and
SUCCEEDED together. Failure rolls business changes back and records FAILED.

STG and FILE_MANIFEST expose the **active version**. Superseded versions live
in `CONTROL.RAW_VERSION_ARCHIVE` and `MANIFEST_VERSION_ARCHIVE`; prior rejection
decisions are snapshotted in `DQ_DECISION_ARCHIVE`. This is not a log of every
ordinary validation attempt. Full-history rebuilding is conservative but
needs volume/cost/transaction-duration testing and archive-retention policies.

If a worker is killed, its audit can remain RUNNING or the commit may have
succeeded before Airflow noticed. Inspect the active manifest/audit and verify
the old Snowflake session has ended before retrying. Do not blindly rerun SQL
or edit audit statuses. Stage uploads and calendar extension happen before
the repair transaction and are not rolled back. A business reversal is another
approved replacement, backed by your backup/Time Travel policy.

## 12. Power BI model

Connect using a separate read-only Snowflake role. Use one-to-many,
single-direction relationships from dimensions to facts:

| Dimension | Fact field |
| --- | --- |
| `DIM_DATE.CALENDAR_DATE` | `FCT_SALES.SALE_DATE` |
| `DIM_DATE.DATE_KEY` | `FCT_INVENTORY_DAILY.SNAPSHOT_DATE_KEY` |
| `DIM_ITEM.ITEM_SK` | Both reporting facts' `ITEM_SK` |
| `DIM_LOCN.LOCN_SK` | Both reporting facts' `LOCN_SK` |
| `DIM_SUBCLASS.SUBCLASS_SK` | `DIM_ITEM.SUBCLASS_SK` |
| `DIM_DISTRICT.DISTRICT_SK` | `DIM_LOCN.DISTRICT_SK` |

Mark `DIM_DATE` using `CALENDAR_DATE` for classic time intelligence; disable
automatic date/time tables for this model. Sort month labels by MONTH_NUMBER
and year-month values chronologically. Use common calendar slicers and no
fact-to-fact relationship.

**Do not use inventory EFFECTIVE_FROM as its daily date relationship.** It
records change starts, not every balance date. Stock can be summed across
items/stores on one date, **not across dates**. Define closing and average
stock explicitly. Missing observations stay missing, not zero or silently
carried forward. Future calendar dates do not create future stock balances.
Fiscal periods, 4-4-5 calendars and location-specific holidays require business
definitions and are intentionally not guessed.

## 13. Tests, troubleshooting and operating boundaries

Credential-free checks:

```bash
PYTHONPATH=dags python3 -m unittest discover -s tests/unit -v
PYTHONPATH=dags uv run --with-requirements requirements-test.txt --no-project python -m unittest discover -s tests/integration -p test_reporting_corrections.py -v
```

The local integration test can take several minutes. DuckDB dialect
adaptations do not prove Snowflake VARIANT, identity, transaction or PUT/COPY
behavior. To run the broader suite, omit `-p`; existing SCD evidence tests are
currently blocked by the missing `evidence/verify_etl_stages.sql`. That absent
worksheet is **not** a required deployment file; use section 9's actual files.

Isolated Snowflake acceptance requires an **existing `RETAIL_ETL_TEST_*`
database** and a connector JSON file outside the repository:

```bash
RETAIL_TEST_ALLOW_WRITES=1 \
RETAIL_TEST_SNOWFLAKE_CONFIG=/secure/path/test-snowflake.json \
PYTHONPATH=dags uv run --with-requirements requirements-test.txt \
  --with snowflake-connector-python --no-project python -m unittest discover \
  -s tests/integration -p test_snowflake_reporting_corrections.py -v
```

The JSON supplies normal Snowflake connector settings such as account, user,
authentication, warehouse, role and **the disposable test database**. The test
creates/drops only uniquely named schemas there and rejects `RETAIL_DWH`.
It exercises actual calendar SQL, classification, corrections, history repair,
NOOP and rollback; it does not exercise Airflow pool scheduling or worker PUT
permissions. Test a controlled non-production DAG run too before business rollout.

| Symptom | First check |
| --- | --- |
| Task queued indefinitely | DAG unpaused? `retail_writer` exists with one slot? Worker healthy? Earlier dependency/backfill still active? |
| `FERNET_KEY` warning | `.env` key is named `FERNET_KEY`, not merely an unused similarly named variable; do not rotate a live key casually |
| Missing provider/import error | `.env` provider requirements, image version, startup/container logs |
| Snowflake login/privilege error | Account identifier, noninteractive auth, assigned role, warehouse/stage/table grants and network policy |
| File not found | `file_con` container path, mounts, canonical filename and the run's data-interval date |
| Expected five dimensions | Replace the deployed Gold Variable; editing only its JSON template is insufficient |
| Missing calendar matches | Extend continuous full-year calendar coverage, deploy the Variable and reload calendar before facts |
| Missing daily inventory after upgrade | Run the standalone all-history Gold bootstrap, not only a narrow daily replay |
| Same filename / different hash | Approved correction workflow; never delete the manifest to bypass it |
| Reconciliation failure | Inspect raw/TMP/DQ accounting and historical-key checks; earlier ordinary task commits remain |

Production boundaries: sales IDs are assumed globally unique; changing them
to store-scoped requires coordinated validation/load/reconciliation changes.
Structural CSV errors fail tasks rather than going to business quarantine.
Ordinary reconciliation is logged, not durably persisted. The local Compose
stack needs production security, backups, monitoring, deployment and retention
work. The reported Gold bootstrap succeeded in the user's environment;
correction-specific Snowflake acceptance remains a separate release gate.
