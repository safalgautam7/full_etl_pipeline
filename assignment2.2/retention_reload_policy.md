# Data Warehouse Retention and Reload Policy

**Assignment:** 2.2  
**Scope:** Northline Retail data warehouse design  
**References:** `layer_inventory.csv`, `architecture.mmd`, and `architecture_notes.md`  
**Status:** Design policy. These rules describe how the current inventory should be retained and reprocessed; they do not claim that retention jobs or recovery workflows have already been implemented.

## 1. Purpose

This document defines how source data and warehouse records will be retained and how an incorrect file, failed batch, or bad business date will be recovered. The main goals are to:

- Keep the original source files available for recovery.
- Retain Bronze and Silver data for 30 days.
- Preserve historical business data in Gold.
- Reprocess only the file and downstream data affected by a correction whenever possible.
- Prevent duplicate business records when the same data is processed again.
- Keep enough batch and rejection information to explain what was processed, rejected, corrected, and published.

The implementation inventory uses **STG** for Bronze, **TMP** for Silver, and dimension/fact targets for **Gold**. `REJ_TABLE` belongs to Silver and `Batch Log` belongs to Gold. `FCT_SALES_PER_WEEK` is a Gold-level aggregate fact, not a separate warehouse layer.

## 2. Retention Policy Overview

| Layer / component | Retention policy | How it is used |
|---|---|---|
| Source files | Archived; no fixed expiry specified in this design | Original daily CSV extracts remain available for recovery and reprocessing. Corrected versions are archived separately. |
| Bronze (`STG_*`) | 30 days | Raw ingested records are kept by ingestion attempt without overwriting the original batch. |
| Silver (`TMP_*`, `REJ_TABLE`) | 30 days | Validated records and rejected-row information are kept long enough to investigate or rebuild affected loads. |
| Gold (dimensions and facts) | Historical maintenance; no fixed expiry specified | Published business data and applicable dimension history are preserved while corrections update the affected business records. |
| `Batch Log` (Gold) | Maintained as processing history; no fixed expiry specified | Tracks ingestion, validation, publication, failures, and reload attempts. |

**Retention clock:** For the 30-day staging windows, use ingestion/completion timestamps rather than deleting an older business date immediately after a late backfill. The exact physical partition columns and cleanup schedule still need to be confirmed. `layer_inventory.csv` records partitioning as `column` but does not name the columns.

## 3. Layer-Specific Retention Policies

### 3.1 Source files

The current inventory contains five full daily CSV extract types:

| Source file pattern | Bronze table | Silver table |
|---|---|---|
| `item_master_yyyymmdd.csv` | `STG_itemmaster` | `TMP_itemmaster` |
| `pos_sales_yyyymmdd.csv` | `STG_possales` | `TMP_possales` |
| `store_master_yyyymmdd.csv` | `STG_storemaster` | `TMP_storemaster` |
| `inventory_master_yyyymmdd.csv` | `STG_inventorymaster` | `TMP_inventorymaster` |
| `price_master_yyyymmdd.csv` | `STG_pricemaster` | `TMP_pricemaster` |

Each of these source files is marked **full** in `layer_inventory.csv`. A full source extract is not the same thing as a delta load in the warehouse.

- Archive the original source files without changing their contents.
- Identify each archive entry by source file name, received timestamp, version or checksum, and associated batch where available.
- Do not overwrite an original source file with a correction. Archive the correction as a separate version.
- Treat the file name date as a source-file identifier until its meaning is confirmed. The actual business date or affected date range must be derived from the source content and load rules.
- The archive is the recovery source when the corresponding Bronze and Silver records have aged out.

### 3.2 Bronze: `STG_*`

**Objects:** `STG_itemmaster`, `STG_possales`, `STG_storemaster`, `STG_inventorymaster`, `STG_pricemaster`.

- Retain Bronze records for **30 days**.
- Follow the inventory's **delta by ingestion date** load pattern: each accepted ingestion adds a distinguishable batch of source records instead of replacing the previous batch.
- Store enough lineage to trace each record to its source file, ingest attempt, and source row. These are required design attributes, not a claim that the columns already exist.
- Preserve original source values in Bronze. Data cleaning and business-rule transformations belong in Silver.
- When a source file is corrected, ingest the corrected version under a **new `BATCH_ID`**. Keep the original Bronze batch until its retention period expires.
- Detect exact replays of an unchanged file so that an accidental retry does not double the effective input to Silver.

**What Bronze guarantees:** A traceable copy of what was ingested within the retention window.  
**What Bronze does not guarantee:** Valid business keys, clean values, deduplication, or readiness for Gold.

### 3.3 Silver: `TMP_*` and `REJ_TABLE`

**Objects:** `TMP_itemmaster`, `TMP_possales`, `TMP_storemaster`, `TMP_inventorymaster`, `TMP_pricemaster`, `REJ_TABLE`.

- Retain Silver data for **30 days**.
- Build the `TMP_*` tables from their corresponding Bronze records using validation, cleaning, and deduplication checks.
- Use `REJ_TABLE` to keep failed records or their identifiers, source, batch, and rejection reasons, as supported by the actual rejection-table design.
- Keep rejected records outside the accepted `TMP_*` output until they are corrected or explicitly approved by a documented rule.
- When a Bronze batch is corrected, replace or supersede **only the affected approved Silver scope**. Do not append a second accepted copy of the same business record.
- If a full daily master or inventory extract changes, evaluate all dependent records or effective dates that could change. The downstream scope may be larger than the file-name date.

**What Silver guarantees:** Accepted records passed the specified validation and deduplication rules for their processing batch.  
**What Silver does not guarantee:** Final Gold surrogate-key relationships or that a source error undetectable by the rules has been resolved.

### 3.4 Gold: historical dimensions, facts, and the aggregate

The objects recorded in the implementation inventory are:

| Gold object group | Objects | Inventory load pattern |
|---|---|---|
| Calendar dimensions | `DIM_DAY`, `DIM_WEEK`, `DIM_MONTH`, `DIM_QUARTER`, `DIM_YEAR` | Seed |
| Business dimensions | `DIM_ITEM`, `DIM_CLASS`, `DIM_DEPARTMENT`, `DIM_COMPANY`, `DIM_LOCATION`, `DIM_DISTRICT`, `DIM_STATE`, `DIM_CUSTOMER` | Delta |
| Detailed facts | `FCT_SALES`, `FCT_PRICE`, `FCT_INVENTORY` | Delta |
| Aggregate fact | `FCT_SALES_PER_WEEK` | Delta |
| Processing metadata | `Batch Log` | Delta |

- Maintain Gold business history rather than applying the Bronze or Silver 30-day expiration rule.
- Update or replace records using the defined business grain and approved source version. A corrected file must not create a duplicate fact simply because it has a new batch ID.
- Preserve dimension history **where the actual dimension design requires it**. Follow the configured SCD rules rather than assuming that every dimension is SCD2.
- Seeded calendar dimensions are refreshed only when their calendar definition or approved seed data changes. They should not be recreated for every daily source-file reload.
- Rebuild affected `FCT_SALES_PER_WEEK` groups after a correction to `FCT_SALES`. A sales correction for one date can affect the total for its entire calendar week.
- Perform foreign-key, uniqueness, and aggregate reconciliation checks before considering a changed Gold scope published.

**What Gold guarantees:** Published records satisfy the target-level rules and can be read consistently by consumers.  
**What Gold does not guarantee:** That an uncorrected error in the original source was automatically detected or that all source data can be reconstructed without an archived file.

### 3.5 Batch Log

`Batch Log` is present in the inventory as a Gold table with a delta load pattern. The inventory does **not** provide its column definitions, so the following are **required or recommended fields for the policy**, not verified existing columns.

| Field | Purpose |
|---|---|
| `BATCH_ID` | Unique ID for an ingestion or processing attempt. |
| `PARENT_BATCH_ID` | Links a retry or correction to the original attempt, when applicable. |
| `BUSINESS_DT` | Business date of the affected records, when a single date applies. |
| `SOURCE_FILE_NAME` | File whose records were processed. |
| `SOURCE_FILE_VERSION` or checksum | Distinguishes original and corrected file contents. |
| `OBJECT_NAME` | `STG_*`, `TMP_*`, `REJ_TABLE`, DIM, or FCT object processed. |
| `BATCH_STATUS` | For example, `STARTED`, `FAILED`, `REPROCESSING`, `VALIDATED`, `PUBLISHED`. |
| `START_TS`, `END_TS` | Start and completion timestamps. |
| `INPUT_ROW_COUNT`, `ACCEPTED_ROW_COUNT`, `REJECTED_ROW_COUNT`, `DUPLICATE_ROW_COUNT` | Records row movement and explains filtering. |
| `ERROR_REASON` | Reason for a failure or rejected publication, when relevant. |

An approved-batch or publication-status mechanism must identify which successful version is active downstream. A successful ingest does not automatically mean that its records have been approved for Gold. The Batch Log should record failures even when a target data transaction rolls back.

## 4. Reload and Recovery Policy

**Recovery unit:** Start with the affected **source file and its dependent objects**. Narrow the scope to a business date, source key, or period only when the file content and target load rules make that safe.

### 4.1 Recovery within the 30-day Bronze/Silver window

1. Identify the affected source file, batch ID, business date or date range, and dependent target objects using the Batch Log and `REJ_TABLE` where applicable.
2. Check whether the original file is wrong or whether the failure happened during processing. For a processing failure with valid Bronze data, reuse the existing raw batch. For corrected source content, archive the new file version and ingest it as a new Bronze batch.
3. Run the relevant Bronze-to-Silver checks again. Rebuild or supersede the affected accepted rows in the corresponding `TMP_*` table and record rejected rows in `REJ_TABLE`.
4. Confirm that the corrected Silver batch is the approved version for downstream use. Incorrect and superseded batches remain available during their retention window but are not republished.
5. Reprocess only the dependent Gold dimensions and facts. Expand the affected date range for dimension-history changes, full snapshots, or overlapping effective-date records when needed.
6. Recompute the impacted `FCT_SALES_PER_WEEK` group if `FCT_SALES` changed. Validate, publish, and record the final status in the Batch Log.

### 4.2 Recovery after 30 days

If the required Bronze or Silver records have already expired:

1. Retrieve the relevant original file from the source archive.
2. If the source file itself was incorrect, obtain and archive a corrected version separately.
3. Ingest the selected source version into the corresponding `STG_*` table with a new `BATCH_ID` and a link to the earlier attempt if known.
4. Rebuild the corresponding `TMP_*` scope, rerun validation and deduplication, and capture rejects in `REJ_TABLE`.
5. Reprocess affected Gold objects using the newly approved Silver output, with idempotent replacement or merge logic.
6. Verify that the correction did not duplicate Gold business records, then mark the new publication in the Batch Log.

The 30-day expiry does **not** mean Gold history is deleted. It means the replay may need to start from the archived source file instead of staging tables.

### 4.3 Gold-only failure

If Bronze and Silver succeeded but a Gold load failed:

- Reuse the existing validated `TMP_*` rows **if those rows are still retained and identified as the approved input**.
- Retry only the failed or dependent Gold objects; do not re-ingest the source just to repeat successful upstream work.
- Recalculate `FCT_SALES_PER_WEEK` if the failed or corrected operation affects `FCT_SALES`.
- If the required Silver input has already expired, restore it from archived source files through Bronze and Silver before retrying Gold.
- Keep the last valid Gold publication available until the replacement passes validation, where the publishing mechanism supports this.

### 4.4 Example: correcting a sales file

Suppose a problem is found in `pos_sales_yyyymmdd.csv` for a particular extract date.

`Archived source version -> STG_possales -> TMP_possales / REJ_TABLE -> FCT_SALES -> FCT_SALES_PER_WEEK`

1. Find the original file and `BATCH_ID`. Save the corrected file as a different archived version, if a correction is supplied.
2. Ingest the corrected version into `STG_possales` under a new `BATCH_ID`, without overwriting the original Bronze rows.
3. Re-run validation and deduplication into `TMP_possales`; put rejected records in `REJ_TABLE`.
4. Identify the sales transaction lines or business dates actually changed. Replace or merge the approved `FCT_SALES` records for those business keys.
5. Rebuild the relevant weekly totals in `FCT_SALES_PER_WEEK` using the calendar mapping in Gold.
6. Check sales totals, business-key uniqueness, rejected-row accounting, and the affected weekly totals before publishing.

This example is a **proposed recovery flow**, not evidence that those steps are already automated.

### 4.5 Other dependencies that can enlarge a reload

| Changed source | First staging objects | Gold records to evaluate |
|---|---|---|
| `item_master_yyyymmdd.csv` | `STG_itemmaster` → `TMP_itemmaster` | `DIM_ITEM`, `DIM_CLASS`, `DIM_DEPARTMENT`, and dependent fact references if the changed item mapping affects them. |
| `store_master_yyyymmdd.csv` | `STG_storemaster` → `TMP_storemaster` | `DIM_LOCATION`, `DIM_DISTRICT`, `DIM_STATE`, `DIM_COMPANY`, and dependent fact references if the location mapping changes. |
| `pos_sales_yyyymmdd.csv` | `STG_possales` → `TMP_possales` | `FCT_SALES`, its dependent `FCT_SALES_PER_WEEK` groups, and customer mappings if present in the approved sales logic. |
| `inventory_master_yyyymmdd.csv` | `STG_inventorymaster` → `TMP_inventorymaster` | `FCT_INVENTORY` for the affected snapshot or effective period. |
| `price_master_yyyymmdd.csv` | `STG_pricemaster` → `TMP_pricemaster` | `FCT_PRICE` for the affected item/location/date or valid period. |

The table describes **logical dependencies suggested by the object names and warehouse model**, not verified task-level lineage. The exact scope depends on the transformations and business keys implemented in the warehouse.

## 5. Batch and Reprocessing Rules

1. Give each new processing attempt a unique `BATCH_ID`.
2. Keep `BATCH_ID` (which processing attempt) separate from `BUSINESS_DT` (which business date the records represent). One file can affect multiple business dates.
3. Link corrected or retried attempts to the original batch through `PARENT_BATCH_ID` when supported by the Batch Log.
4. Never overwrite an original archived file or original Bronze batch during a correction.
5. Select only the approved version of source records for Silver-to-Gold publication.
6. Make recovery **idempotent**: running the same approved input twice must not change the published number of business records or totals.
7. Start with file-level scope, then expand only to necessary related items, locations, dates, and dependent tables.
8. Treat a corrected full source extract as a new version of its represented data, not automatically as new business activity to append to Gold.
9. Do not delete 30-day staging data needed by an active reload. Finish or safely resume the reload before applying retention cleanup to those batches.
10. Record failed and successful processing attempts, rejection counts, checksums or versions, and publication outcomes.

## 6. Quality Checks and Failure Actions

The checks below apply to **reprocessing as well as normal loads**. A failed gate prevents affected unapproved data from moving to the next layer.

| Rule | Boundary | Check | Failure action |
|---|---|---|---|
| **R1: File identity** | Source → Bronze | Required file can be identified; detect a repeated unchanged file and distinguish corrected versions. | **FAIL** a conflicting or unreadable mandatory file; skip safe identical retries. |
| **R2: Ingestion accounting** | Source → Bronze | Source rows and loaded raw rows can be accounted for; malformed records are identified. | **FAIL** the affected load if accounting is unreliable; preserve the source for investigation. |
| **R3: Required values** | Bronze → Silver | Required keys, dates, and numeric values follow the source-specific validation rules. | **QUARANTINE** invalid rows in `REJ_TABLE`; **FAIL** if the mandatory dataset is unusable. |
| **R4: Duplicates** | Bronze → Silver | Duplicate source/business records are detected and handled by a defined survivor rule. | **QUARANTINE** conflicting duplicates; **ALERT** on an unexpected increase. |
| **R5: Referential integrity** | Silver → Gold | Fact records resolve to required dimension keys and do not break declared relationships. | **FAIL** the affected Gold publication or **QUARANTINE** unresolved rows according to the approved rule. |
| **R6: Gold business grain** | Silver → Gold | Reprocessing does not produce duplicate business-grain rows in the affected dimension or fact. | **FAIL** publication. |
| **R7: Amount reconciliation** | Silver → Gold | Eligible validated sales totals reconcile with `FCT_SALES` for the same source and date/filter scope. | **FAIL** publication for unexplained differences. |
| **R8: Weekly aggregate reconciliation** | Gold detail → Gold aggregate | `FCT_SALES_PER_WEEK` totals match the eligible `FCT_SALES` rows mapped to that calendar week. | **FAIL** aggregate publication; **ALERT** the owner. |

**Meaning of actions:** **FAIL** blocks the affected load/publication; **QUARANTINE** records rejected data in `REJ_TABLE` instead of accepted output; **ALERT** records a discrepancy requiring review.

### Validation after a reload

| Validation | Expected result |
|---|---|
| Bronze-to-Silver row accounting | Every eligible Bronze input row is accounted for as accepted, rejected, or removed by a documented duplicate/filter rule. `REJ_TABLE` identifies rejected rows. |
| Silver-to-Gold sales reconciliation | Approved `TMP_possales` sales totals reconcile with `FCT_SALES` using the same accepted-record and business-rule scope. |
| Dimension current-record check | For dimensions that use a current flag or SCD2 tracking, at most one current version exists per business key; keys expected to be active have exactly one. |
| Surrogate-key uniqueness | Keys used by Gold dimensions and facts remain unique under their defined constraints. |
| Foreign-key validation | All required fact-to-dimension references are valid. |
| Duplicate validation | Re-running the approved batch does not create duplicate Gold business-grain records. |
| Weekly aggregate reconciliation | `FCT_SALES_PER_WEEK` matches the corresponding `FCT_SALES` business weeks after a sales correction. |
| Batch status check | The Batch Log identifies the approved published batch and retains the previous attempt's outcome. |

Only successfully validated data should proceed to the next layer or become the new published Gold version.

## 7. Assumptions and Design Limits

- This policy reflects the **34 objects in the supplied `layer_inventory.csv`**: 5 source files, 5 Bronze tables, 6 Silver tables including `REJ_TABLE`, and 18 Gold objects including `Batch Log`.
- `layer_inventory.csv` identifies partitioning only as `column`. The actual partition columns, grain keys, and physical deletion commands must be confirmed before automating the 30-day purge or reload.
- The inventory supplies load-pattern descriptions but does not provide transformation SQL, Batch Log DDL, rejection-table DDL, or an execution history. Proposed fields, business-key logic, quality checks, and atomic-publish behavior are **design requirements**, not claims of completed implementation.
- The five daily source files are marked `full`; Bronze and Silver are described as delta loads by ingestion date or validation outcome. The recovery flow respects this distinction.
- Gold retention is historical, but no fixed duration is specified. No unapproved seven-year or indefinite-retention number is introduced.
- The calendar is seeded in Gold and `FCT_SALES_PER_WEEK` is already part of Gold. A bad sales day must therefore trigger a check of its corresponding weekly aggregate.
- Earlier architecture notes were drafted around a different source catalog and a different Gold ERD. Where those notes conflict with the supplied implementation inventory, **the inventory's object names and load patterns take precedence for this policy**. Their general fail/quarantine/alert principles remain applicable.
