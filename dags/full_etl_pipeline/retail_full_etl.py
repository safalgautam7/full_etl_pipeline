"""Existing full pipeline with native Airflow daily scheduling/backfill.

Use the UI Backfill dialog, oldest first, max active runs 1. Each run's
data interval supplies its business date; no custom controller/API is needed.
Manual runs retain the original bulk behavior.
"""

from inspect import unwrap

import pendulum
from airflow.sdk import TaskGroup, dag
from airflow.timetables.interval import CronDataIntervalTimetable

from full_etl_pipeline.retail_gold_pipeline import retail_gold_pipeline
from full_etl_pipeline.retail_tmp_processing import retail_tmp_processing
from full_etl_pipeline.stg_ingestion import retail_stg_ingestion


@dag(
    dag_id="retail_full_etl",
    start_date=pendulum.datetime(2026, 11, 9, tz="UTC"),
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"retries": 0},
    params={"file_limit": 0},
    tags=["retail", "orchestration", "stg-tmp-tgt"],
)
def retail_full_etl():
    # @dag preserves the original function via __wrapped__. Unwrap only the DAG
    # factory so its existing @task definitions attach to this DAG/TaskGroup;
    # calling the decorated factory here would create another standalone DAG.
    with TaskGroup(group_id="run_stg", tooltip="Bronze: discover and ingest files") as run_stg:
        unwrap(retail_stg_ingestion)()

    with TaskGroup(group_id="run_tmp", tooltip="Silver: validate, load TMP, and quarantine") as run_tmp:
        unwrap(retail_tmp_processing)()

    with TaskGroup(group_id="run_gold", tooltip="Gold: dimensions, facts, and reconciliation") as run_gold:
        unwrap(retail_gold_pipeline)()

    run_stg >> run_tmp >> run_gold
    # Airflow's built-in past-dependency gate waits for the preceding run's
    # ingestion AND final reconciliation. The extra edge is transitively true
    # already; making it direct lets wait_for_downstream gate the whole day.
    discover = run_stg.children["run_stg.discover_files"]
    reconcile = run_gold.children["run_gold.reconcile_gold"]
    discover.depends_on_past = True
    discover.wait_for_downstream = True
    discover >> reconcile


retail_full_etl()
