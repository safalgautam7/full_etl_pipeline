"""Run the existing retail tasks in expandable STG, TMP, and Gold groups.

The standalone layer DAGs and their ETL logic remain unchanged. Their original
task-building functions are reused here, so the full graph shows real tasks
rather than operators that trigger separate DAG runs.

All tasks run under this DAG's run ID (including STG's BATCH_ID). The default
file_limit=0 processes all recognized files unless an inclusive date range is supplied.
Do not run standalone layer DAGs concurrently with this DAG: their per-DAG
concurrency limits do not protect shared Snowflake tables across different DAGs.
"""

from inspect import unwrap

import pendulum
from airflow.sdk import TaskGroup, dag

from full_etl_pipeline.retail_gold_pipeline import retail_gold_pipeline
from full_etl_pipeline.retail_tmp_processing import retail_tmp_processing
from full_etl_pipeline.stg_ingestion import retail_stg_ingestion


@dag(
    dag_id="retail_full_etl",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"retries": 0},
    params={"file_limit": 0, "start_date": None, "end_date": None},
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


retail_full_etl()
