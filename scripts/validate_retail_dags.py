"""Parse/validate the retail graphs without executing ETL or querying Snowflake.

In the supplied stack:
docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
    python /opt/airflow/scripts/validate_retail_dags.py
"""

from full_etl_pipeline.retail_full_etl import retail_full_etl
from full_etl_pipeline.retail_gold_pipeline import retail_gold_pipeline
from full_etl_pipeline.retail_tmp_processing import retail_tmp_processing
from full_etl_pipeline.stg_ingestion import retail_stg_ingestion
from airflow.timetables.base import TimeRestriction
import pendulum


def main():
    combined = retail_full_etl()
    combined.validate()
    combined.check_cycle()
    assert len(combined.tasks) == 13, combined.task_ids
    assert set(combined.task_group.children) == {"run_stg", "run_tmp", "run_gold"}
    assert combined.params["file_limit"] == 0
    assert combined.max_active_runs == combined.max_active_tasks == 1
    assert combined.start_date.date().isoformat() == "2026-11-09"
    assert combined.timetable.periodic
    discover = combined.get_task("run_stg.discover_files")
    assert discover.depends_on_past and discover.wait_for_downstream
    assert discover.downstream_task_ids == {"run_stg.ingest_one", "run_gold.reconcile_gold"}
    assert combined.get_task("run_stg.ingest_one").__class__.__name__ == "DecoratedMappedOperator"
    assert combined.get_task("run_stg.ingest_one").downstream_task_ids == {"run_tmp.load_tmp_item_master"}
    assert combined.get_task("run_tmp.load_tmp_inventory_snapshot").downstream_task_ids == {"run_gold.load_dim_subclass"}
    for group, factory in [
        ("run_stg", retail_stg_ingestion),
        ("run_tmp", retail_tmp_processing),
        ("run_gold", retail_gold_pipeline),
    ]:
        standalone = factory()
        standalone.validate()
        standalone.check_cycle()
        actual = {task_id.removeprefix(group + ".") for task_id in combined.task_ids if task_id.startswith(group + ".")}
        assert set(standalone.task_ids) == actual
        for original in standalone.tasks:
            grouped = combined.get_task(f"{group}.{original.task_id}")
            assert original.retries == grouped.retries
            assert original.retry_delay == grouped.retry_delay
            assert original.trigger_rule == grouped.trigger_rule
            expected = {f"{group}.{task_id}" for task_id in original.downstream_task_ids}
            actual_downstream = {
                task_id for task_id in grouped.downstream_task_ids if task_id.startswith(group + ".")
            }
            assert expected == actual_downstream
    print("PASS: original 13-task pipeline, native daily timetable, whole-previous-day success gate, layer parity and concurrency")
    restriction = TimeRestriction(earliest=pendulum.datetime(2026, 11, 9, tz="UTC"),
                                  latest=pendulum.datetime(2026, 11, 13, tz="UTC"), catchup=True)
    previous = None
    dates = []
    while True:
        info = combined.timetable.next_dagrun_info(last_automated_data_interval=previous,
                                                  restriction=restriction)
        if info is None:
            break
        dates.append(info.data_interval.start.date().isoformat())
        previous = info.data_interval
    assert dates == [f"2026-11-{day:02d}" for day in range(9, 14)], dates
    print("PASS: native timetable generates Nov 9, 10, 11, 12, 13 as five daily intervals")


if __name__ == "__main__":
    main()
