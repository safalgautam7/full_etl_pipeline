"""Parse/validate the retail graphs without executing ETL or querying Snowflake.

In the supplied stack:
docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
    python /opt/airflow/scripts/validate_retail_dags.py
"""

from full_etl_pipeline.retail_full_etl import retail_full_etl
from full_etl_pipeline.retail_gold_pipeline import retail_gold_pipeline
from full_etl_pipeline.retail_tmp_processing import retail_tmp_processing
from full_etl_pipeline.stg_ingestion import retail_stg_ingestion


def main():
    combined = retail_full_etl()
    combined.validate()
    combined.check_cycle()
    assert len(combined.tasks) == 13, combined.task_ids
    assert set(combined.task_group.children) == {"run_stg", "run_tmp", "run_gold"}
    assert combined.params["file_limit"] == 0
    assert combined.params["start_date"] is None
    assert combined.params["end_date"] is None
    assert combined.max_active_runs == combined.max_active_tasks == 1
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
        if group == "run_stg":
            assert standalone.params["start_date"] is None
            assert standalone.params["end_date"] is None
        standalone.check_cycle()
        actual = {task_id.removeprefix(group + ".") for task_id in combined.task_ids if task_id.startswith(group + ".")}
        assert set(standalone.task_ids) == actual
        for original in standalone.tasks:
            grouped = combined.get_task(f"{group}.{original.task_id}")
            assert original.retries == grouped.retries
            assert original.retry_delay == grouped.retry_delay
            assert original.trigger_rule == grouped.trigger_rule
            assert {f"{group}.{task_id}" for task_id in original.downstream_task_ids} == {
                task_id for task_id in grouped.downstream_task_ids if task_id.startswith(group + ".")
            }
    print("PASS: 13 task definitions, mapping, layer boundaries, standalone parity, retries, concurrency, and no cycles")


if __name__ == "__main__":
    main()
