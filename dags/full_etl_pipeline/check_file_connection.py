
from pathlib import Path
import csv
import pendulum

from airflow.sdk import dag, task
from airflow.providers.standard.hooks.filesystem import FSHook
from airflow.providers.standard.sensors.filesystem import FileSensor


@dag(
    dag_id="etl_check_file_connection",
    start_date=pendulum.datetime(2026, 11, 17, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["etl", "connection-test"],
)
def check_file_connection():

    # Wait for one known CSV file.
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        fs_conn_id="file_con",
        filepath="item_master_20261109.csv",
        poke_interval=10,
        timeout=60,
        mode="reschedule",
    )

    @task
    def verify_csv_files():

        # Resolve directory using the Airflow connection.
        hook = FSHook(fs_conn_id="file_con")
        data_dir = Path(hook.get_path())

        print(f"Data directory: {data_dir}")

        if not data_dir.is_dir():
            raise FileNotFoundError(data_dir)

        # Discover all CSV files.
        csv_files = sorted(data_dir.glob("*.csv"))

        print(f"CSV files found: {len(csv_files)}")

        if len(csv_files) != 17:
            raise ValueError("Expected exactly 17 CSV files")

        total_rows = 0

        for file in csv_files:
            with file.open(
                "r", encoding="utf-8-sig", newline=""
            ) as f:
            
                reader = csv.DictReader(f)

                if not reader.fieldnames:
                    raise ValueError(f"Missing header: {file.name}")

                rows = sum(1 for _ in reader)

            print(f"{file.name}: {rows} rows")

            if rows != 30:
                raise ValueError(
                    f"{file.name}: expected 30 rows, got {rows}"
                )

            total_rows += rows

        print(f"TOTAL CSV FILES: {len(csv_files)}")
        print(f"TOTAL DATA ROWS: {total_rows}")

        if total_rows != 510:
            raise ValueError("Unexpected total row count")

    wait_for_file >> verify_csv_files()


check_file_connection()
