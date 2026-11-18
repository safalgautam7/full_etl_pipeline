from contextlib import closing
from ctypes.util import test
from pathlib import Path
from sched import scheduler
from threading import local

import pendulum
from airflow.sdk import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.hooks.filesystem import FSHook

@dag(
    dag_id = "test_snowflake_stage",
    start_date = pendulum.datetime(2026,11,17,tz = "UTC"),
    schedule = None,
    catchup = False,
    tags = ["etl", "snowflake", "test"],
)
def test_snowflake_stage():

    @task
    def upload_and_verify():
        fs_hook = FSHook(fs_conn_id="file_con")
        data_dir = Path(fs_hook.get_path())

        file_name = "item_master_20261110.csv"
        local_file = data_dir / file_name

        if not local_file.is_file():
            raise FileNotFoundError(local_file)

        print(f"local file found: {local_file}")
        print(f"file size: {local_file.stat().st_size} bytes") #.stat() -> metadata and .st_size -> size in bytes

        sf_hook = SnowflakeHook(
            snowflake_conn_id = "retail_snowflake"
        )

        stage_path = (
            "@RETAIL_DWH.STG.CSV_LANDING/_connection_test/"
        )


        #convert local path to file:/// URI for snowflake PUT command

        file_uri = local_file.resolve().as_uri()

        with closing(sf_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                
                #upload the csv to an isolated test prefix
                put_sql = (
                    f"PUT '{file_uri}' '{stage_path}' "
                    "AUTO_COMPRESS = FALSE OVERWRITE = TRUE"
                )

                cursor.execute(put_sql)
                put_results = cursor.fetchall()

                print("upload results:", put_results)

                #check if snowflake lists the uplaoded file in the stage
                cursor.execute(f"LIST {stage_path}")
                staged_files = cursor.fetchall()

                for row in staged_files:
                    print("staged_file", row[0])

                found = any(
                    str(row[0]).split("/")[-1] == file_name for row in staged_files
                )

                if not found:
                    raise ValueError(
                        "uploaded csv was not found in snowflake stage"
                    )
                print("stage upload test passed")

    upload_and_verify()
test_snowflake_stage()