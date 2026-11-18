
import pendulum
from airflow.sdk import dag, task

@dag(
    dag_id="test_snowflake_connection",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
)
def test_snowflake_connection():

    @task
    def verify():
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(
            snowflake_conn_id="retail_snowflake",
            database="RETAIL_DWH",
            schema="STG",
            role="SYSADMIN",
        )

        result = hook.get_first("""
            SELECT
                CURRENT_DATABASE(),
                CURRENT_SCHEMA(),
                CURRENT_WAREHOUSE(),
                CURRENT_ROLE()
        """)

        print("SNOWFLAKE CONNECTION RESULT:", result)

    verify()

test_snowflake_connection()
