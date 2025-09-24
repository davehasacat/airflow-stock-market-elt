from __future__ import annotations

import os
import pendulum
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_dbt_python.operators.dbt import DbtRunOperator

# --- DAG Configuration ---
DBT_PROJECT_DIR = "/usr/local/airflow/dbt"
DBT_PROFILE_NAME = "stock_market_elt"
DBT_TARGET_NAME = "dev"
S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_dwh")
BUCKET_NAME = os.getenv("BUCKET_NAME", "test")

@dag(
    dag_id="full_stack_connection_test",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["test", "minio", "postgres", "dbt"],
    doc_md="""
    ### Full Stack Connection Test DAG

    This DAG tests the connections to all major components of the ELT stack:
    1.  **Minio (S3)**: Lists keys in the test bucket.
    2.  **Postgres DWH**: Executes a simple query.
    3.  **dbt**: Runs a single staging model to verify dbt can connect and execute.
    """,
)
def full_stack_connection_test_dag():
    """
    A DAG to test connections to S3 (Minio), Postgres, and dbt.
    """

    @task
    def test_minio_connection():
        """Checks the Minio S3 connection by listing keys in the test bucket."""
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        keys = s3_hook.list_keys(bucket_name=BUCKET_NAME)
        print(f"Found {len(keys)} keys in bucket '{BUCKET_NAME}'. Connection successful.")
        return True

    @task
    def test_postgres_connection():
        """Checks the Postgres DWH connection by running a simple query."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        result = pg_hook.get_first("SELECT 1;")
        if result and result[0] == 1:
            print("Postgres connection successful.")
        else:
            raise ValueError("Postgres connection test failed.")
        return True

    # Use the DbtRunOperator to test a single model run
    # This is more direct than using a TaskGroup for a single test
    test_dbt_connection = DbtRunOperator(
        task_id="test_dbt_connection",
        project_dir=DBT_PROJECT_DIR,
        profile_name=DBT_PROFILE_NAME,
        target_name=DBT_TARGET_NAME,
        select=["stg_polygon__stock_bars_daily"], # Test a single, fast-running model
    )

    # --- Define Task Dependencies using TaskFlow API ---
    # The tasks are called in order, and Airflow infers the dependency
    postgres_test_result = test_postgres_connection()
    minio_test_result = test_minio_connection()

    # Set the dbt test to run after the other two have succeeded
    test_dbt_connection.set_upstream([postgres_test_result, minio_test_result])


full_stack_connection_test_dag()
