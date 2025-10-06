from __future__ import annotations

import os
import pendulum

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

# --- DAG Configuration ---
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
DBT_EXECUTABLE_PATH = os.getenv("DBT_EXECUTABLE_PATH")
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

    This DAG uses the officially recommended `astronomer-cosmos` package to test the full stack.
    """,
)
def full_stack_connection_test_dag():
    """
    A DAG to test connections to S3, Postgres, and dbt using Cosmos.
    """

    @task
    def test_minio_connection():
        """Checks the Minio S3 connection."""
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        s3_hook.list_keys(bucket_name=BUCKET_NAME)
        print(f"Minio connection to bucket '{BUCKET_NAME}' successful.")

    @task
    def test_postgres_connection():
        """Checks the Postgres DWH connection."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg_hook.get_first("SELECT 1;")
        print("Postgres connection successful.")

    # This TaskGroup finds the dbt project, parse it, and creates an Airflow task for the specified model.
    test_dbt_connection = DbtTaskGroup(
        group_id="test_dbt_connection",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=ProfileConfig(
            profile_name="stock_market_elt",
            target_name="dev",
            profiles_yml_filepath=os.path.join(DBT_PROJECT_DIR, "profiles.yml"),
        ),
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH),
        operator_args={"select": "source_polygon_stock_bars_daily"},
    )

    # Define task dependencies
    [test_minio_connection(), test_postgres_connection()] >> test_dbt_connection

full_stack_connection_test_dag()
