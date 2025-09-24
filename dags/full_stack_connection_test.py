from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig

with DAG(
    dag_id="full_stack_connection_test",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["test", "minio", "postgres", "dbt"],
) as dag:
    # check minio connection by listing keys in the 'test' bucket
    test_minio_connection = S3ListOperator(
        task_id="test_minio_connection",
        aws_conn_id="minio_s3",
        bucket="test",
    )

    # check postgres_dwh connection by running a simple query
    test_postgres_connection = SQLExecuteQueryOperator(
        task_id="test_postgres_connection",
        conn_id="postgres_dwh",
        sql="SELECT 1;",
    )
    
    # execution_config = ExecutionConfig(dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt")

    # check dbt connection by running dbt seed
    # test_dbt_connection = DbtTaskGroup(
    #     group_id="test_dbt_connection",
    #     project_config=ProjectConfig(
    #         dbt_project_path="/usr/local/airflow/dbt",
    #     ),
    #     profile_config=ProfileConfig(
    #         profile_name="stock_data_backtesting",
    #         target_name="dev",
    #         profiles_yml_filepath="/usr/local/airflow/dbt/profiles.yml",
    #     ),
    #     execution_config=execution_config,
    #     render_config=RenderConfig(
    #         select=["connection_test"]
    #     )
    # )

    # define the task dependencies
    test_minio_connection >> test_postgres_connection
