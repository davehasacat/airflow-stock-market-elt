from __future__ import annotations
import os
import pendulum
from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

# --- dbt Configuration ---
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
DBT_EXECUTABLE_PATH = os.getenv("DBT_EXECUTABLE_PATH")

@dag(
    dag_id="dbt_test_runner",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 2 * * *", # Runs daily at 2 AM, after the main pipeline
    catchup=False,
    tags=["dbt", "monitoring"],
    doc_md="""
    ### dbt Test Runner DAG

    This DAG is responsible for running all dbt tests to monitor the
    health of the data in the warehouse. For now, it only runs the tests.
    Future work will include parsing and storing the results.
    """,
)
def dbt_test_runner_dag():
    """
    This DAG uses DbtTaskGroup to execute dbt tests.
    """
    # This task group will run all tests defined in your dbt project
    run_dbt_tests = DbtTaskGroup(
        group_id="run_dbt_tests",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=ProfileConfig(
            profile_name="stock_market_elt",
            target_name="dev",
            profiles_yml_filepath=os.path.join(DBT_PROJECT_DIR, "profiles.yml"),
        ),
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH),
        operator_args={
            "dbt_cmd": "test", # The 'test' command will run all data tests
        }
    )

dbt_test_runner_dag()
