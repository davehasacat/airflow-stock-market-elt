from __future__ import annotations
import os
import pendulum
from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

# Import email alert function
from dags.utils.utils import send_failure_email

# --- dbt Configuration ---
# Set the directory of the dbt project and the path to the dbt executable
# These are configured in the .env file
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
DBT_EXECUTABLE_PATH = os.getenv("DBT_EXECUTABLE_PATH")

@dag(
    dag_id="dbt_build",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    # This DAG is now scheduled to run at 3 AM on weekdays
    schedule="0 3 * * 1-5",
    catchup=False,
    tags=["dbt", "build"],
    default_args={
        "on_failure_callback": send_failure_email
    },
    doc_md="""
    ### dbt Transformation DAG for Stock Market Data

    This DAG runs daily on a fixed schedule. It runs the `dbt build`
    command to execute all dbt models and tests, transforming the raw data
    into analytics-ready marts.
    """,
)
def dbt_build_dag():
    """
    This DAG uses DbtTaskGroup to execute dbt models on a schedule.
    """
    # Define a DbtTaskGroup to run the dbt build command.
    # Cosmos will parse the dbt project and create Airflow tasks for each dbt model and test.
    dbt_build = DbtTaskGroup(
        group_id="dbt_build_all_models",
        # Configure the path to the dbt project
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        # Configure the dbt profile to connect to the data warehouse
        profile_config=ProfileConfig(
            profile_name="stock_market_elt",
            target_name="dev",
            profiles_yml_filepath=os.path.join(DBT_PROJECT_DIR, "profiles.yml"),
        ),
        # Configure the execution environment, including the dbt executable path
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH),
        # Define arguments for the dbt command
        operator_args={
            "dbt_cmd": "build", # The 'build' command will run all models, tests, seeds, and snapshots
        }
    )

# Instantiate the DAG
dbt_build_dag()
