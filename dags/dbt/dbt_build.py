from __future__ import annotations
import os
import pendulum
from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

# Import the shared Dataset object that this DAG consumes
# Import email alert function
from dags.datasets import POSTGRES_DWH_RAW_DATASET
from dags.utils import send_failure_email

# --- dbt Configuration ---
# Set the directory of the dbt project and the path to the dbt executable
# These are configured in the .env file
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
DBT_EXECUTABLE_PATH = os.getenv("DBT_EXECUTABLE_PATH")

@dag(
    dag_id="dbt_build",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
     # This DAG is scheduled to run only when the POSTGRES_DWH_RAW_DATASET is updated by the 'load' DAG
    schedule=[POSTGRES_DWH_RAW_DATASET],
    catchup=False,
    tags=["dbt", "build"],
    default_args={
        "on_failure_callback": send_failure_email
    },
    doc_md="""
    ### dbt Transformation DAG for Polygon.io Data

    This DAG is triggered by the `stocks_polygon_load` DAG upon the successful
    completion of the data loading process. It runs the `dbt build` command
    to execute all dbt models and tests, transforming the raw data into
    analytics-ready marts.
    """,
)
def stocks_dbt_transform():
    """
    This DAG uses DbtTaskGroup to execute dbt models.
    It is triggered when the raw data table in PostgreSQL is updated.
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
stocks_dbt_transform()
