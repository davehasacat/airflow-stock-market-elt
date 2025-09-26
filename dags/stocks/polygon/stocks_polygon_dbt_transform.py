from __future__ import annotations
import os
import pendulum
from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

# Import the shared Dataset objects
from dags.datasets import POSTGRES_DWH_RAW_DATASET

# --- dbt Configuration ---
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
DBT_EXECUTABLE_PATH = os.getenv("DBT_EXECUTABLE_PATH")

@dag(
    dag_id="stocks_polygon_dbt_transform",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=[POSTGRES_DWH_RAW_DATASET], # This DAG runs only when the postgres DWH raw table is updated
    catchup=False,
    tags=["dbt", "transform", "polygon"],
    doc_md="""
    ### dbt Transformation DAG for Polygon.io Data

    This DAG is triggered by the `stocks_polygon_load` DAG upon the successful
    completion of the data loading process. It runs the `dbt build` command
    to execute all dbt models and tests, transforming the raw data into
    analytics-ready marts.
    """,
)
def stocks_polygon_dbt_transform_dag():
    """
    This DAG runs the dbt build command to transform the raw Polygon.io data.
    """
    dbt_build = DbtTaskGroup(
        group_id="dbt_build_all_models",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=ProfileConfig(
            profile_name="stock_market_elt",
            target_name="dev",
            profiles_yml_filepath=os.path.join(DBT_PROJECT_DIR, "profiles.yml"),
        ),
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH),
        operator_args={
            "dbt_cmd": "build"
        }
    )

stocks_polygon_dbt_transform_dag()
