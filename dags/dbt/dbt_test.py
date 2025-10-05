from __future__ import annotations
import os
import pendulum
import json
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from psycopg2.extras import execute_values

# --- dbt Configuration ---
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")
DBT_EXECUTABLE_PATH = os.getenv("DBT_EXECUTABLE_PATH", "/usr/local/airflow/dbt_venv/bin/dbt")
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_dwh")

@dag(
    dag_id="dbt_test",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 2 * * *", # Runs daily at 2 AM, after the main pipeline
    catchup=False,
    tags=["dbt", "monitoring"],
    doc_md="""
    ### dbt Test Runner and Parser DAG

    This DAG runs `dbt test` and then uses a Python task to parse the
    `run_results.json` artifact and load the results into a table
    in the data warehouse for monitoring.
    """,
)
def dbt_test_dag():
    """
    This DAG runs dbt tests and loads the results to a table.
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
        operator_args={"dbt_cmd": "test"}
    )

    @task
    def parse_and_load_test_results():
        """
        Parses the dbt run_results.json artifact and loads the test
        results into a dedicated table in the data warehouse.
        """
        run_results_path = os.path.join(DBT_PROJECT_DIR, "target/run_results.json")
        
        try:
            with open(run_results_path, "r") as f:
                data = json.load(f)
        except FileNotFoundError:
            print(f"Could not find run_results.json at {run_results_path}. Skipping.")
            return

        test_results = []
        for result in data.get("results", []):
            if result["unique_id"].startswith("test."):
                test_results.append({
                    "test_id": result["unique_id"],
                    "test_name": result["unique_id"].split(".")[-1],
                    "status": result["status"],
                    "failures": result.get("failures", 0),
                    "execution_time": result["execution_time"],
                    "run_generated_at": data["metadata"]["generated_at"]
                })

        if not test_results:
            print("No test results found in the artifact. Skipping load.")
            return

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        
        with conn.cursor() as cursor:
            # Create table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS public.dbt_test_results (
                    test_id TEXT PRIMARY KEY,
                    test_name TEXT,
                    status VARCHAR(10),
                    failures INTEGER,
                    execution_time FLOAT,
                    run_generated_at TIMESTAMPTZ,
                    loaded_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            # Prepare data for upsert
            cols = ["test_id", "test_name", "status", "failures", "execution_time", "run_generated_at"]
            values = [tuple(res.get(col) for col in cols) for res in test_results]
            
            # Use ON CONFLICT to upsert results
            upsert_sql = f"""
                INSERT INTO public.dbt_test_results ({", ".join(cols)})
                VALUES %s
                ON CONFLICT (test_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    failures = EXCLUDED.failures,
                    execution_time = EXCLUDED.execution_time,
                    run_generated_at = EXCLUDED.run_generated_at,
                    loaded_at = NOW();
            """
            execute_values(cursor, upsert_sql, values)
        
        conn.commit()
        conn.close()
        print(f"Successfully loaded {len(test_results)} test results.")

    # Set the task dependency
    run_dbt_tests >> parse_and_load_test_results()

dbt_test_dag()
