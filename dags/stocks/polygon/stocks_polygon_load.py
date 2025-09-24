from __future__ import annotations
import pendulum
import json
import os
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    dag_id="stocks_polygon_load",
    start_date=pendulum.datetime(2025, 9, 17, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["load", "polygon"],
    doc_md="""
    ### Stocks Polygon Load DAG (Incremental)

    This DAG is triggered by `stocks_polygon_ingest`. It takes a list of S3
    keys, processes them in parallel, and performs an incremental load (UPSERT)
    into a Postgres data warehouse.
    """,
)
def stocks_polygon_load_dag():
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_dwh")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    
    POSTGRES_TABLE = "raw_polygon_stock_bars"

    # --- REVISED TASK ---
    @task
    def get_s3_keys_from_trigger(**kwargs) -> list[str]:
        """
        Pulls the list of S3 keys from the dag_run.conf dictionary, which is
        passed via the task context (kwargs).
        """
        dag_run = kwargs["dag_run"]
        s3_keys = dag_run.conf.get("s3_keys")
        
        if not s3_keys:
            raise ValueError("No 's3_keys' found in DAG run configuration. Aborting.")

        print(f"Received {len(s3_keys)} S3 keys to process.")
        return s3_keys

    @task
    def transform_one_file(s3_key: str) -> dict | None:
        """Reads one JSON file from S3, transforms it, and returns a clean dictionary."""
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        file_content = s3_hook.read_key(key=s3_key, bucket_name=BUCKET_NAME)
        data = json.loads(file_content)

        if data.get("resultsCount") == 1 and data.get("results"):
            result = data["results"][0]
            trade_date = pendulum.from_timestamp(result["t"] / 1000).to_date_string()
            
            return {
                "ticker": data["ticker"],
                "trade_date": trade_date,
                "volume": result["v"],
                "vwap": result.get("vw"),
                "open": result["o"],
                "close": result["c"],
                "high": result["h"],
                "low": result["l"],
                "transactions": result["n"],
            }
        return None

    @task
    def load_to_postgres_incremental(clean_records: list[dict]):
        """Performs an incremental load (UPSERT) into the target Postgres table."""
        records_to_insert = [record for record in clean_records if record]

        if not records_to_insert:
            print("No new records to load. Skipping warehouse operation.")
            return

        print(f"Preparing to incrementally load {len(records_to_insert)} records into {POSTGRES_TABLE}.")
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        target_fields = list(records_to_insert[0].keys())
        conflict_cols = ["ticker", "trade_date"]

        pg_hook.insert_rows(
            table=POSTGRES_TABLE,
            rows=records_to_insert,
            target_fields=target_fields,
            commit_every=1000,
            on_conflict="do_update",
            conflict_target=conflict_cols,
            index_elements=target_fields
        )
        print(f"Successfully merged {len(records_to_insert)} records into {POSTGRES_TABLE}.")

    # --- DEFINE THE TASK FLOW (no changes needed here) ---
    s3_keys_list = get_s3_keys_from_trigger()
    transformed_data = transform_one_file.expand(s3_key=s3_keys_list)
    load_to_postgres_incremental(transformed_data)

stocks_polygon_load_dag()
