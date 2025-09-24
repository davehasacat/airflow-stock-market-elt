from __future__ import annotations
import pendulum
import json
import os
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    dag_id="stocks_polygon_load",
    start_date=pendulum.datetime(2025, 9, 22, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["load", "polygon"],
    doc_md="""
    ### Stocks Polygon Load DAG (Incremental, Batch)

    This DAG is triggered by `stocks_polygon_ingest`. It groups a large list of S3
    keys into batches and processes these batches in parallel before performing
    an incremental load into Postgres.
    """,
)
def stocks_polygon_load_dag():
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_dwh")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    POSTGRES_TABLE = "raw_polygon_stock_bars"
    BATCH_SIZE = 500 # Process 500 files per mapped task

    @task
    def get_s3_keys_from_trigger(**kwargs) -> list[str]:
        """Pulls the list of S3 keys from the dag_run.conf dictionary."""
        dag_run = kwargs["dag_run"]
        s3_keys = dag_run.conf.get("s3_keys")
        
        if not s3_keys:
            raise ValueError("No 's3_keys' found in DAG run configuration. Aborting.")
        
        print(f"Received {len(s3_keys)} S3 keys to process.")
        return s3_keys

    # --- NEW TASK TO CREATE BATCHES ---
    @task
    def batch_s3_keys(s3_keys: list[str]) -> list[list[str]]:
        """Groups the incoming list of S3 keys into smaller batches."""
        batches = [s3_keys[i:i + BATCH_SIZE] for i in range(0, len(s3_keys), BATCH_SIZE)]
        print(f"Created {len(batches)} batches of approximately {BATCH_SIZE} keys each.")
        return batches

    # --- MODIFIED TASK TO PROCESS A BATCH ---
    @task
    def transform_batch(batch_of_keys: list[str]) -> list[dict]:
        """Reads and transforms a batch of JSON files from S3."""
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        clean_records = []
        
        for s3_key in batch_of_keys:
            file_content = s3_hook.read_key(key=s3_key, bucket_name=BUCKET_NAME)
            data = json.loads(file_content)

            if data.get("resultsCount") == 1 and data.get("results"):
                result = data["results"][0]
                trade_date = pendulum.from_timestamp(result["t"] / 1000).to_date_string()
                
                clean_record = {
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
                clean_records.append(clean_record)
        return clean_records

    # --- NEW TASK TO FLATTEN RESULTS ---
    @task
    def flatten_results(nested_list: list[list[dict]]) -> list[dict]:
        """Takes the nested list of results and returns a single flat list."""
        return [record for batch in nested_list for record in batch if record]

    @task
    def load_to_postgres_incremental(clean_records: list[dict]):
        """Performs an incremental load (UPSERT) into the target Postgres table."""
        if not clean_records:
            print("No new records to load. Skipping warehouse operation.")
            return

        print(f"Preparing to incrementally load {len(clean_records)} records into {POSTGRES_TABLE}.")
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        target_fields = list(clean_records[0].keys())
        conflict_cols = ["ticker", "trade_date"]

        pg_hook.insert_rows(
            table=POSTGRES_TABLE,
            rows=clean_records,
            target_fields=target_fields,
            commit_every=1000,
            on_conflict="do_update",
            conflict_target=conflict_cols,
            index_elements=target_fields
        )
        print(f"Successfully merged {len(clean_records)} records into {POSTGRES_TABLE}.")

    # --- DEFINE THE NEW TASK FLOW ---
    s3_keys = get_s3_keys_from_trigger()
    key_batches = batch_s3_keys(s3_keys)
    
    # Map over the batches of keys, not the individual keys
    transformed_batches = transform_batch.expand(batch_of_keys=key_batches)
    
    # Flatten the list of lists before loading
    flat_records = flatten_results(transformed_batches)
    
    load_to_postgres_incremental(flat_records)

stocks_polygon_load_dag()
