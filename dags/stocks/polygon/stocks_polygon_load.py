from __future__ import annotations
import pendulum
import json
import os
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException

# Import the shared Dataset objects
from dags.datasets import S3_MANIFEST_DATASET, POSTGRES_DWH_RAW_DATASET

@dag(
    dag_id="stocks_polygon_load",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=[S3_MANIFEST_DATASET],
    catchup=False,
    tags=["load", "polygon"],
)
def stocks_polygon_load_dag():
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_dwh")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    POSTGRES_TABLE = "source_polygon_stock_bars_daily"
    BATCH_SIZE = 1000

    @task
    def get_s3_keys_from_manifest() -> list[str]:
        """Reads the list of S3 keys from the 'latest' manifest file."""
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        
        # Read from the fixed, predictable filename.
        manifest_key = "manifests/manifest_latest.txt"

        if not s3_hook.check_for_key(manifest_key, bucket_name=BUCKET_NAME):
            raise FileNotFoundError(f"Manifest file not found in S3: {manifest_key}")

        manifest_content = s3_hook.read_key(key=manifest_key, bucket_name=BUCKET_NAME)
        s3_keys = [key for key in manifest_content.strip().splitlines() if key]

        if not s3_keys:
            raise AirflowSkipException("Manifest file is empty. No keys to process.")
        
        print(f"Found {len(s3_keys)} S3 keys to process from manifest: {manifest_key}")
        return s3_keys

    @task
    def batch_s3_keys(s3_keys: list[str]) -> list[list[str]]:
        batches = [s3_keys[i:i + BATCH_SIZE] for i in range(0, len(s3_keys), BATCH_SIZE)]
        print(f"Created {len(batches)} batches of approximately {BATCH_SIZE} keys each.")
        return batches

    @task
    def transform_batch(batch_of_keys: list[str]) -> list[dict]:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        clean_records = []
        
        for s3_key in batch_of_keys:
            try:
                file_content = s3_hook.read_key(key=s3_key, bucket_name=BUCKET_NAME)
                data = json.loads(file_content)

                if data.get("resultsCount") == 1 and data.get("results"):
                    result = data["results"][0]
                    trade_date = pendulum.from_timestamp(result["t"] / 1000).to_date_string()
                    
                    clean_record = {
                        "ticker": data["ticker"],
                        "trade_date": trade_date,
                        "volume": result.get("v"),
                        "vwap": result.get("vw"),
                        "open": result.get("o"),
                        "close": result.get("c"),
                        "high": result.get("h"),
                        "low": result.get("l"),
                        "transactions": result.get("n"),
                    }
                    clean_records.append(clean_record)
            except Exception as e:
                print(f"Error processing file {s3_key}: {e}")
                continue
        return clean_records

    @task
    def flatten_results(nested_list: list[list[dict]]) -> list[dict]:
        return [record for batch in nested_list for record in batch if record]

    @task(outlets=[POSTGRES_DWH_RAW_DATASET])
    def load_to_postgres_incremental(clean_records: list[dict]):
        if not clean_records:
            print("No new records to load. Skipping warehouse operation.")
            return

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (
            ticker TEXT NOT NULL,
            trade_date DATE NOT NULL,
            "open" NUMERIC(19, 4),
            high NUMERIC(19, 4),
            low NUMERIC(19, 4),
            "close" NUMERIC(19, 4),
            volume BIGINT,
            vwap NUMERIC(19, 4),
            transactions BIGINT,
            inserted_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
            PRIMARY KEY (ticker, trade_date)
        );
        """
        pg_hook.run(create_table_sql)
        
        print(f"Preparing to incrementally load {len(clean_records)} records into {POSTGRES_TABLE}.")
        
        rows_to_insert = [tuple(rec.values()) for rec in clean_records]
        target_fields = list(clean_records[0].keys())
        
        update_cols = [col for col in target_fields if col not in ["ticker", "trade_date"]]
        
        pg_hook.insert_rows(
            table=POSTGRES_TABLE,
            rows=rows_to_insert,
            target_fields=target_fields,
            commit_every=1000,
            on_conflict="do_update",
            conflict_target=["ticker", "trade_date"],
            update_columns=update_cols
        )
        print(f"Successfully merged {len(clean_records)} records into {POSTGRES_TABLE}.")

    @task
    def move_s3_files_to_processed(s3_keys: list[str]):
        """Moves the processed S3 files to a 'processed' directory."""
        if not s3_keys:
            print("No S3 keys to move.")
            return

        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        keys_to_delete = []
        for s3_key in s3_keys:
            if s3_hook.check_for_key(s3_key, bucket_name=BUCKET_NAME):
                # Destination key replaces the 'raw_data/' prefix with 'processed/'
                dest_key = s3_key.replace("raw_data/", "processed/", 1)
                
                # Copy the object to the new location
                s3_hook.copy_object(
                    source_bucket_key=s3_key,
                    dest_bucket_key=dest_key,
                    source_bucket_name=BUCKET_NAME,
                    dest_bucket_name=BUCKET_NAME,
                )
                keys_to_delete.append(s3_key)
            else:
                print(f"Key {s3_key} not found, skipping.")
        
        # Delete the original objects
        if keys_to_delete:
            s3_hook.delete_objects(bucket=BUCKET_NAME, keys=keys_to_delete)

        print(f"Successfully moved {len(keys_to_delete)} files to 'processed/' directory.")


    # --- Task Flow Definition ---
    s3_keys = get_s3_keys_from_manifest()
    key_batches = batch_s3_keys(s3_keys)
    transformed_batches = transform_batch.expand(batch_of_keys=key_batches)
    flat_records = flatten_results(transformed_batches)
    load_op = load_to_postgres_incremental(flat_records)
    
    # The new task depends on the successful completion of the load operation
    move_op = move_s3_files_to_processed(s3_keys)
    load_op >> move_op

stocks_polygon_load_dag()
