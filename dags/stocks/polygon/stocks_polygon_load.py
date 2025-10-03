from __future__ import annotations
import pendulum
import json
import os
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from psycopg2.extras import execute_values

# Import the shared Dataset objects for data-driven scheduling
from dags.datasets import S3_MANIFEST_DATASET, POSTGRES_DWH_RAW_DATASET

# Define the DAG
@dag(
    dag_id="stocks_polygon_load",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=[S3_MANIFEST_DATASET],
    catchup=False,
    tags=["load", "polygon"],
    dagrun_timeout=timedelta(hours=2),
)
def stocks_polygon_load_dag():
    """
    This DAG is responsible for loading the raw JSON data from S3 into a
    PostgreSQL data warehouse. It is triggered by the completion of the
    'stocks_polygon_ingest' DAG.
    """
    # Define connection and configuration variables from environment variables or use defaults
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_dwh")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    POSTGRES_TABLE = "source_polygon_stock_bars_daily"
    BATCH_SIZE = 1500

    @task
    def get_s3_keys_from_manifest() -> list[str]:
        """
        Reads the list of S3 keys from the 'latest' manifest file, which was
        created by the upstream 'ingest' DAG.
        """
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
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
        """
        Batches S3 keys for parallel processing in downstream tasks.
        """
        batches = [s3_keys[i:i + BATCH_SIZE] for i in range(0, len(s3_keys), BATCH_SIZE)]
        print(f"Created {len(batches)} batches of approximately {BATCH_SIZE} keys each.")
        return batches

    @task
    def transform_batch(batch_of_keys: list[str]) -> dict:
        """
        Reads a batch of JSON files from S3, extracts the relevant data,
        and transforms it into a clean, tabular format.
        """
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        clean_records, successful_keys = [], []
        for s3_key in batch_of_keys:
            try:
                file_content = s3_hook.read_key(key=s3_key, bucket_name=BUCKET_NAME)
                data = json.loads(file_content)
                # Ensure the JSON file contains the expected data structure
                if data.get("resultsCount") == 1 and data.get("results"):
                    result = data["results"][0]
                    trade_date = pendulum.from_timestamp(result["t"] / 1000).to_date_string()
                    clean_records.append({
                        "ticker": data["ticker"], "trade_date": trade_date,
                        "volume": result.get("v"), "vwap": result.get("vw"), "open": result.get("o"),
                        "close": result.get("c"), "high": result.get("h"), "low": result.get("l"),
                        "transactions": result.get("n"),
                    })
                    successful_keys.append(s3_key)
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                print(f"Skipping file {s3_key} due to processing error: {e}")
                continue
        return {"records": clean_records, "keys": successful_keys}

    @task
    def flatten_results(transformed_batches: list[dict]) -> dict:
        """
        Flattens the results from all parallel 'transform_batch' tasks into a
        single list of records and a single list of S3 keys.
        """
        all_records = [rec for batch in transformed_batches for rec in batch['records'] if rec]
        all_keys = [key for batch in transformed_batches for key in batch['keys'] if key]
        return {"records": all_records, "keys": all_keys}

    @task(outlets=[POSTGRES_DWH_RAW_DATASET])
    def load_to_postgres_incremental(clean_records: list[dict]):
        """
        Loads the transformed records into a PostgreSQL table. This function
        uses an 'upsert' (update or insert) operation to ensure data integrity
        and avoid duplicates. This will also trigger the downstream 'dbt' DAG.
        """
        if not clean_records:
            raise AirflowSkipException("No records to load.")
        # Create the table if it doesn't already exist
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg_hook.run(f"""
        CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (
            ticker TEXT NOT NULL, trade_date DATE NOT NULL, "open" NUMERIC(19, 4),
            high NUMERIC(19, 4), low NUMERIC(19, 4), "close" NUMERIC(19, 4),
            volume BIGINT, vwap NUMERIC(19, 4), transactions BIGINT,
            inserted_at TIMESTAMPTZ DEFAULT NOW() NOT NULL, PRIMARY KEY (ticker, trade_date)
        );
        """)

        print(f"Preparing to bulk merge {len(clean_records)} records into {POSTGRES_TABLE}.")
        conn = pg_hook.get_conn()
        try:
            with conn.cursor() as cursor:
                cols = ["ticker", "trade_date", "volume", "vwap", "open", "close", "high", "low", "transactions"]
                values = [tuple(rec.get(col) for col in cols) for rec in clean_records]
                # SQL statement for the 'upsert' operation
                upsert_sql = f"""
                    INSERT INTO {POSTGRES_TABLE} ({', '.join(f'"{c}"' for c in cols)}) VALUES %s
                    ON CONFLICT (ticker, trade_date) DO UPDATE SET
                        volume = EXCLUDED.volume, vwap = EXCLUDED.vwap, "open" = EXCLUDED."open",
                        "close" = EXCLUDED."close", high = EXCLUDED.high, low = EXCLUDED.low,
                        transactions = EXCLUDED.transactions, inserted_at = NOW();
                """
                execute_values(cursor, upsert_sql, values)
        except Exception as e:
            conn.rollback()
            raise e
        else:
            conn.commit()
            print(f"Successfully merged {len(clean_records)} records.")
        finally:
            conn.close()

    @task
    def batch_keys_for_move(s3_keys: list[str]) -> list[list[str]]:
        """
        Batches the S3 keys of successfully processed files for the move operation.
        """
        return [s3_keys[i:i + BATCH_SIZE] for i in range(0, len(s3_keys), BATCH_SIZE)]

    @task
    def move_s3_files_to_processed_batch(s3_key_batch: list[str]):
        """
        Moves a batch of successfully processed S3 files from the 'raw_data'
        directory to a 'processed' directory for archiving.
        """
        if not s3_key_batch:
            print("No S3 keys in this batch to move.")
            return

        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        source_prefix = "raw_data/"
        dest_prefix = "processed/"
        copied_keys_for_deletion = []

        for s3_key in s3_key_batch:
            try:
                dest_key = s3_key.replace(source_prefix, dest_prefix, 1)
                s3_hook.copy_object(
                    source_bucket_key=s3_key, dest_bucket_key=dest_key,
                    source_bucket_name=BUCKET_NAME, dest_bucket_name=BUCKET_NAME,
                )
                copied_keys_for_deletion.append(s3_key)
            except Exception as e:
                print(f"ERROR: Failed to copy '{s3_key}'. Error: {e}")
        # Delete the original files after they have been copied
        if copied_keys_for_deletion:
            s3_hook.delete_objects(bucket=BUCKET_NAME, keys=copied_keys_for_deletion)
            print(f"Successfully moved {len(copied_keys_for_deletion)} files.")

    # --- Task Flow Definition ---
    s3_keys = get_s3_keys_from_manifest()
    key_batches_for_transform = batch_s3_keys(s3_keys)
    transformed_batches = transform_batch.expand(batch_of_keys=key_batches_for_transform)
    flat_data = flatten_results(transformed_batches)
    
    load_op = load_to_postgres_incremental(flat_data["records"])
    
    key_batches_for_move = batch_keys_for_move(flat_data["keys"])
    
    move_op = move_s3_files_to_processed_batch.expand(s3_key_batch=key_batches_for_move)

    # The move operation is dependent on a successful load operation
    load_op >> key_batches_for_move

# Instantiate the DAG
stocks_polygon_load_dag()
