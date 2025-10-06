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

from dags.utils.tradier_datasets import S3_TRADIER_MANIFEST_DATASET, POSTGRES_DWH_TRADIER_RAW_DATASET

@dag(
    dag_id="stocks_tradier_load",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=[S3_TRADIER_MANIFEST_DATASET],
    catchup=False,
    tags=["load", "tradier"],
    dagrun_timeout=timedelta(hours=2),
)
def stocks_tradier_load_dag():
    """
    This DAG loads raw JSON data for S&P 500 stocks from S3 (sourced from Tradier)
    into a PostgreSQL data warehouse. It's triggered by the completion of the
    'stocks_tradier_ingest' DAG.
    """
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_dwh")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    POSTGRES_TABLE = "source_tradier_stock_bars_daily"
    BATCH_SIZE = 1000

    @task
    def get_s3_keys_from_manifest() -> list[str]:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        manifest_key = "manifests/tradier_manifest_latest.txt"
        if not s3_hook.check_for_key(manifest_key, bucket_name=BUCKET_NAME):
            raise FileNotFoundError(f"Manifest file not found in S3: {manifest_key}")
        manifest_content = s3_hook.read_key(key=manifest_key, bucket_name=BUCKET_NAME)
        s3_keys = [key for key in manifest_content.strip().splitlines() if key]
        if not s3_keys:
            raise AirflowSkipException("Manifest file is empty. No keys to process.")
        print(f"Found {len(s3_keys)} S3 keys to process from Tradier manifest: {manifest_key}")
        return s3_keys

    @task
    def batch_s3_keys(s3_keys: list[str]) -> list[list[str]]:
        return [s3_keys[i:i + BATCH_SIZE] for i in range(0, len(s3_keys), BATCH_SIZE)]

    @task
    def transform_batch(batch_of_keys: list[str]) -> dict:
        """
        Reads a batch of JSON files from S3, extracts the relevant data including VWAP,
        and transforms it into a clean, tabular format. This version handles
        the varying structure of the Tradier API's 'day' field.
        """
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        clean_records, successful_keys = [], []
        for s3_key in batch_of_keys:
            try:
                file_content = s3_hook.read_key(key=s3_key, bucket_name=BUCKET_NAME)
                data = json.loads(file_content)
                ticker = s3_key.split('/')[-1].split('_')[0]

                if data.get('history') and data['history'].get('day'):
                    # The 'day' field can be a dictionary or a list of dictionaries
                    day_data = data['history']['day']
                    if not isinstance(day_data, list):
                        day_data = [day_data]  # Ensure it's always a list

                    for day in day_data:
                        clean_records.append({
                            "ticker": ticker,
                            "trade_date": day.get("date"),
                            "volume": day.get("volume"),
                            "vwap": day.get("vwap"),
                            "open": day.get("open"),
                            "close": day.get("close"),
                            "high": day.get("high"),
                            "low": day.get("low"),
                        })
                    successful_keys.append(s3_key)
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                print(f"Skipping file {s3_key} due to processing error: {e}")
                continue
        return {"records": clean_records, "keys": successful_keys}

    @task
    def flatten_results(transformed_batches: list[dict]) -> dict:
        all_records = [rec for batch in transformed_batches for rec in batch['records'] if rec]
        all_keys = [key for batch in transformed_batches for key in batch['keys'] if key]
        return {"records": all_records, "keys": all_keys}

    @task(outlets=[POSTGRES_DWH_TRADIER_RAW_DATASET])
    def load_to_postgres_incremental(clean_records: list[dict]):
        """
        Loads the transformed records into a PostgreSQL table. This function
        uses an 'upsert' (update or insert) operation to ensure data integrity
        and avoid duplicates. It now includes the VWAP column.
        """
        if not clean_records:
            raise AirflowSkipException("No records to load.")
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg_hook.run(f"""
        CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (
            ticker TEXT NOT NULL, trade_date DATE NOT NULL, "open" NUMERIC(19, 4),
            high NUMERIC(19, 4), low NUMERIC(19, 4), "close" NUMERIC(19, 4),
            volume BIGINT, vwap NUMERIC(19, 4),
            inserted_at TIMESTAMPTZ DEFAULT NOW() NOT NULL, PRIMARY KEY (ticker, trade_date)
        );
        """)
        conn = pg_hook.get_conn()
        try:
            with conn.cursor() as cursor:
                cols = ["ticker", "trade_date", "volume", "vwap", "open", "close", "high", "low"]
                values = [tuple(rec.get(col) for col in cols) for rec in clean_records]
                upsert_sql = f"""
                    INSERT INTO {POSTGRES_TABLE} ({', '.join(f'"{c}"' for c in cols)}) VALUES %s
                    ON CONFLICT (ticker, trade_date) DO UPDATE SET
                        volume = EXCLUDED.volume, vwap = EXCLUDED.vwap, "open" = EXCLUDED."open",
                        "close" = EXCLUDED."close", high = EXCLUDED.high, low = EXCLUDED.low,
                        inserted_at = NOW();
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
        return [s3_keys[i:i + BATCH_SIZE] for i in range(0, len(s3_keys), BATCH_SIZE)]

    @task
    def move_s3_files_to_processed_batch(s3_key_batch: list[str]):
        if not s3_key_batch:
            print("No S3 keys in this batch to move.")
            return

        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        source_prefix = "raw_data/tradier/"
        dest_prefix = "processed/tradier/"
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
        if copied_keys_for_deletion:
            s3_hook.delete_objects(bucket=BUCKET_NAME, keys=copied_keys_for_deletion)
            print(f"Successfully moved {len(copied_keys_for_deletion)} files.")

    s3_keys = get_s3_keys_from_manifest()
    key_batches_for_transform = batch_s3_keys(s3_keys)
    transformed_batches = transform_batch.expand(batch_of_keys=key_batches_for_transform)
    flat_data = flatten_results(transformed_batches)
    
    load_op = load_to_postgres_incremental(flat_data["records"])
    
    key_batches_for_move = batch_keys_for_move(flat_data["keys"])
    
    move_op = move_s3_files_to_processed_batch.expand(s3_key_batch=key_batches_for_move)

    load_op >> key_batches_for_move

stocks_tradier_load_dag()
