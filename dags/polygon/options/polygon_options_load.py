from __future__ import annotations
import pendulum
import json
import os
import re
from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from psycopg2.extras import execute_values

# In a real scenario, this would be defined in dags/utils/polygon_datasets.py
from airflow.datasets import Dataset
S3_POLYGON_OPTIONS_MANIFEST_DATASET = Dataset("s3://test/manifests/polygon_options_manifest_latest.txt")
POSTGRES_DWH_POLYGON_OPTIONS_RAW_DATASET = Dataset("postgres_dwh://public/source_polygon_options_bars_daily")


def parse_option_symbol(symbol):
    """
    Parses a standard Polygon option symbol to extract its components.
    Example: O:SPY251219C00150000
    """
    if symbol.startswith("O:"):
        symbol = symbol[2:]

    match = re.match(r"([A-Z\.]+)(\d{6})([CP])(\d{8})", symbol)
    if not match:
        return None
    
    underlying = match.group(1)
    exp_date_str = match.group(2)
    option_type = 'call' if match.group(3) == 'C' else 'put'
    strike_price = float(match.group(4)) / 1000.0
    
    expiration_date = datetime.strptime(exp_date_str, '%y%m%d').date()
    
    return {
        "underlying_ticker": underlying,
        "expiration_date": expiration_date,
        "option_type": option_type,
        "strike_price": strike_price
    }

@dag(
    dag_id="polygon_options_load",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=[S3_POLYGON_OPTIONS_MANIFEST_DATASET],
    catchup=False,
    tags=["load", "polygon", "options"],
    dagrun_timeout=timedelta(hours=4),
)
def polygon_options_load_dag():
    """
    This DAG loads raw JSON options data from S3 (sourced from Polygon)
    into a PostgreSQL data warehouse. It is triggered by the completion of an
    options ingest DAG.
    """
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_dwh")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    POSTGRES_TABLE = "source_polygon_options_bars_daily"
    BATCH_SIZE = 1000

    @task
    def get_and_batch_s3_keys() -> list[list[str]]:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        manifest_key = "manifests/polygon_options_manifest_latest.txt"
        if not s3_hook.check_for_key(manifest_key, bucket_name=BUCKET_NAME):
            raise FileNotFoundError(f"Manifest file not found in S3: {manifest_key}")
        
        manifest_content = s3_hook.read_key(key=manifest_key, bucket_name=BUCKET_NAME)
        s3_keys = [key for key in manifest_content.strip().splitlines() if key]
        if not s3_keys:
            raise AirflowSkipException("Manifest file is empty. No keys to process.")
        
        print(f"Found {len(s3_keys)} S3 keys. Batching into groups of {BATCH_SIZE}.")
        return [s3_keys[i:i + BATCH_SIZE] for i in range(0, len(s3_keys), BATCH_SIZE)]
    
    @task
    def create_table():
        """Creates the target table if it doesn't exist."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg_hook.run(f"""
        CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (
            option_symbol TEXT NOT NULL, trade_date DATE NOT NULL,
            underlying_ticker TEXT, expiration_date DATE, strike_price NUMERIC(19, 4),
            option_type VARCHAR(4), "open" NUMERIC(19, 4), high NUMERIC(19, 4),
            low NUMERIC(19, 4), "close" NUMERIC(19, 4), volume BIGINT,
            vwap NUMERIC(19, 4), transactions BIGINT,
            inserted_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
            PRIMARY KEY (option_symbol, trade_date)
        );
        """)

    @task
    def transform_batch(batch_of_keys: list[str]) -> dict:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        clean_records, successful_keys = [], []
        for s3_key in batch_of_keys:
            try:
                file_content = s3_hook.read_key(key=s3_key, bucket_name=BUCKET_NAME)
                data = json.loads(file_content)
                option_symbol = data.get("ticker")
                if not option_symbol:
                    continue
                
                contract_details = parse_option_symbol(option_symbol)
                if not contract_details:
                    print(f"Skipping file with invalid option symbol format: {s3_key}")
                    continue

                if data.get("resultsCount") == 1 and data.get("results"):
                    result = data["results"][0]
                    record = {
                        "option_symbol": option_symbol,
                        "trade_date": pendulum.from_timestamp(result["t"] / 1000).to_date_string(),
                        "volume": result.get("v"), "vwap": result.get("vw"),
                        "open": result.get("o"), "close": result.get("c"),
                        "high": result.get("h"), "low": result.get("l"),
                        "transactions": result.get("n"),
                    }
                    record.update(contract_details)
                    clean_records.append(record)
                    successful_keys.append(s3_key)
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                print(f"Skipping file {s3_key} due to processing error: {e}")
                continue
        return {"records": clean_records, "keys": successful_keys}

    @task(outlets=[POSTGRES_DWH_POLYGON_OPTIONS_RAW_DATASET])
    def process_and_load_batch(transformed_data: dict):
        clean_records = transformed_data["records"]
        s3_key_batch = transformed_data["keys"]

        if not clean_records:
            print("No records in this batch to load.")
        else:
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = pg_hook.get_conn()
            try:
                with conn.cursor() as cursor:
                    cols = [
                        "option_symbol", "trade_date", "underlying_ticker", "expiration_date",
                        "strike_price", "option_type", "open", "high", "low", "close",
                        "volume", "vwap", "transactions"
                    ]
                    values = [tuple(rec.get(col) for col in cols) for rec in clean_records]
                    upsert_sql = f"""
                        INSERT INTO {POSTGRES_TABLE} ({', '.join(f'"{c}"' for c in cols)}) VALUES %s
                        ON CONFLICT (option_symbol, trade_date) DO UPDATE SET
                            underlying_ticker = EXCLUDED.underlying_ticker,
                            expiration_date = EXCLUDED.expiration_date,
                            strike_price = EXCLUDED.strike_price, option_type = EXCLUDED.option_type,
                            "open" = EXCLUDED."open", high = EXCLUDED.high, low = EXCLUDED.low,
                            "close" = EXCLUDED."close", volume = EXCLUDED.volume, vwap = EXCLUDED.vwap,
                            transactions = EXCLUDED.transactions, inserted_at = NOW();
                    """
                    execute_values(cursor, upsert_sql, values)
            finally:
                conn.commit()
                conn.close()
                print(f"Successfully merged {len(clean_records)} records from batch.")

        if not s3_key_batch:
            print("No S3 keys in this batch to move.")
            return

        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        source_prefix = "raw_data/options/"
        dest_prefix = "processed/options/"
        
        copied_keys = []
        for s3_key in s3_key_batch:
            try:
                dest_key = s3_key.replace(source_prefix, dest_prefix, 1)
                s3_hook.copy_object(
                    source_bucket_key=s3_key, dest_bucket_key=dest_key,
                    source_bucket_name=BUCKET_NAME, dest_bucket_name=BUCKET_NAME
                )
                copied_keys.append(s3_key)
            except Exception as e:
                print(f"ERROR: Failed to copy '{s3_key}'. Error: {e}")

        if copied_keys:
            s3_hook.delete_objects(bucket=BUCKET_NAME, keys=copied_keys)
            print(f"Successfully moved {len(copied_keys)} files in batch.")


    # --- Task Flow Definition ---
    key_batches = get_and_batch_s3_keys()
    table_created = create_table()
    
    # Ensure the table is created before any parallel tasks start
    transformed_batches = transform_batch.expand(batch_of_keys=key_batches)
    table_created >> transformed_batches
    
    process_and_load_batch.expand(transformed_data=transformed_batches)

polygon_options_load_dag()
