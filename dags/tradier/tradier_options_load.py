from __future__ import annotations
import pendulum
import json
import os
from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from psycopg2.extras import execute_values
import re

from dags.utils.tradier_datasets import S3_TRADIER_OPTIONS_MANIFEST_DATASET, POSTGRES_DWH_TRADIER_OPTIONS_RAW_DATASET

def parse_option_symbol(symbol):
    """
    Parses a standard OCC option symbol to extract its components.
    Example: AAPL251219C00150000
    """
    match = re.match(r"([A-Z]+)(\d{6})([CP])(\d{8})", symbol)
    if not match:
        return None
    
    underlying = match.group(1)
    exp_date_str = match.group(2)
    option_type = 'call' if match.group(3) == 'C' else 'put'
    strike_price = float(match.group(4)) / 1000.0
    
    # Correctly parse the expiration date
    expiration_date = datetime.strptime(exp_date_str, '%y%m%d').date()
    
    return {
        "underlying_ticker": underlying,
        "expiration_date": expiration_date,
        "option_type": option_type,
        "strike_price": strike_price
    }

@dag(
    dag_id="tradier_options_load",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=[S3_TRADIER_OPTIONS_MANIFEST_DATASET],
    catchup=False,
    tags=["load", "tradier", "options"],
    dagrun_timeout=timedelta(hours=4),
)
def tradier_options_load_dag():
    """
    This DAG loads raw JSON options data from S3 (sourced from Tradier)
    into a PostgreSQL data warehouse. It is triggered by the completion of the
    'options_tradier_ingest' DAG.
    """
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_dwh")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    POSTGRES_TABLE = "source_tradier_options_bars_daily"
    BATCH_SIZE = 1000

    @task
    def get_s3_keys_from_manifest() -> list[str]:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        manifest_key = "manifests/tradier_options_manifest_latest.txt"
        if not s3_hook.check_for_key(manifest_key, bucket_name=BUCKET_NAME):
            raise FileNotFoundError(f"Manifest file not found in S3: {manifest_key}")
        manifest_content = s3_hook.read_key(key=manifest_key, bucket_name=BUCKET_NAME)
        s3_keys = [key for key in manifest_content.strip().splitlines() if key]
        if not s3_keys:
            raise AirflowSkipException("Manifest file is empty. No keys to process.")
        print(f"Found {len(s3_keys)} S3 keys to process from Tradier options manifest: {manifest_key}")
        return s3_keys

    @task
    def batch_s3_keys(s3_keys: list[str]) -> list[list[str]]:
        return [s3_keys[i:i + BATCH_SIZE] for i in range(0, len(s3_keys), BATCH_SIZE)]

    @task
    def transform_batch(batch_of_keys: list[str]) -> dict:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        clean_records, successful_keys = [], []
        for s3_key in batch_of_keys:
            try:
                # Extract the option symbol from the S3 key
                option_symbol = s3_key.split('/')[-1].split('_')[0]
                
                # Parse the symbol to get contract details
                contract_details = parse_option_symbol(option_symbol)
                if not contract_details:
                    print(f"Skipping file with invalid option symbol format: {s3_key}")
                    continue

                file_content = s3_hook.read_key(key=s3_key, bucket_name=BUCKET_NAME)
                data = json.loads(file_content)

                if data.get('history') and data['history'].get('day'):
                    day_data = data['history']['day']
                    if not isinstance(day_data, list):
                        day_data = [day_data]

                    for day in day_data:
                        record = {
                            "option_symbol": option_symbol,
                            "trade_date": day.get("date"),
                            "volume": day.get("volume"),
                            "vwap": day.get("vwap"),
                            "open": day.get("open"),
                            "close": day.get("close"),
                            "high": day.get("high"),
                            "low": day.get("low"),
                        }
                        # Merge the parsed contract details into the record
                        record.update(contract_details)
                        clean_records.append(record)
                    
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

    @task(outlets=[POSTGRES_DWH_TRADIER_OPTIONS_RAW_DATASET])
    def load_to_postgres_incremental(clean_records: list[dict]):
        if not clean_records:
            raise AirflowSkipException("No records to load.")
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg_hook.run(f"""
        CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (
            option_symbol TEXT NOT NULL,
            trade_date DATE NOT NULL,
            underlying_ticker TEXT,
            expiration_date DATE,
            strike_price NUMERIC(19, 4),
            option_type VARCHAR(4),
            "open" NUMERIC(19, 4),
            high NUMERIC(19, 4),
            low NUMERIC(19, 4),
            "close" NUMERIC(19, 4),
            volume BIGINT,
            vwap NUMERIC(19, 4),
            inserted_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
            PRIMARY KEY (option_symbol, trade_date)
        );
        """)
        conn = pg_hook.get_conn()
        try:
            with conn.cursor() as cursor:
                cols = [
                    "option_symbol", "trade_date", "underlying_ticker", "expiration_date",
                    "strike_price", "option_type", "open", "high", "low", "close", "volume", "vwap"
                ]
                values = [tuple(rec.get(col) for col in cols) for rec in clean_records]
                upsert_sql = f"""
                    INSERT INTO {POSTGRES_TABLE} ({', '.join(f'"{c}"' for c in cols)}) VALUES %s
                    ON CONFLICT (option_symbol, trade_date) DO UPDATE SET
                        underlying_ticker = EXCLUDED.underlying_ticker,
                        expiration_date = EXCLUDED.expiration_date,
                        strike_price = EXCLUDED.strike_price,
                        option_type = EXCLUDED.option_type,
                        "open" = EXCLUDED."open", high = EXCLUDED.high, low = EXCLUDED.low,
                        "close" = EXCLUDED."close", volume = EXCLUDED.volume, vwap = EXCLUDED.vwap,
                        inserted_at = NOW();
                """
                execute_values(cursor, upsert_sql, values)
        except Exception as e:
            conn.rollback()
            raise e
        else:
            conn.commit()
            print(f"Successfully merged {len(clean_records)} records into {POSTGRES_TABLE}.")
        finally:
            conn.close()

    @task
    def batch_keys_for_move(s3_keys: list[str]) -> list[list[str]]:
        return [s3_keys[i:i + BATCH_SIZE] for i in range(0, len(s3_keys), BATCH_SIZE)]

    @task
    def move_s3_files_to_processed_batch(s3_key_batch: list[str]):
        if not s3_key_batch:
            return

        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        source_prefix = "raw_data/tradier_options/"
        dest_prefix = "processed/tradier_options/"
        # ... rest of the move logic is the same

    # --- Task Flow Definition ---
    s3_keys = get_s3_keys_from_manifest()
    key_batches = batch_s3_keys(s3_keys)
    transformed_batches = transform_batch.expand(batch_of_keys=key_batches)
    flat_data = flatten_results(transformed_batches)
    load_op = load_to_postgres_incremental(flat_data["records"])
    keys_to_move = batch_keys_for_move(flat_data["keys"])
    move_op = move_s3_files_to_processed_batch.expand(s3_key_batch=keys_to_move)

    load_op >> keys_to_move

tradier_options_load_dag()
