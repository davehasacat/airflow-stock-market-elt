from __future__ import annotations
import pendulum
import os
import requests
import json
import csv
import time
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException

from dags.utils.tradier_datasets import S3_TRADIER_OPTIONS_MANIFEST_DATASET

@dag(
    dag_id="options_tradier_ingest",
    start_date=pendulum.datetime(2025, 10, 1, tz="UTC"),
    schedule="0 1 * * 1-5",  # Runs at 1 AM on weekdays
    catchup=True,
    tags=["ingestion", "tradier", "options"],
)
def options_tradier_ingest_dag():
    """
    This DAG ingests options market data from the Tradier API for S&P 500 stocks.
    It fetches all option contracts for each ticker and then ingests the daily
    OHLCV data for each contract into MinIO S3 storage.
    """
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")
    # Stricter rate limit for the high volume of calls in this DAG
    REQUEST_DELAY = 1.0  # 1 second delay = 60 requests per minute

    @task(pool="api_pool")
    def get_all_option_symbols(**kwargs) -> list[str]:
        """
        For each S&P 500 ticker, fetches all available expiration dates,
        then fetches the option chain for each expiration to get all
        individual option contract symbols.
        """
        conn = BaseHook.get_connection('tradier_api')
        api_key = conn.password
        headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}

        sp500_tickers_path = os.path.join(DBT_PROJECT_DIR, "seeds", "options_tickers.csv")
        underlying_tickers = []
        with open(sp500_tickers_path, mode='r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                underlying_tickers.append(row["ticker"])

        all_option_symbols = []
        for ticker in underlying_tickers:
            # 1. Get Expiration Dates for the ticker
            expirations_url = f"https://api.tradier.com/v1/markets/options/expirations?symbol={ticker}"
            try:
                time.sleep(REQUEST_DELAY)
                exp_response = requests.get(expirations_url, headers=headers)
                exp_response.raise_for_status()
                exp_data = exp_response.json()
                
                # Check if the 'expirations' key exists and is not null
                expirations_data = exp_data.get('expirations')
                if not expirations_data:
                    print(f"No options expirations found for ticker {ticker}. Skipping.")
                    continue

                expirations = expirations_data.get('date', [])
                if not isinstance(expirations, list):
                    expirations = [expirations]

                # 2. Get Option Chains for each expiration
                for exp_date in expirations:
                    chain_url = f"https://api.tradier.com/v1/markets/options/chains?symbol={ticker}&expiration={exp_date}"
                    time.sleep(REQUEST_DELAY)
                    chain_response = requests.get(chain_url, headers=headers)
                    chain_response.raise_for_status()
                    chain_data = chain_response.json()

                    if chain_data.get('options') and chain_data['options'].get('option'):
                        contracts = chain_data['options']['option']
                        if not isinstance(contracts, list):
                            contracts = [contracts]
                        
                        for contract in contracts:
                            all_option_symbols.append(contract['symbol'])

            except requests.exceptions.HTTPError as e:
                print(f"Could not process {ticker}. Error: {e}")
                continue
        
        if not all_option_symbols:
            raise AirflowSkipException("No option symbols were found.")

        print(f"Found {len(all_option_symbols)} total option contracts to process.")
        return all_option_symbols

    @task(retries=3, retry_delay=pendulum.duration(minutes=10), pool="api_pool")
    def process_option_symbol_batch(option_symbols: list[str], **kwargs) -> list[str]:
        """
        Fetches the daily OHLCV data for a batch of option symbols for the
        given execution date and saves the raw JSON response to S3.
        """
        execution_date = kwargs["ds"]
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        processed_s3_keys = []
        target_date = pendulum.parse(execution_date).subtract(days=1).to_date_string()
        conn = BaseHook.get_connection('tradier_api')
        api_key = conn.password
        headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}

        for symbol in option_symbols:
            history_url = f"https://api.tradier.com/v1/markets/history?symbol={symbol}&interval=daily&start={target_date}&end={target_date}"
            try:
                time.sleep(REQUEST_DELAY)
                response = requests.get(history_url, headers=headers)
                response.raise_for_status()
                data = response.json()

                if data.get('history') and data['history'].get('day'):
                    json_string = json.dumps(data)
                    s3_key = f"raw_data/tradier_options/{symbol}_{target_date}.json"
                    s3_hook.load_string(string_data=json_string, key=s3_key, bucket_name=BUCKET_NAME, replace=True)
                    processed_s3_keys.append(s3_key)

            except requests.exceptions.HTTPError as e:
                print(f"HTTP Error for option symbol {symbol}: {e}")
                if e.response.status_code == 429:
                    print("Rate limit exceeded. Waiting for 60 seconds.")
                    time.sleep(60)
                continue
        
        return processed_s3_keys

    @task
    def batch_symbols(symbol_list: list[str]) -> list[list[str]]:
        """Batches the list of option symbols for parallel processing."""
        batch_size = 500  # Smaller batches for this more intensive task
        return [symbol_list[i:i + batch_size] for i in range(0, len(symbol_list), batch_size)]

    @task
    def flatten_s3_key_list(nested_list: list[list[str]]) -> list[str]:
        return [key for sublist in nested_list for key in sublist if key]

    @task(outlets=[S3_TRADIER_OPTIONS_MANIFEST_DATASET])
    def write_manifest_to_s3(s3_keys: list[str], **kwargs):
        if not s3_keys:
            raise AirflowSkipException("No S3 keys to create a manifest for.")

        manifest_content = "\n".join(s3_keys)
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        manifest_key = "manifests/tradier_options_manifest_latest.txt"
        s3_hook.load_string(string_data=manifest_content, key=manifest_key, bucket_name=BUCKET_NAME, replace=True)
        print(f"Tradier options manifest file updated: {manifest_key}")

    # --- Task Flow Definition ---
    all_symbols = get_all_option_symbols()
    symbol_batches = batch_symbols(all_symbols)
    processed_keys_nested = process_option_symbol_batch.expand(option_symbols=symbol_batches)
    s3_keys_flat = flatten_s3_key_list(processed_keys_nested)
    write_manifest_to_s3(s3_keys_flat)

options_tradier_ingest_dag()
