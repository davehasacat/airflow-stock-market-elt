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
from airflow.models.param import Param

from airflow.datasets import Dataset
S3_POLYGON_OPTIONS_MANIFEST_DATASET = Dataset("s3://test/manifests/polygon_options_manifest_latest.txt")

@dag(
    dag_id="polygon_options_ingest_backfill",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["ingestion", "polygon", "options", "backfill"],
    params={
        "start_date": Param(default="2024-01-01", type="string", description="The start date for the backfill (YYYY-MM-DD)."),
        "end_date": Param(default="2024-01-31", type="string", description="The end date for the backfill (YYYY-MM-DD).")
    }
)
def polygon_options_ingest_backfill_dag():
    """
    This DAG backfills historical options data from the Polygon API for a specified
    date range and a list of custom underlying tickers.
    """
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")
    REQUEST_DELAY_SECONDS = 0.5

    @task(pool="api_pool")
    def get_all_option_symbols() -> list[str]:
        # This task remains the same
        api_key = os.getenv('POLYGON_OPTIONS_API_KEY')
        if not api_key:
            raise ValueError("API key POLYGON_OPTIONS_API_KEY not found in environment variables.")
        custom_tickers_path = os.path.join(DBT_PROJECT_DIR, "seeds", "custom_tickers.csv")
        with open(custom_tickers_path, mode='r') as csvfile:
            reader = csv.DictReader(csvfile)
            underlying_tickers = [row["ticker"] for row in reader]
        all_option_symbols = []
        for ticker in underlying_tickers:
            next_url = f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker={ticker}&limit=1000&apiKey={api_key}"
            while next_url:
                response = requests.get(next_url)
                response.raise_for_status()
                data = response.json()
                symbols = [contract["ticker"] for contract in data.get("results", [])]
                all_option_symbols.extend(symbols)
                next_url = data.get("next_url")
                if next_url:
                    next_url += f"&apiKey={api_key}"
                time.sleep(REQUEST_DELAY_SECONDS)
        if not all_option_symbols:
            raise AirflowSkipException("No option symbols found for the provided tickers.")
        print(f"Found {len(all_option_symbols)} total option contracts to process.")
        return all_option_symbols

    # Add a new task to batch the symbols into smaller chunks
    @task
    def batch_option_symbols(symbol_list: list[str]) -> list[list[str]]:
        """Batches the list of option symbols to avoid hitting the Airflow map limit."""
        batch_size = 500  # Process 500 symbols per parallel task
        return [symbol_list[i:i + batch_size] for i in range(0, len(symbol_list), batch_size)]

    # This task now processes a batch of symbols instead of a single one
    @task(retries=3, retry_delay=pendulum.duration(minutes=10), pool="api_pool")
    def process_symbol_batch_backfill(symbol_batch: list[str], **kwargs) -> list[str]:
        """
        Fetches the complete historical data for a BATCH of option contracts over the
        specified backfill period and creates an S3 file for each trading day.
        """
        start_date = kwargs["params"]["start_date"]
        end_date = kwargs["params"]["end_date"]
        api_key = os.getenv('POLYGON_OPTIONS_API_KEY')
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        
        all_processed_s3_keys = []
        # Loop through each symbol in the batch
        for option_symbol in symbol_batch:
            url = f"https://api.polygon.io/v2/aggs/ticker/{option_symbol}/range/1/day/{start_date}/{end_date}?adjusted=true&sort=asc&apiKey={api_key}"
            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                if data.get("resultsCount", 0) > 0 and data.get("results"):
                    for result in data["results"]:
                        trade_date = pendulum.from_timestamp(result["t"] / 1000).to_date_string()
                        formatted_result = {"ticker": data["ticker"], "resultsCount": 1, "results": [result]}
                        json_string = json.dumps(formatted_result)
                        s3_key = f"raw_data/options/{option_symbol}_{trade_date}.json"
                        s3_hook.load_string(string_data=json_string, key=s3_key, bucket_name=BUCKET_NAME, replace=True)
                        all_processed_s3_keys.append(s3_key)
                time.sleep(REQUEST_DELAY_SECONDS)
            except requests.exceptions.HTTPError as e:
                print(f"Could not process {option_symbol}. Status: {e.response.status_code}. Skipping.")
        
        return all_processed_s3_keys

    @task
    def flatten_s3_key_list(nested_list: list[list[str]]) -> list[str]:
        return [key for sublist in nested_list for key in sublist if key]

    @task(outlets=[S3_POLYGON_OPTIONS_MANIFEST_DATASET])
    def write_manifest_to_s3(s3_keys: list[str], **kwargs):
        if not s3_keys:
            raise AirflowSkipException("No S3 keys were processed during the options backfill.")
        manifest_content = "\n".join(s3_keys)
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        manifest_key = "manifests/polygon_options_manifest_latest.txt"
        s3_hook.load_string(string_data=manifest_content, key=manifest_key, bucket_name=BUCKET_NAME, replace=True)
        print(f"Options backfill manifest file created: {manifest_key}")

    # --- Task Flow ---
    all_symbols = get_all_option_symbols()
    symbol_batches = batch_option_symbols(all_symbols)
    # Expand over the list of batches, not the full list of symbols
    processed_keys_nested = process_symbol_batch_backfill.expand(symbol_batch=symbol_batches)
    s3_keys_flat = flatten_s3_key_list(processed_keys_nested)
    write_manifest_to_s3(s3_keys_flat)

polygon_options_ingest_backfill_dag()
