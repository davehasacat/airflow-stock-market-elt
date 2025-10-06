from __future__ import annotations
import pendulum
import os
import requests
import json
import csv
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException

# Import the new Dataset object for Tradier data
from dags.utils.tradier_datasets import S3_TRADIER_MANIFEST_DATASET

@dag(
    dag_id="tradier_stocks_ingest",
    start_date=pendulum.datetime(2025, 10, 1, tz="UTC"),
    schedule="0 0 * * 1-5",
    catchup=True,
    tags=["ingestion", "tradier"],
)
def tradier_stocks_ingest_dag():
    """
    This DAG ingests S&P 500 stock market data from the Tradier API.
    It fetches the tickers from the sp500_tickers.csv file, validates them,
    batches them, and then ingests the daily OHLCV data for each ticker
    into MinIO S3 storage.
    """
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    BATCH_SIZE = 100
    DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")

    @task
    def get_and_batch_tickers_to_s3() -> list[str]:
        """
        Reads S&P 500 tickers from the dbt seed file, validates each ticker
        against the Tradier API, and then splits them into smaller batches.
        Each batch is saved as a text file in an S3 bucket for parallel processing.
        """
        conn = BaseHook.get_connection('tradier_api')
        api_key = conn.password
        if not api_key:
            raise ValueError("API key not found in Airflow connection 'tradier_api'.")

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json"
        }
        
        sp500_tickers_path = os.path.join(DBT_PROJECT_DIR, "seeds", "sp500_tickers.csv")
        all_tickers = []
        
        with open(sp500_tickers_path, mode='r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                ticker = row["ticker"]
                # Validate the ticker against the Tradier API
                url = f"https://api.tradier.com/v1/markets/lookup?q={ticker}"
                response = requests.get(url, headers=headers)
                
                # Check for a successful response and that the symbol exists
                if response.status_code == 200:
                    data = response.json()
                    if data.get('securities') and data['securities'] is not None and data['securities']['security']:
                        # Ensure the returned symbol is an exact match
                        securities = data['securities']['security']
                        # The response could be a list or a dict
                        if isinstance(securities, list):
                            if any(s['symbol'] == ticker for s in securities):
                                all_tickers.append(ticker)
                        elif isinstance(securities, dict):
                            if securities['symbol'] == ticker:
                                all_tickers.append(ticker)
                else:
                    print(f"Could not validate ticker {ticker}, status code: {response.status_code}")

        if not all_tickers:
            raise AirflowSkipException("No tickers were validated against the Tradier API.")

        print(f"Validated {len(all_tickers)} tickers against the Tradier API.")

        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        batch_size = BATCH_SIZE
        batch_file_keys = []
        for i in range(0, len(all_tickers), batch_size):
            batch = all_tickers[i:i + batch_size]
            batch_string = "\n".join(batch)
            batch_file_key = f"batches/tradier_tickers_batch_{i // batch_size + 1}.txt"
            s3_hook.load_string(string_data=batch_string, key=batch_file_key, bucket_name=BUCKET_NAME, replace=True)
            batch_file_keys.append(batch_file_key)

        return batch_file_keys

    @task(retries=3, retry_delay=pendulum.duration(minutes=5), pool="api_pool")
    def process_ticker_batch(batch_s3_key: str, **kwargs) -> list[str]:
        """
        Reads a batch of tickers from an S3 file, fetches the daily OHLCV data
        for each ticker for the given execution date from Tradier, and saves the
        raw JSON response to S3, while respecting API rate limits.
        """
        import time

        execution_date = kwargs["ds"]
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

        tickers_string = s3_hook.read_key(key=batch_s3_key, bucket_name=BUCKET_NAME)
        tickers_in_batch = tickers_string.splitlines()

        processed_s3_keys = []
        target_date = pendulum.parse(execution_date).subtract(days=1).to_date_string()

        conn = BaseHook.get_connection('tradier_api')
        api_key = conn.password

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json"
        }

        # Calculate delay to stay within the 120 requests/minute limit
        # 60 seconds / 120 requests = 0.5 seconds per request
        request_delay = 0.5

        for ticker in tickers_in_batch:
            url = f"https://api.tradier.com/v1/markets/history?symbol={ticker}&interval=daily&start={target_date}&end={target_date}"
            
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                data = response.json()

                if data.get('history') and data['history'].get('day'):
                    json_string = json.dumps(data)
                    s3_key = f"raw_data/tradier/{ticker}_{target_date}.json"
                    s3_hook.load_string(string_data=json_string, key=s3_key, bucket_name=BUCKET_NAME, replace=True)
                    processed_s3_keys.append(s3_key)

            except requests.exceptions.HTTPError as e:
                print(f"HTTP Error for {ticker}: {e}")
                # If a 429 (Too Many Requests) error occurs, wait and retry
                if e.response.status_code == 429:
                    print("Rate limit exceeded. Waiting for 60 seconds before retrying.")
                    time.sleep(60)
                    # Consider adding a retry mechanism here if needed
            
            # Wait for the calculated delay before the next request
            time.sleep(request_delay)


        return processed_s3_keys

    @task
    def flatten_s3_key_list(nested_list: list[list[str]]) -> list[str]:
        """
        Takes a nested list of S3 keys (from the parallel processing of batches)
        and flattens it into a single list.
        """
        return [key for sublist in nested_list for key in sublist if key]

    @task(outlets=[S3_TRADIER_MANIFEST_DATASET])
    def write_manifest_to_s3(s3_keys: list[str], **kwargs):
        """
        Overwrites the 'latest' manifest file with the list of all S3 keys
        that were created in the current DAG run. This manifest file is used
        to trigger the downstream 'load' DAG.
        """
        if not s3_keys:
            raise AirflowSkipException("No S3 keys to create a manifest for.")

        manifest_content = "\n".join(s3_keys)
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

        # Use a fixed, predictable filename for the manifest
        manifest_key = "manifests/tradier_manifest_latest.txt"
        
        s3_hook.load_string(
            string_data=manifest_content,
            key=manifest_key,
            bucket_name=BUCKET_NAME,
            replace=True
        )
        print(f"Tradier manifest file updated: {manifest_key}")


    # --- Task Flow Definition ---
    # Defines the order and dependencies of the tasks in the DAG
    batch_keys = get_and_batch_tickers_to_s3()
    processed_keys_nested = process_ticker_batch.expand(batch_s3_key=batch_keys)
    s3_keys_flat = flatten_s3_key_list(processed_keys_nested)
    write_manifest_to_s3(s3_keys_flat)

# Instantiate the DAG
tradier_stocks_ingest_dag()
