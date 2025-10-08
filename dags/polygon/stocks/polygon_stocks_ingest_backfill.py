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
from airflow.models.param import Param

from dags.utils.polygon_datasets import S3_MANIFEST_DATASET

@dag(
    dag_id="polygon_stocks_ingest_backfill",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["ingestion", "polygon", "backfill"],
    params={
        "start_date": Param(default="2024-01-01", type="string", description="The start date for the backfill (YYYY-MM-DD)."),
        "end_date": Param(default="2025-09-30", type="string", description="The end date for the backfill (YYYY-MM-DD).")
    }
)
def polygon_stocks_ingest_backfill_dag():
    """
    This DAG efficiently backfills historical stock market data from the Polygon API
    for a specified date range, targeting only tickers from the custom_tickers.csv file.
    """
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")

    @task
    def get_custom_tickers() -> list[str]:
        """
        Reads the list of tickers from the dbt seeds directory.
        """
        custom_tickers_path = os.path.join(DBT_PROJECT_DIR, "seeds", "custom_tickers.csv")
        tickers = []
        with open(custom_tickers_path, mode='r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                tickers.append(row["ticker"])
        return tickers

    @task
    def generate_date_range(**kwargs) -> list[str]:
        """
        Generates a list of dates between the start and end dates provided in the DAG run configuration.
        """
        start_date_str = kwargs["params"]["start_date"]
        end_date_str = kwargs["params"]["end_date"]
        
        start = pendulum.parse(start_date_str)
        end = pendulum.parse(end_date_str)
        
        trading_dates = []
        current_date = start

        while current_date <= end:
            if current_date.day_of_week not in [5, 6]:
                trading_dates.append(current_date.to_date_string())
            current_date = current_date.add(days=1)
        
        return trading_dates

    @task(retries=3, retry_delay=pendulum.duration(minutes=10), pool="api_pool")
    def process_date(target_date: str, custom_tickers: list[str]) -> list[str]:
        """
        Fetches daily OHLCV data for a single date and filters the results to include
        only the tickers from the provided custom_tickers list.
        """
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        conn = BaseHook.get_connection('polygon_api')
        api_key = conn.password
        
        # Use a set for efficient O(1) lookups
        custom_tickers_set = set(custom_tickers)

        url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{target_date}?adjusted=true&apiKey={api_key}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"No data found for {target_date} (likely a holiday). Skipping.")
                return []
            raise e

        processed_s3_keys = []
        if data.get('resultsCount', 0) > 0 and data.get('results'):
            for result in data['results']:
                ticker = result.get("T")
                # Filter for custom tickers only
                if ticker in custom_tickers_set:
                    formatted_result = {
                        "ticker": ticker, "queryCount": 1, "resultsCount": 1, "adjusted": True,
                        "results": [{"v": result.get("v"), "vw": result.get("vw"), "o": result.get("o"),
                                     "c": result.get("c"), "h": result.get("h"), "l": result.get("l"),
                                     "t": result.get("t"), "n": result.get("n")}],
                        "status": "OK", "request_id": data.get("request_id")
                    }
                    json_string = json.dumps(formatted_result)
                    s3_key = f"raw_data/{ticker}_{target_date}.json"
                    s3_hook.load_string(string_data=json_string, key=s3_key, bucket_name=BUCKET_NAME, replace=True)
                    processed_s3_keys.append(s3_key)

        return processed_s3_keys

    @task
    def flatten_s3_key_list(nested_list: list[list[str]]) -> list[str]:
        """
        Takes a nested list of S3 keys and flattens it into a single list.
        """
        return [key for sublist in nested_list for key in sublist if key]

    @task(outlets=[S3_MANIFEST_DATASET])
    def write_manifest_to_s3(s3_keys: list[str], **kwargs):
        """
        Writes the list of all created S3 keys to a manifest file.
        """
        if not s3_keys:
            raise AirflowSkipException("No S3 keys were processed during the backfill.")

        manifest_content = "\n".join(s3_keys)
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        manifest_key = "manifests/manifest_latest.txt"
        s3_hook.load_string(string_data=manifest_content, key=manifest_key, bucket_name=BUCKET_NAME, replace=True)
        print(f"Backfill manifest file created: {manifest_key}")

    # --- Task Flow Definition ---
    custom_tickers = get_custom_tickers()
    date_range = generate_date_range()
    # Pass the output of get_custom_tickers to each mapped process_date task
    processed_keys_nested = process_date.partial(custom_tickers=custom_tickers).expand(target_date=date_range)
    s3_keys_flat = flatten_s3_key_list(processed_keys_nested)
    write_manifest_to_s3(s3_keys_flat)

polygon_stocks_ingest_backfill_dag()
