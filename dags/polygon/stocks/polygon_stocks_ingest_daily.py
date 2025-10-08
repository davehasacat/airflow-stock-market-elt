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

from dags.utils.polygon_datasets import S3_MANIFEST_DATASET

@dag(
    dag_id="polygon_stocks_ingest_daily",
    start_date=pendulum.now(tz="UTC"),
    schedule="0 0 * * 1-5",
    catchup=True,
    tags=["ingestion", "polygon", "daily"],
)
def polygon_stocks_ingest_daily_dag():
    """
    This DAG efficiently ingests daily stock market data from the Polygon API
    for a list of custom tickers using the 'Grouped Daily' endpoint.
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

    @task(retries=3, retry_delay=pendulum.duration(minutes=10), pool="api_pool")
    def get_grouped_daily_data_and_split(custom_tickers: list[str], **kwargs) -> list[str]:
        """
        Fetches all daily OHLCV data for the execution date and filters the results
        to include only the tickers from the provided custom_tickers list.
        """
        execution_date = kwargs["ds"]
        target_date = pendulum.parse(execution_date).subtract(days=1).to_date_string()
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        conn = BaseHook.get_connection('polygon_api')
        api_key = conn.password
        
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

    @task(outlets=[S3_MANIFEST_DATASET])
    def write_manifest_to_s3(s3_keys: list[str], **kwargs):
        """
        Writes the list of S3 keys to the manifest file to trigger the downstream 'load' DAG.
        """
        if not s3_keys:
            raise AirflowSkipException("No S3 keys were processed.")

        manifest_content = "\n".join(s3_keys)
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        manifest_key = "manifests/manifest_latest.txt"
        s3_hook.load_string(string_data=manifest_content, key=manifest_key, bucket_name=BUCKET_NAME, replace=True)
        print(f"Manifest file updated: {manifest_key}")

    # --- Task Flow Definition ---
    custom_tickers = get_custom_tickers()
    s3_keys = get_grouped_daily_data_and_split(custom_tickers)
    write_manifest_to_s3(s3_keys)

polygon_stocks_ingest_daily_dag()
