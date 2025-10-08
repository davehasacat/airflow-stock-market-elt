from __future__ import annotations
import pendulum
import os
import requests
import json
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
    This DAG efficiently ingests all daily stock market data from the Polygon API
    using the 'Grouped Daily' endpoint, reducing API calls from thousands to just one per day.
    """
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")

    @task(retries=3, retry_delay=pendulum.duration(minutes=10), pool="api_pool")
    def get_grouped_daily_data_and_split(**kwargs) -> list[str]:
        """
        Fetches all daily OHLCV data for all tickers for the execution date using
        a single API call to the 'Grouped Daily' endpoint. It then splits the
        results and saves each ticker's data as a separate JSON file in S3.
        """
        execution_date = kwargs["ds"]
        target_date = pendulum.parse(execution_date).subtract(days=1).to_date_string()
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        conn = BaseHook.get_connection('polygon_api')
        api_key = conn.password

        # 1. Make a single API call to the Grouped Daily endpoint
        url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{target_date}?adjusted=true&apiKey={api_key}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        processed_s3_keys = []
        if data.get('resultsCount', 0) > 0 and data.get('results'):
            # 2. Iterate through the results and create a file for each ticker
            for result in data['results']:
                ticker = result.get("T")
                # Reconstruct the JSON to match the format of the original DAG's output
                # This ensures the downstream 'load' DAG can process it without changes
                formatted_result = {
                    "ticker": ticker,
                    "queryCount": 1,
                    "resultsCount": 1,
                    "adjusted": True,
                    "results": [{
                        "v": result.get("v"),
                        "vw": result.get("vw"),
                        "o": result.get("o"),
                        "c": result.get("c"),
                        "h": result.get("h"),
                        "l": result.get("l"),
                        "t": result.get("t"),
                        "n": result.get("n"),
                    }],
                    "status": "OK",
                    "request_id": data.get("request_id")
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
    s3_keys = get_grouped_daily_data_and_split()
    write_manifest_to_s3(s3_keys)

polygon_stocks_ingest_daily_dag()
