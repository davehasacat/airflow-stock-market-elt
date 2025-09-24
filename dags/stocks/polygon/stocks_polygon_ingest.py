from __future__ import annotations

import json
import os
from typing import Any, Dict

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Import the shared Dataset object. This allows this DAG to act as a "producer".
from datasets import S3_RAW_DATA_DATASET


@dag(
    dag_id="stocks_polygon_ingest",
    start_date=pendulum.datetime(2025, 9, 16, tz="UTC"),
    schedule="@daily",
    catchup=True,
    tags=["ingestion", "polygon"],
)
def stocks_polygon_ingest_dag():
    """
    ### Polygon Stock Data Ingestion DAG (Updated)

    This DAG performs the first step of the ELT process:
    1.  **Get Connection Details**: Fetches the Polygon API host and key from a single Airflow connection.
    2.  **Extract (Idempotent)**: Fetches a list of all stock tickers for a specific date from the Polygon.io API.
    3.  **Batch & Fetch**: Splits tickers into batches and fetches daily OHLCV
        data for each, landing the raw JSON responses in an S3 bucket.
    4.  **Produce Dataset**: Updates an Airflow Dataset to signal that new raw
        data is ready, which triggers the `stocks_polygon_load` DAG.
    """
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")

    @task
    def get_polygon_connection_details() -> Dict[str, Any]:
        """
        Retrieves and returns the host and API key from the 'polygon_api' connection.
        """
        conn = BaseHook.get_connection('polygon_api')
        api_key = conn.password
        scheme = conn.conn_type
        host = conn.host

        if not all([api_key, scheme, host]):
            raise ValueError("API key, host, or scheme not found in Airflow connection 'polygon_api'.")

        api_host = f"{scheme}://{host}"
        
        return {"api_key": api_key, "api_host": api_host}

    @task(pool="api_pool")
    def get_and_batch_tickers_to_s3(**kwargs) -> list[str]:
        """
        Fetches the complete list of active stock tickers from Polygon.io for a
        specific date, splits them into smaller batches of 500, and uploads
        each batch as a text file to S3.
        """
        # Pull the connection details directly from the upstream task
        ti = kwargs["ti"]
        polygon_conn = ti.xcom_pull(task_ids='get_polygon_connection_details')
        api_key = polygon_conn["api_key"]
        api_host = polygon_conn["api_host"]

        execution_date = kwargs["ds"]
        all_tickers = []
        next_url = f"{api_host}/v3/reference/tickers?active=true&market=stocks&type=CS&date={execution_date}&limit=1000&apiKey={api_key}"

        while next_url:
            response = requests.get(next_url, timeout=30)
            response.raise_for_status()
            data = response.json()
            all_tickers.extend(item["ticker"] for item in data.get('results', []))
            next_url = data.get('next_url')
            if next_url:
                next_url += f"&apiKey={api_key}"

        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        batch_size = 500
        batch_file_keys = []
        for i in range(0, len(all_tickers), batch_size):
            batch = all_tickers[i:i + batch_size]
            batch_string = "\n".join(batch)
            batch_file_key = f"batches/{execution_date}/tickers_batch_{i // batch_size + 1}.txt"
            s3_hook.load_string(string_data=batch_string, key=batch_file_key, bucket_name=BUCKET_NAME, replace=True)
            batch_file_keys.append(batch_file_key)
        return batch_file_keys

    @task(retries=3, retry_delay=pendulum.duration(minutes=5), pool="api_pool")
    def process_ticker_batch(batch_s3_key: str, **kwargs) -> list[str]:
        """
        Reads a batch of tickers from an S3 file, fetches the daily OHLCV
        data for each ticker for the target date, and uploads the raw JSON
        response to S3.
        """
        # Pull the connection details directly from the upstream task
        ti = kwargs["ti"]
        polygon_conn = ti.xcom_pull(task_ids='get_polygon_connection_details')
        api_key = polygon_conn["api_key"]
        api_host = polygon_conn["api_host"]
        
        execution_date = kwargs["ds"]
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        tickers_string = s3_hook.read_key(key=batch_s3_key, bucket_name=BUCKET_NAME)
        tickers_in_batch = tickers_string.splitlines()
        processed_s3_keys = []
        for ticker in tickers_in_batch:
            target_date = pendulum.parse(execution_date).subtract(days=1).to_date_string()
            url = f"{api_host}/v2/aggs/ticker/{ticker}/range/1/day/{target_date}/{target_date}?adjusted=true&sort=asc&apiKey={api_key}"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            if data.get('resultsCount', 0) > 0:
                json_string = json.dumps(data)
                s3_key = f"raw_data/{target_date}/{ticker}.json"
                s3_hook.load_string(string_data=json_string, key=s3_key, bucket_name=BUCKET_NAME, replace=True)
                processed_s3_keys.append(s3_key)
        return processed_s3_keys

    @task
    def flatten_s3_keys(s3_keys_nested: list[list[str]]) -> list[str]:
        """
        Flattens a list of lists into a single list of S3 keys.
        """
        return [key for sublist in s3_keys_nested if sublist for key in sublist]

    @task.short_circuit
    def check_for_data(s3_keys: list[str]) -> bool:
        """
        Skips downstream tasks if no S3 files were processed.
        """
        print(f"Found {len(s3_keys)} S3 files to process.")
        return len(s3_keys) > 0

    # Task Dependencies
    polygon_connection = get_polygon_connection_details()
    batch_keys = get_and_batch_tickers_to_s3()
    
    # Explicitly set the upstream dependency so the tasks know to wait
    polygon_connection >> batch_keys

    processed_keys = process_ticker_batch.expand(batch_s3_key=batch_keys)
    s3_keys_to_load = flatten_s3_keys(s3_keys_nested=processed_keys)

    trigger_load_dag = TriggerDagRunOperator(
        task_id="trigger_stocks_polygon_load",
        trigger_dag_id="stocks_polygon_load",
        conf={"s3_keys": "{{ ti.xcom_pull(task_ids='flatten_s3_keys') }}"},
        outlets=[S3_RAW_DATA_DATASET]
    )

    s3_keys_to_load >> check_for_data(s3_keys=s3_keys_to_load) >> trigger_load_dag


stocks_polygon_ingest_dag()
