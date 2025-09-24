from __future__ import annotations
import pendulum
import os
import requests
import json
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable

# Define the DAG
@dag(
    dag_id="stocks_polygon_ingest",
    start_date=pendulum.datetime(2025, 9, 22, tz="UTC"),
    schedule="@daily",
    catchup=True,
    tags=["ingestion", "polygon"],
)
def stocks_polygon_ingest_dag():
    # Define S3 and Bucket connections
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")

    # Task to get all tickers, split them into batches, and save each batch to S3
    @task(pool="api_pool")
    def get_and_batch_tickers_to_s3() -> list[str]:
        conn = BaseHook.get_connection('polygon_api')
        api_key = conn.password
        if not api_key:
            raise ValueError("API key not found in Airflow connection 'polygon_api'.")
        
        all_tickers = []
        next_url = f"https://api.polygon.io/v3/reference/tickers?active=true&market=stocks&type=CS&limit=1000&apiKey={api_key}"
        
        while next_url:
            response = requests.get(next_url)
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
            batch_file_key = f"batches/tickers_batch_{i // batch_size + 1}.txt"
            s3_hook.load_string(string_data=batch_string, key=batch_file_key, bucket_name=BUCKET_NAME, replace=True)
            batch_file_keys.append(batch_file_key)
            
        return batch_file_keys

    # Task to process a batch of tickers from a file in S3
    @task(retries=3, retry_delay=pendulum.duration(minutes=5), pool="api_pool")
    def process_ticker_batch(batch_s3_key: str, **kwargs) -> list[str]:
        execution_date = kwargs["ds"]
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        
        tickers_string = s3_hook.read_key(key=batch_s3_key, bucket_name=BUCKET_NAME)
        tickers_in_batch = tickers_string.splitlines()
        
        processed_s3_keys = []
        target_date = pendulum.parse(execution_date).subtract(days=1).to_date_string()
        
        conn = BaseHook.get_connection('polygon_api')
        api_key = conn.password
        
        for ticker in tickers_in_batch:
            url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{target_date}/{target_date}?adjusted=true&sort=asc&apiKey={api_key}"
            
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            if data.get('resultsCount', 0) > 0:
                json_string = json.dumps(data)
                s3_key = f"raw_data/{ticker}_{target_date}.json"
                s3_hook.load_string(string_data=json_string, key=s3_key, bucket_name=BUCKET_NAME, replace=True)
                processed_s3_keys.append(s3_key)

        return processed_s3_keys
    # Task to flatten the list of lists from the mapped task
    @task
    def flatten_s3_key_list(nested_list: list[list[str]]) -> list[str]:
        """Takes the nested list from the mapped task and returns a single flat list."""
        return [key for sublist in nested_list for key in sublist if key]

    # --- Define the DAG's task dependencies ---
    
    batch_keys = get_and_batch_tickers_to_s3()
    processed_keys_nested = process_ticker_batch.expand(batch_s3_key=batch_keys)
    
    # Call the flattening task
    s3_keys_flat = flatten_s3_key_list(processed_keys_nested)

    # Trigger the downstream DAG, passing the clean, flat list of keys
    trigger_downstream_dag = TriggerDagRunOperator(
        task_id="trigger_load_dag",
        trigger_dag_id="stocks_polygon_load",
        conf={"s3_keys": s3_keys_flat},  # Pass the XComArg from the flatten task directly
        wait_for_completion=True, # Optional: wait for the triggered DAG to finish
    )

stocks_polygon_ingest_dag()
