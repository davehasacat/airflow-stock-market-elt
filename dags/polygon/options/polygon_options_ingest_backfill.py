from __future__ import annotations
import pendulum
import os
import requests
import json
import csv
import time
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param

from airflow.datasets import Dataset

# Define a Dataset. When this DAG updates the specified S3 key, it signals
# to other DAGs that new data is available, triggering them to run.
S3_POLYGON_OPTIONS_MANIFEST_DATASET = Dataset("s3://test/manifests/polygon_options_manifest_latest.txt")

# Define the DAG with parameters for the backfill date range.
# This makes the DAG manually triggerable with custom start and end dates.
@dag(
    dag_id="polygon_options_ingest_backfill",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # This DAG is not scheduled and must be run manually.
    catchup=False,
    tags=["ingestion", "polygon", "options", "backfill"],
    # `params` allow users to provide input when manually triggering the DAG.
    params={
        "start_date": Param(default="2024-01-01", type="string", description="The start date for the backfill (YYYY-MM-DD)."),
        "end_date": Param(default="2024-01-31", type="string", description="The end date for the backfill (YYYY-MM-DD).")
    }
)
def polygon_options_ingest_backfill_dag():
    """
    ## Polygon Options Backfill DAG

    This DAG is designed for manually backfilling historical options data from Polygon.

    ### Process:
    1.  **Get All Option Symbols**: Reads a list of underlying stock tickers from a CSV file in the dbt project and fetches all associated option contract symbols from the Polygon API.
    2.  **Batch Symbols**: Groups the large list of symbols into smaller batches to avoid overwhelming Airflow's dynamic task mapping limits.
    3.  **Process Batches**: For each batch of symbols, it fetches the historical aggregate data for the user-specified date range.
    4.  **Save to S3**: Each trading day's data for each contract is saved as a separate JSON file in S3.
    5.  **Create Manifest**: A final manifest file is created in S3, listing all the new file paths. This manifest update triggers the downstream `polygon_options_load` DAG.
    """
    # --- Global Variables ---
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")
    # A small delay between API calls to respect rate limits.
    REQUEST_DELAY_SECONDS = 0.5 

    # --- Task Definitions ---

    @task(pool="api_pool")
    def get_all_option_symbols() -> list[str]:
        """
        Reads underlying tickers from a dbt seed file and fetches all associated
        option contract symbols from the Polygon API. This task paginates through
        the API results to build a complete list.
        """
        api_key = os.getenv('POLYGON_OPTIONS_API_KEY')
        if not api_key:
            raise ValueError("API key POLYGON_OPTIONS_API_KEY not found in environment variables.")

        # Read the list of target stocks from the dbt seed file.
        custom_tickers_path = os.path.join(DBT_PROJECT_DIR, "seeds", "custom_tickers.csv")
        with open(custom_tickers_path, mode='r') as csvfile:
            reader = csv.DictReader(csvfile)
            underlying_tickers = [row["ticker"] for row in reader]

        all_option_symbols = []
        print(f"Fetching option contracts for tickers: {underlying_tickers}")
        
        # Loop through each underlying ticker to find its option contracts.
        for ticker in underlying_tickers:
            next_url = f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker={ticker}&limit=1000&apiKey={api_key}"
            
            # The API is paginated, so we loop until there's no `next_url`.
            while next_url:
                response = requests.get(next_url)
                response.raise_for_status()
                data = response.json()
                
                # Extract the option ticker symbol from each contract.
                symbols = [contract["ticker"] for contract in data.get("results", [])]
                all_option_symbols.extend(symbols)
                
                next_url = data.get("next_url")
                if next_url:
                    next_url += f"&apiKey={api_key}"
                time.sleep(REQUEST_DELAY_SECONDS) # Be respectful of the API rate limit.
        
        if not all_option_symbols:
            raise AirflowSkipException("No option symbols found for the provided tickers.")
        
        print(f"Found {len(all_option_symbols)} total option contracts to process.")
        return all_option_symbols

    @task
    def batch_option_symbols(symbol_list: list[str]) -> list[list[str]]:
        """
        Batches the list of option symbols into smaller chunks. This allows the
        downstream tasks to be parallelized efficiently without hitting Airflow's
        `max_map_length` limit.
        """
        batch_size = 500  # Each parallel task will process 500 symbols.
        return [symbol_list[i:i + batch_size] for i in range(0, len(symbol_list), batch_size)]

    @task(retries=3, retry_delay=pendulum.duration(minutes=10), pool="api_pool")
    def process_symbol_batch_backfill(symbol_batch: list[str], **kwargs) -> list[str]:
        """
        Fetches historical data for a BATCH of option symbols over a date range.
        It then formats and saves one S3 file per trading day for each symbol.
        """
        # Retrieve the date range from the DAG's parameters.
        start_date = kwargs["params"]["start_date"]
        end_date = kwargs["params"]["end_date"]
        api_key = os.getenv('POLYGON_OPTIONS_API_KEY')
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        
        all_processed_s3_keys = []
        # Process each option symbol within the assigned batch.
        for option_symbol in symbol_batch:
            # This API endpoint fetches aggregate bars for a ticker over a date range.
            url = f"https://api.polygon.io/v2/aggs/ticker/{option_symbol}/range/1/day/{start_date}/{end_date}?adjusted=true&sort=asc&apiKey={api_key}"
            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()

                # If the API returns results, loop through each day's bar.
                if data.get("resultsCount", 0) > 0 and data.get("results"):
                    for result in data["results"]:
                        trade_date = pendulum.from_timestamp(result["t"] / 1000).to_date_string()
                        
                        # Re-format the data to match the structure expected by the load DAG.
                        formatted_result = {"ticker": data["ticker"], "resultsCount": 1, "results": [result]}
                        json_string = json.dumps(formatted_result)
                        
                        # Create a unique S3 key for each contract and trade date.
                        s3_key = f"raw_data/options/{option_symbol}_{trade_date}.json"
                        s3_hook.load_string(string_data=json_string, key=s3_key, bucket_name=BUCKET_NAME, replace=True)
                        all_processed_s3_keys.append(s3_key)
                
                time.sleep(REQUEST_DELAY_SECONDS) # Rate limit delay.
            except requests.exceptions.HTTPError as e:
                # Log errors but don't fail the entire batch.
                print(f"Could not process {option_symbol}. Status: {e.response.status_code}. Skipping.")
        
        return all_processed_s3_keys

    @task
    def flatten_s3_key_list(nested_list: list[list[str]]) -> list[str]:
        """
        Takes the list of lists of S3 keys generated by the parallel tasks and
        flattens it into a single list for the manifest.
        """
        return [key for sublist in nested_list for key in sublist if key]

    @task(outlets=[S3_POLYGON_OPTIONS_MANIFEST_DATASET])
    def write_manifest_to_s3(s3_keys: list[str], **kwargs):
        """
        Writes the final, flat list of S3 file paths to the manifest file in S3.
        Updating this file triggers the downstream loading DAG.
        """
        if not s3_keys:
            raise AirflowSkipException("No S3 keys were processed during the options backfill.")
        
        manifest_content = "\n".join(s3_keys)
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        manifest_key = "manifests/polygon_options_manifest_latest.txt"
        
        s3_hook.load_string(string_data=manifest_content, key=manifest_key, bucket_name=BUCKET_NAME, replace=True)
        print(f"Options backfill manifest file created with {len(s3_keys)} keys: {manifest_key}")

    # --- Task Flow Definition ---
    
    # 1. Get all symbols.
    all_symbols = get_all_option_symbols()
    
    # 2. Batch the symbols for parallel processing.
    symbol_batches = batch_option_symbols(all_symbols)
    
    # 3. Use `.expand()` to dynamically create a parallel task for each batch.
    processed_keys_nested = process_symbol_batch_backfill.expand(symbol_batch=symbol_batches)
    
    # 4. Flatten the results from the parallel tasks.
    s3_keys_flat = flatten_s3_key_list(processed_keys_nested)
    
    # 5. Write the final manifest file.
    write_manifest_to_s3(s3_keys_flat)

polygon_options_ingest_backfill_dag()
