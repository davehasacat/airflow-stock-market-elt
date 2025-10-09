from __future__ import annotations
import pendulum
import os
import requests
import json
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.datasets import Dataset
from airflow.exceptions import AirflowSkipException

# Define a Dataset. This acts as a signal to other DAGs. When this DAG
# successfully completes and updates the manifest file, any DAGs scheduled
# to run after this Dataset is updated will be triggered.
S3_POLYGON_OPTIONS_MANIFEST_DATASET = Dataset("s3://test/manifests/polygon_options_manifest_latest.txt")

# Define the DAG. This DAG runs daily on weekdays to ingest options data.
@dag(
    dag_id="polygon_options_ingest_daily",
    # Set a dynamic start_date to run for the current day.
    start_date=pendulum.now(tz="UTC"),
    # Schedule to run at midnight UTC, Monday through Friday.
    schedule="0 0 * * 1-5",
    # `catchup=True` means the DAG will run for any past, un-run schedules.
    catchup=True,
    tags=["ingestion", "polygon", "options", "daily"],
    dagrun_timeout=timedelta(hours=12),
    # `doc_md` provides detailed documentation visible in the Airflow UI.
    doc_md="""
    ## Polygon Options Daily Ingest DAG

    This DAG orchestrates the daily ingestion of options data from the Polygon API.

    ### Process:
    1.  **Get Tickers**: Fetches a list of underlying stock tickers (e.g., AAPL, SPY) from a pre-defined dbt seed table in the data warehouse.
    2.  **Fetch and Save Options Data**: For each ticker, it performs two main API calls in a loop:
        a.  **Find Contracts**: It fetches all option contracts for the ticker that are either currently active or will be in the future. This is done by querying the `/v3/reference/options/contracts` endpoint.
        b.  **Fetch Daily Bar**: For each contract found, it fetches the daily aggregate bar (OHLCV) for the DAG's execution date using the `/v2/aggs/ticker/...` endpoint.
        c.  **Save to S3**: The raw JSON response for each contract's daily bar is saved to a unique file in the S3 bucket.
    3.  **Create Manifest**: After all tickers have been processed, it gathers the S3 keys of all the newly saved files and writes them into a single manifest file in S3. This manifest update triggers the downstream `polygon_options_load` DAG via the `S3_POLYGON_OPTIONS_MANIFEST_DATASET`.
    """,
)
def polygon_options_ingest_daily_dag():
    # --- Global Variables ---
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_dwh")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

    # --- Task Definitions ---

    @task
    def get_tickers_from_dwh() -> list[str]:
        """
        Retrieves a list of stock tickers from the `sp500_tickers` seed table,
        which is expected to be maintained by a separate dbt process. This
        determines which underlyings to fetch options data for.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = "SELECT ticker FROM public.sp500_tickers;"
        records = pg_hook.get_records(sql)
        
        # If the dbt seed table is empty, use a fallback list for testing.
        if not records:
            print("No tickers found in DWH, using fallback list.")
            return ["AAPL", "MSFT", "SPY"]
            
        tickers = [row[0] for row in records]
        print(f"Retrieved {len(tickers)} tickers from DWH.")
        return tickers

    @task(pool="polygon") # Uses a custom pool to limit concurrent API calls.
    def fetch_and_save_options_data(ticker: str, trade_date: str) -> list[str]:
        """
        For a single underlying ticker, fetches all relevant option contracts
        and saves their daily aggregate data for a specific trade date to S3.
        """
        if not POLYGON_API_KEY:
            raise ValueError("POLYGON_API_KEY environment variable not set.")

        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        saved_keys = []
        
        # API endpoint to get all option contracts for the underlying ticker.
        # We filter for contracts that expire on or after the current trade date.
        contracts_url = (
            f"https://api.polygon.io/v3/reference/options/contracts"
            f"?underlying_ticker={ticker}"
            f"&expiration_date.gte={trade_date}"
            f"&limit=1000&apiKey={POLYGON_API_KEY}"
        )
        
        # Loop to handle API pagination for contracts.
        while contracts_url:
            try:
                response = requests.get(contracts_url)
                response.raise_for_status()
                data = response.json()
                
                contracts = data.get("results", [])
                
                # For each contract, fetch its daily bar data.
                for contract in contracts:
                    options_ticker = contract["ticker"]
                    
                    bar_url = (
                        f"https://api.polygon.io/v2/aggs/ticker/{options_ticker}/range/1/day/{trade_date}/{trade_date}"
                        f"?apiKey={POLYGON_API_KEY}"
                    )
                    bar_response = requests.get(bar_url)
                    bar_response.raise_for_status()

                    bar_data = bar_response.json()
                    # Only save the file if the API returned actual trade data.
                    if bar_data.get("resultsCount") > 0:
                        s3_key = f"raw_data/options/{options_ticker}_{trade_date}.json"
                        s3_hook.load_string(
                            string_data=json.dumps(bar_data),
                            key=s3_key,
                            bucket_name=BUCKET_NAME,
                            replace=True,
                        )
                        saved_keys.append(s3_key)

                # Get the URL for the next page of results. If it's null, the loop ends.
                contracts_url = data.get("next_url")
                if contracts_url:
                    contracts_url += f"&apiKey={POLYGON_API_KEY}"

            except requests.exceptions.RequestException as e:
                print(f"Failed to fetch data for ticker {ticker}: {e}")
                break # Exit loop for this ticker if an API error occurs.

        print(f"Saved {len(saved_keys)} option contract files for ticker {ticker}.")
        return saved_keys

    @task(outlets=[S3_POLYGON_OPTIONS_MANIFEST_DATASET])
    def create_manifest(s3_keys_per_ticker: list):
        """
        Flattens the list of S3 keys from all parallel tasks and writes them to a
        single manifest file in S3. This signals the completion of the ingest process.
        """
        # The input is a list of lists; this flattens it into a single list.
        flat_list = [key for sublist in s3_keys_per_ticker for key in sublist if key]
        
        if not flat_list:
            raise AirflowSkipException("No new files were created; skipping manifest generation.")
        
        manifest_content = "\n".join(flat_list)
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        manifest_key = "manifests/polygon_options_manifest_latest.txt"

        s3_hook.load_string(
            string_data=manifest_content,
            key=manifest_key,
            bucket_name=BUCKET_NAME,
            replace=True,
        )
        print(f"Manifest created with {len(flat_list)} keys at s3://{BUCKET_NAME}/{manifest_key}")

    # --- Task Flow Definition ---

    # `{{ ds }}` is an Airflow template that resolves to the DAG's logical execution date (YYYY-MM-DD).
    trade_date_str = "{{ ds }}"
    
    # 1. Fetch the list of tickers.
    tickers_list = get_tickers_from_dwh()
    
    # 2. Dynamically map the `fetch_and_save_options_data` task over the list of tickers.
    # A parallel task will be created for each ticker.
    s3_keys_list = fetch_and_save_options_data.partial(trade_date=trade_date_str).expand(ticker=tickers_list)
    
    # 3. Once all parallel tasks are complete, run `create_manifest` with their collected results.
    create_manifest(s3_keys_list)

polygon_options_ingest_daily_dag()
