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

# Define the Dataset that this DAG will update, triggering the load DAG
S3_POLYGON_OPTIONS_MANIFEST_DATASET = Dataset("s3://test/manifests/polygon_options_manifest_latest.txt")

@dag(
    dag_id="polygon_options_ingest_daily",
    start_date=pendulum.now(tz="UTC"),
    schedule="0 0 * * 1-5",
    catchup=True,
    tags=["ingestion", "polygon", "options", "daily"],
    dagrun_timeout=timedelta(hours=12),
    doc_md="""
    ## Polygon Options Ingest DAG

    This DAG performs the following steps:
    1.  **Get Tickers**: Fetches a list of underlying stock tickers from the data warehouse (seeded by dbt).
    2.  **Fetch and Save Options Data**: For each ticker, it:
        a. Finds all currently active or future option contracts.
        b. Fetches the daily aggregate bar (OHLCV) for each of those contracts for the execution date.
        c. Saves the raw JSON response for each contract to S3.
    3.  **Create Manifest**: Gathers the S3 keys of all newly saved files and writes them to a manifest file, which triggers the `polygon_options_load` DAG.
    """,
)
def polygon_options_ingest_daily_dag():
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_dwh")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

    @task
    def get_tickers_from_dwh() -> list[str]:
        """
        Retrieves a list of stock tickers from the sp500_tickers seed table in the DWH.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = "SELECT ticker FROM public.sp500_tickers;"
        records = pg_hook.get_records(sql)
        if not records:
            # Fallback for testing if dbt seeds haven't run
            return ["AAPL", "MSFT", "SPY"]
        return [row[0] for row in records]

    @task(pool="polygon")
    def fetch_and_save_options_data(ticker: str, trade_date: str) -> list[str]:
        """
        Fetches all option contracts for a given underlying ticker and saves their
        daily aggregate data for a specific trade date to S3.
        """
        if not POLYGON_API_KEY:
            raise ValueError("POLYGON_API_KEY environment variable not set.")

        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        saved_keys = []
        
        # 1. Get option contracts for the underlying ticker, filtering for active contracts
        contracts_url = (
            f"https://api.polygon.io/v3/reference/options/contracts"
            f"?underlying_ticker={ticker}"
            f"&expiration_date.gte={trade_date}"
            f"&limit=1000&apiKey={POLYGON_API_KEY}"
        )
        
        while contracts_url:
            try:
                response = requests.get(contracts_url)
                response.raise_for_status()
                data = response.json()
                
                contracts = data.get("results", [])
                
                for contract in contracts:
                    options_ticker = contract["ticker"]
                    
                    # 2. Get the daily bar for each contract for the specific trade_date
                    bar_url = (
                        f"https://api.polygon.io/v2/aggs/ticker/{options_ticker}/range/1/day/{trade_date}/{trade_date}"
                        f"?apiKey={POLYGON_API_KEY}"
                    )
                    bar_response = requests.get(bar_url)
                    bar_response.raise_for_status()

                    bar_data = bar_response.json()
                    if bar_data.get("resultsCount") > 0:
                        s3_key = f"raw_data/options/{options_ticker}_{trade_date}.json"
                        s3_hook.load_string(
                            string_data=json.dumps(bar_data),
                            key=s3_key,
                            bucket_name=BUCKET_NAME,
                            replace=True,
                        )
                        saved_keys.append(s3_key)

                # Handle pagination
                contracts_url = data.get("next_url")
                if contracts_url:
                    contracts_url += f"&apiKey={POLYGON_API_KEY}"

            except requests.exceptions.RequestException as e:
                print(f"Failed to fetch data for ticker {ticker}: {e}")
                # Continue to the next ticker even if one fails
                break

        print(f"Saved {len(saved_keys)} option contract files for ticker {ticker}.")
        return saved_keys

    @task(outlets=[S3_POLYGON_OPTIONS_MANIFEST_DATASET])
    def create_manifest(s3_keys_per_ticker: list):
        """
        Flattens the list of S3 keys and writes them to a manifest file in S3.
        """
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
    trade_date_str = "{{ ds }}"
    tickers_list = get_tickers_from_dwh()
    s3_keys_list = fetch_and_save_options_data.partial(trade_date=trade_date_str).expand(ticker=tickers_list)
    create_manifest(s3_keys_list)

polygon_options_ingest_daily_dag()
