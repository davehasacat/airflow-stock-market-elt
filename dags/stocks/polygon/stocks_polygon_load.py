from __future__ import annotations
import pendulum
import json
import os
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Import the shared Dataset objects
from dags.datasets import S3_RAW_DATA_DATASET, POSTGRES_DWH_RAW_DATASET

@dag(
    dag_id="stocks_polygon_load",
    start_date=pendulum.datetime(2025, 9, 23, tz="UTC"),
    # This DAG now runs WHEN the S3 raw data is ready
    schedule=[S3_RAW_DATA_DATASET],
    catchup=False,
    tags=["load", "polygon"],
    doc_md="""
    ### Stocks Polygon Load DAG (Best Practice Schema)

    This DAG is triggered by `stocks_polygon_ingest`. It creates the target table
    with a robust, best-practice schema if it doesn't exist, then processes
    batches of S3 keys in parallel to perform an incremental load (UPSERT)
    into the Postgres data warehouse.
    """,
)
def stocks_polygon_load_dag():
    # --- DAG Configuration ---
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_dwh")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    POSTGRES_TABLE = "source_polygon_stock_bars_daily"
    BATCH_SIZE = 500

    @task
    def get_s3_keys_from_trigger(**kwargs) -> list[str]:
        """Pulls the list of S3 keys from the dag_run.conf dictionary."""
        dag_run = kwargs.get("dag_run")
        if not dag_run or not dag_run.conf:
            raise ValueError("DAG run or configuration is missing.")
            
        s3_keys = dag_run.conf.get("s3_keys")
        if not s3_keys:
            raise ValueError("No 's3_keys' found in DAG run configuration. Aborting.")
        
        print(f"Received {len(s3_keys)} S3 keys to process.")
        return s3_keys

    @task
    def batch_s3_keys(s3_keys: list[str]) -> list[list[str]]:
        """Groups the incoming list of S3 keys into smaller batches."""
        batches = [s3_keys[i:i + BATCH_SIZE] for i in range(0, len(s3_keys), BATCH_SIZE)]
        print(f"Created {len(batches)} batches of approximately {BATCH_SIZE} keys each.")
        return batches

    @task
    def transform_batch(batch_of_keys: list[str]) -> list[dict]:
        """Reads and transforms a batch of JSON files from S3."""
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        clean_records = []
        
        for s3_key in batch_of_keys:
            try:
                file_content = s3_hook.read_key(key=s3_key, bucket_name=BUCKET_NAME)
                data = json.loads(file_content)

                if data.get("resultsCount") == 1 and data.get("results"):
                    result = data["results"][0]
                    trade_date = pendulum.from_timestamp(result["t"] / 1000).to_date_string()
                    
                    clean_record = {
                        "ticker": data["ticker"],
                        "trade_date": trade_date,
                        "volume": result.get("v"),
                        "vwap": result.get("vw"),
                        "open": result.get("o"),
                        "close": result.get("c"),
                        "high": result.get("h"),
                        "low": result.get("l"),
                        "transactions": result.get("n"),
                    }
                    clean_records.append(clean_record)
            except Exception as e:
                print(f"Error processing file {s3_key}: {e}")
                continue
        return clean_records

    @task
    def flatten_results(nested_list: list[list[dict]]) -> list[dict]:
        """Takes the nested list of results and returns a single flat list."""
        return [record for batch in nested_list for record in batch if record]

    @task(outlets=[POSTGRES_DWH_RAW_DATASET])  # task now produces an update to the postgres dataset
    def load_to_postgres_incremental(clean_records: list[dict]):
        """
        Ensures the target table exists with the correct schema, then performs
        an incremental load (UPSERT).
        """
        if not clean_records:
            print("No new records to load. Skipping warehouse operation.")
            return

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # --- Best-Practice Schema Definition ---
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (
            ticker TEXT NOT NULL,
            trade_date DATE NOT NULL,
            "open" NUMERIC(19, 4),
            high NUMERIC(19, 4),
            low NUMERIC(19, 4),
            "close" NUMERIC(19, 4),
            volume BIGINT,
            vwap NUMERIC(19, 4),
            transactions BIGINT,
            inserted_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
            PRIMARY KEY (ticker, trade_date)
        );
        """
        pg_hook.run(create_table_sql)
        
        print(f"Preparing to incrementally load {len(clean_records)} records into {POSTGRES_TABLE}.")
        
        rows_to_insert = [tuple(rec.values()) for rec in clean_records]
        target_fields = list(clean_records[0].keys())
        
        # We must now also update the `inserted_at` timestamp on conflict
        update_cols = [f'"{col}" = EXCLUDED."{col}"' for col in target_fields if col not in ["ticker", "trade_date"]]
        update_cols.append('"inserted_at" = NOW()')
        
        pg_hook.insert_rows(
            table=POSTGRES_TABLE,
            rows=rows_to_insert,
            target_fields=target_fields,
            commit_every=1000,
            on_conflict="do_update",
            conflict_target=["ticker", "trade_date"],
            index_elements=update_cols
        )
        print(f"Successfully merged {len(clean_records)} records into {POSTGRES_TABLE}.")

    # --- Task Flow Definition ---
    # The get_s3_keys task needs to be adapted slightly to read from the dataset event
    @task
    def get_s3_keys_from_dataset_trigger(**kwargs) -> list[str]:
        """Pulls the list of S3 keys from the triggering dataset event."""
        # When scheduled by a dataset, the conf is populated from the trigger's `extra` param
        dag_run = kwargs.get("dag_run")
        if not dag_run or not dag_run.conf:
            raise ValueError("DAG run or configuration is missing.")
        
        s3_keys = dag_run.conf.get("s3_keys")
        if not s3_keys:
            raise ValueError("No 's3_keys' found in DAG run configuration. Aborting.")
        
        print(f"Received {len(s3_keys)} S3 keys to process from dataset trigger.")
        return s3_keys

    s3_keys = get_s3_keys_from_dataset_trigger()
    key_batches = batch_s3_keys(s3_keys)
    transformed_batches = transform_batch.expand(batch_of_keys=key_batches)
    flat_records = flatten_results(transformed_batches)
    load_to_postgres_incremental(flat_records)

stocks_polygon_load_dag()
