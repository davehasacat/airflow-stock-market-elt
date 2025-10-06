from airflow.datasets import Dataset

# Dataset for the manifest file in S3 created by the Tradier ingest DAG
S3_TRADIER_MANIFEST_DATASET = Dataset("s3://test/manifests/tradier_manifest_latest.txt")

# Dataset for the raw DWH table updated by the Tradier load DAG
POSTGRES_DWH_TRADIER_RAW_DATASET = Dataset("postgres_dwh://public/source_tradier_stock_bars_daily")
