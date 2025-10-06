from airflow.datasets import Dataset

# Dataset for the manifest file in S3 created by ingest DAG and lists the raw data files to be processed
S3_MANIFEST_DATASET = Dataset("s3://test/manifests")

# Dataset for the raw DWH table updated by the load DAG
POSTGRES_DWH_RAW_DATASET = Dataset("postgres_dwh://public/source_polygon_stock_bars_daily")
