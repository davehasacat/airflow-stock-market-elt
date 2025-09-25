from airflow.datasets import Dataset

# Dataset for raw data landed in S3
S3_RAW_DATA_DATASET = Dataset("s3://test/raw_data")

# Dataset for the raw DWH table updated by the load DAG
POSTGRES_DWH_RAW_DATASET = Dataset("postgres_dwh://public/source_polygon_stock_bars_daily")
