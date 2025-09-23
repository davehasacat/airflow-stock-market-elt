from airflow.datasets import Dataset

"""
This file defines the Dataset objects that are used to pass data dependencies
between DAGs.

A Dataset is defined by a URI-like string. It's a best practice to keep these
in a central file to avoid typos and make them easily discoverable.
"""

# This Dataset represents the raw JSON files that are landed in S3 by the
# ingest DAG. The load DAG will be scheduled to run only when this Dataset
# has been produced.
S3_RAW_DATA_DATASET = Dataset("s3://test/raw_data")
