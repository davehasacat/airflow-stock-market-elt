# Airflow Stock Market ELT Pipeline

This repository contains a complete, production-grade ELT pipeline for ingesting stock market data from the Polygon API, storing it in a data warehouse, and visualizing it with an interactive Streamlit dashboard. The pipeline is orchestrated using Apache Airflow and is designed to run locally using the Astro CLI.

## Table of Contents

* Features
* Tech Stack
* Pipeline Architecture
* Demonstration
* Getting Started
* Future Work
* Documentation

## Features

* **Scalable, High-Throughput Ingestion**: The entire pipeline is designed with modern, parallel processing patterns to efficiently handle tens of thousands of stock tickers.
* **Data-Driven, Decoupled DAGs**: The Airflow DAGs are fully decoupled and communicate through Airflow Datasets, creating a resilient, event-driven workflow.
* **Incremental Loading Strategy**: The data loading pattern uses an incremental approach to preserve historical data and significantly improve performance by only processing new or updated records on each run.
* **Robust Data Transformations**: The project uses dbt Core to create a final analytics layer with key financial indicators (e.g., moving averages, volatility metrics) that directly feed into the back-testing of trading strategies.
* **Interactive Data Visualization with Streamlit**: An interactive dashboard built with Streamlit serves as the user interface for displaying stock data and backtesting analysis.
* **Backtesting Scenarios**: The Streamlit application allows users to define and run backtesting scenarios for different trading strategies.

## Tech Stack

* **Orchestration**: Apache Airflow
* **Data Ingestion**: Python, Polygon API
* **Data Lake**: MinIO S3
* **Data Warehouse**: PostgreSQL
* **Transformation**: dbt Core
* **Dashboarding**: Streamlit
* **Containerization**: Docker
* **Local Development**: Astro CLI

## Pipeline Architecture

The ELT process is orchestrated by three modular and data-driven Airflow DAGs that form a seamless, automated workflow. The architecture is designed for high throughput and scalability, capable of ingesting and processing data for thousands of stock tickers efficiently.

The DAGs are fully decoupled and communicate through **Airflow Datasets**, which are URIs that represent a piece of data. This creates a more resilient, event-driven workflow.

<img width="5468" height="4808" alt="image" src="https://github.com/user-attachments/assets/947aba94-57f9-49bf-80d0-980b9c60ca5f" />

1. **`stocks_polygon_ingest`**: This DAG fetches a complete list of all available stock tickers from the Polygon.io API. It then splits the tickers into small, manageable batches and dynamically creates parallel tasks to ingest the daily OHLCV data for each ticker, landing the raw JSON files in Minio object storage. Upon completion, it writes a list of all created file keys to a manifest file and **produces to an S3 Dataset** (`s3://test/manifests`).

2. **`stocks_polygon_load`**: This DAG is scheduled to run only when the S3 manifest Dataset is updated. It reads the list of newly created JSON files from the manifest and, using a similar batching strategy, loads the data in parallel into a raw table in the Postgres data warehouse. This ensures that the data loading process is just as scalable as the ingestion. When the load is successful, it produces to a Postgres Dataset (`postgres_dwh://public/source_polygon_stock_bars_daily`).

3. **`stocks_polygon_dbt_transform`**: When the `load` DAG successfully updates the raw table, it produces the corresponding Dataset that triggers the final `transform` DAG. This DAG runs `dbt build` to execute all dbt models, which transforms the raw data into:
    * A clean, casted staging model (`stg_polygon__stock_bars_casted`).
    * An enriched intermediate model with technical indicators (`int_polygon__stock_bars_enriched`).
    * A final, analytics-ready facts table (`fct_polygon__stock_bars_performance`).

    It also runs data quality tests to ensure the integrity of the transformed data.

## Demonstration

### Airflow Orchestration

The screenshots below show a successful, end-to-end run of the entire orchestrated pipeline in the Airflow UI, including a complete backfill for all trading days in 2025. This demonstrates the successful execution and data-driven scheduling of all three DAGs.

<img width="1828" height="510" alt="Capture" src="https://github.com/user-attachments/assets/b06cc529-7587-4560-8e92-b1967c6e2406" />

<img width="1814" height="457" alt="Capture2" src="https://github.com/user-attachments/assets/0fc23702-fa80-4ef4-a27b-07e991aba98a" />

### Interactive Dashboard

The Streamlit dashboard allows you to interactively backtest a momentum trading strategy. The dashboard will display the results, including an interactive chart showing the buy/sell signals on the price line, your portfolio's value over time, and key performance metrics like Total Return and Sharpe Ratio.

<img width="1799" height="718" alt="Capture4" src="https://github.com/user-attachments/assets/566e9e7f-cabf-4afa-ab14-38a98bab94d0" />

<img width="1809" height="996" alt="Capture5" src="https://github.com/user-attachments/assets/64bf2e83-cf31-4263-8a8b-6493668b66b5" />

## Getting Started

### Prerequisites

* Docker Desktop
* Astro CLI
* A Polygon API Key _(Note: this project requires a paid API key with unlimited API calls)_

### Running the Project

1. **Configure Environment Variables**: Create a file named `.env` in the project root. Copy the contents of `.env.example` into it and fill in your POLYGON_API_KEY. The rest of the variables are pre-configured for the local environment.

2. **Build the Streamlit Image**: Before starting the Astro cluster, you need to build the custom Docker image for the Streamlit dashboard. Run the following command from your project's root directory:

    ``` bash
    docker build -t streamlit-app ./streamlit
    ```

3. **Start the environment** with a single command from your project's root directory:

    ``` bash
    astro dev start
    ```

4. **Create the Minio Bucket**:
    * Navigate to the Minio console at [http://localhost:9001](http://localhost:9001).
    * Log in with the credentials from your `.env` file (default is `minioadmin` / `minioadmin`).
    * Click the **Create a Bucket** button, enter a bucket name (`test` is the default), and click **Create Bucket**.

5. **Run the Full Pipeline**: In the Airflow UI (http://localhost:8080), un-pause and trigger the `stocks_polygon_ingest` DAG. This will kick off the entire data pipeline. The initial run will backfill all data for the current year (January 1, 2025, to the present day). **Note that this first run is extensive and will take approximately 7-10 hours to complete**.
If you would like to run a shorter backfill for demonstration purposes, you can change the `start_date` in the `stocks_polygon_ingest.py` DAG:

    ``` python
    @dag(
    dag_id="stocks_polygon_ingest",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"), # <-- UPDATE THIS LINE
    schedule="0 0 * * 1-5",
    catchup=True,
    tags=["ingestion", "polygon"],
    )
    ```

6. **View the Dashboard**:Once the pipeline has run successfully, navigate to the Streamlit dashboard at [http://localhost:8501](http://localhost:8501) to view and interact with the data.

## Future Work

This project serves as a strong foundation for a robust financial data platform. The next steps for expanding this project include:

* [ ] **Add Data Quality Monitoring**: Implement more advanced data quality checks and alerting (dbt tests) to monitor the health of the data pipeline and ensure the reliability of the data.

## Documentation

For more detailed information on the tools and technologies used in this project, please refer to their official documentation:

* **[Apache Airflow Documentation](https://airflow.apache.org/docs/)**
* **[Astro CLI Documentation](https://www.astronomer.io/docs/astro/cli/overview)**
* **[dbt Documentation](https://docs.getdbt.com/)**
* **[Docker Documentation](https://docs.docker.com/)**
* **[Minio Documentation](https://docs.min.io/)**
* **[PostgreSQL Documentation](https://www.postgresql.org/docs/)**
* **[Streamlit Documentation](https://docs.streamlit.io/)**
