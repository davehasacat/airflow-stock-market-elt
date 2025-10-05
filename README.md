# Airflow Stock Market ELT Pipeline

This repository contains a complete, production-grade ELT pipeline for ingesting stock market data from the Polygon API, storing it in a data warehouse, and visualizing it with an interactive Plotly Dash dashboard. The pipeline is orchestrated using Apache Airflow and is designed to run locally using the Astro CLI.

## Table of Contents

* [Features](#features)
* [Tech Stack](#tech-stack)
* [Pipeline Architecture](#pipeline-architecture)
* [Demonstration](#demonstration)
* [Getting Started](#getting-started)
* [Future Work](#future-work)
* [Documentation](#documentation)

## Features

* **Scalable, High-Throughput Ingestion**: The entire pipeline is designed with modern, parallel processing patterns to efficiently handle tens of thousands of stock tickers.
* **Data-Driven, Decoupled DAGs**: The Airflow DAGs are fully decoupled and communicate through Airflow Datasets, creating a resilient, event-driven workflow.
* **Incremental Loading Strategy**: The data loading pattern uses an incremental approach to preserve historical data and significantly improve performance by only processing new or updated records on each run.
* **Historical Data Tracking with dbt Snapshots**: Leverages dbt snapshots to track changes to the raw stock data over time, creating a complete historical record of every change.
* **Robust Data Transformations**: The project uses dbt Core to create a final analytics layer with key financial indicators (e.g., moving averages, volatility metrics) that directly feed into the back-testing of trading strategies.
* **Interactive Data Visualization with Plotly Dash**: An interactive dashboard built with Plotly Dash serves as the user interface for displaying stock data and backtesting analysis.
* **Multiple Backtesting Scenarios**: The Dash application allows users to define and run backtesting scenarios for a variety of common trading strategies, including **Momentum**, **Mean Reversion**, **MACD**, and **RSI**.
* **Advanced Performance Metrics**: The backtesting results include a comprehensive set of performance metrics, such as **Max Drawdown**, **Sortino Ratio**, and **Profit Factor**.
* **Data Quality Monitoring**: A dedicated tab in the dashboard visualizes the results of `dbt` data quality tests, providing transparency into the health of the data pipeline.
* **Enhanced UI/UX**: The dashboard features loading spinners for a smooth user experience, interactive charts with volume subplots, and a tabbed layout for organized and insightful results.

## Tech Stack

* **Orchestration**: Apache Airflow
* **Data Ingestion**: Python, Polygon API
* **Data Lake**: MinIO S3
* **Data Warehouse**: PostgreSQL
* **Transformation**: dbt Core
* **Dashboarding**: Plotly Dash
* **Containerization**: Docker
* **Local Development**: Astro CLI

## Pipeline Architecture

The ELT process is orchestrated by three modular and data-driven Airflow DAGs that form a seamless, automated workflow. The architecture is designed for high throughput and scalability, capable of ingesting and processing data for thousands of stock tickers efficiently.

The DAGs are fully decoupled and communicate through **Airflow Datasets**, which are URIs that represent a piece of data. This creates a more resilient, event-driven workflow.

<img width="3520" height="5428" alt="image" src="https://github.com/user-attachments/assets/9d725b62-4b9b-476c-931d-ae4afaa464f6" />

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

The Plotly dashboard provides a comprehensive suite of tools for financial analysis. The "Backtesting" tab allows you to select from multiple trading strategies, adjust their parameters, and interactively backtest them. The results are displayed in a clean, tabbed layout with advanced performance metrics. The "Data Quality" tab provides a transparent view into the health of the underlying data, showing the results of the latest and historical dbt test runs.

-- new images here --

## Getting Started

### Prerequisites

* Docker Desktop
* Astro CLI
* A Polygon API Key _(Note: this project requires a paid API key with unlimited API calls)_

### Running the Project

1. **Configure Environment Variables**: Create a file named `.env` in the project root. Copy the contents of `.env.example` into it and fill in your POLYGON_API_KEY. The rest of the variables are pre-configured for the local environment.

2. **Build the Plotly Dash Image**: Before starting the Astro cluster, you need to build the custom Docker image for the Plotly dashboard. Run the following command from your project's root directory:

    ``` bash
    docker build -t dashboard-app ./dashboard
    ```

3. **Start the environment** with a single command from your project's root directory:

    ``` bash
    astro dev start
    ```

4. **Create the Minio Bucket**:
    * Navigate to the Minio console at [http://localhost:9001](http://localhost:9001).
    * Log in with the credentials from your `.env` file (default is `minioadmin` / `minioadmin`).
    * Click the **Create a Bucket** button, enter a bucket name (`test` is the default), and click **Create Bucket**.

5. **Run the Full Pipeline**:
    * In the Airflow UI (http://localhost:8080), un-pause and trigger the `stocks_polygon_ingest` DAG. This will kick off the entire data pipeline. The initial run will backfill all data for the current year (January 1, 2025, to the present day).
    * **Note that this first run is extensive and will take approximately 7-10 hours to complete** If you would like to run a shorter backfill for demonstration purposes, you can change the `start_date` in the `stocks_polygon_ingest.py` DAG:

    ``` python
    @dag(
    dag_id="stocks_polygon_ingest",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"), # <-- UPDATE THIS LINE
    schedule="0 0 * * 1-5",
    catchup=True,
    tags=["ingestion", "polygon"],
    )
    ```

6. **Perform an Initial Full Refresh of dbt Models**:
    * After the first run of the pipeline completes, it's a good practice to run a full refresh of your dbt models to ensure the incremental logic builds correctly on the complete historical data. Shell into the scheduler container again:

    ``` bash
    astro dev bash
    ```

    * Inside the container, run `dbt build` with the `--full-refresh` flag:

    ``` bash
    cd dbt &&
    /usr/local/airflow/dbt_venv/bin/dbt build --full-refresh
    ```

7. **View the Dashboard**: Once the pipeline has run successfully, navigate to the Plotly dashboard at [http://localhost:8501](http://localhost:8501) to view and interact with the data.

## Future Work

This project is now feature-complete and serves as a strong foundation for a robust financial data platform. Future enhancements could focus on performance, advanced analysis, and production-hardening.

* [ ] **Add Strategy Comparison**: Implement a feature to run two backtests and compare their performance metrics and portfolio values side-by-side.
* [ ] **Performance Optimization**: Explore performance improvements, such as implementing caching for data loads or using `Numba` to accelerate backtesting calculations.
* [ ] **Add Alerting**: Integrate the data quality pipeline with Airflow alerts to send an email or Slack notification when a `dbt` test fails.
* [x] ~~**Add Data Quality Monitoring**~~: Implement more advanced data quality checks and alerting (dbt tests) to monitor the health of the data pipeline and ensure the reliability of the data.
* [x] ~~**Add Advanced Performance Metrics**~~: Enhance the backtesting results with more sophisticated metrics like Max Drawdown, Sortino Ratio, and Profit Factor.

## Documentation

For more detailed information on the tools and technologies used in this project, please refer to their official documentation:

* **[Apache Airflow Documentation](https://airflow.apache.org/docs/)**
* **[Astro CLI Documentation](https://www.astronomer.io/docs/astro/cli/overview)**
* **[dbt Documentation](https://docs.getdbt.com/)**
* **[Docker Documentation](https://docs.docker.com/)**
* **[Minio Documentation](https://docs.min.io/)**
* **[PostgreSQL Documentation](https://www.postgresql.org/docs/)**
* **[Plotly Dash Documentation](https://dash.plotly.com/)**
