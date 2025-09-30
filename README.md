# Airflow Stock Market ELT Pipeline

This repository contains a complete ELT pipeline for ingesting stock market data from the Polygon API, storing it in a data warehouse, and visualizing it with an interactive Streamlit dashboard. The pipeline is orchestrated using Apache Airflow and is designed to run on a local development environment using the Astro CLI.

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

## Proof of Success

### Airflow Orchestration

The screenshots below show a successful, end-to-end run of the entire orchestrated pipeline in the Airflow UI, including a complete backfill for all trading days in 2025. This demonstrates the successful execution and data-driven scheduling of all three DAGs.

<img width="1828" height="510" alt="Capture" src="https://github.com/user-attachments/assets/b06cc529-7587-4560-8e92-b1967c6e2406" />

<img width="1814" height="457" alt="Capture2" src="https://github.com/user-attachments/assets/0fc23702-fa80-4ef4-a27b-07e991aba98a" />

### Data Transformation

The data is successfully transformed through staging, intermediate, and marts layers and is available for querying in the data warehouse. The final `fct_polygon__stock_bars_performance` table provides a clean, analytics-ready dataset with calculated metrics, as shown by the following query result:

``` sql
SELECT * FROM
    public.fct_polygon__stock_bars_performance
WHERE
    ticker = 'GOOGL'
LIMIT 5;
```

<img width="1798" height="202" alt="Capture5" src="https://github.com/user-attachments/assets/0b192cc7-71a2-4ac6-9f62-4ad775494a90" />

### Interactive Dashboard

The tech stack has been expanded to include a Streamlit application. This provides an interactive, user-friendly dashboard for visualizing and analyzing the ingested stock data, offering a clear and tangible view of the pipeline's output.

<img width="1799" height="718" alt="Capture4" src="https://github.com/user-attachments/assets/566e9e7f-cabf-4afa-ab14-38a98bab94d0" />

## Using the Interactive Dashboard

The Streamlit dashboard allows you to interactively backtest a momentum trading strategy. Here's how to use it:

  1. **Select Ticker and Timeframe**:
       * Use the "**Select Ticker**" dropdown in the sidebar to choose a stock.
       * Use the "**Start Date**" and "**End Date**" selectors to focus on a specific period. The dashboard will display an interactive candlestick chart for the selected range.
  2. **Configure the Strategy**: In the sidebar, adjust the parameters for the backtest:
       * In the "**Momentum Strategy Backtest**" section, adjust the parameters for the moving averages and RSI to define your trading logic.
  3. **Customize Chart Options**:
       * Use the "**Select metrics to display**" multi-select box to choose which indicators (Moving Averages, RSI, Volume) you want to overlay on the charts.
  4. **Run the Backtest and Analyze Results**:
       * Click the "**Run Backtest**" button.
       * The dashboard will display the results, including an interactive chart showing the buy/sell signals on the price line, your portfolio's value over time, and key performance metrics like Total Return and Sharpe Ratio.

<img width="1809" height="996" alt="Capture5" src="https://github.com/user-attachments/assets/64bf2e83-cf31-4263-8a8b-6493668b66b5" />

---

## Tech Stack

* **Orchestration**: Apache Airflow
* **Data Ingestion**: Python, Polygon API
* **Data Lake**: MinIO S3
* **Data Warehouse**: PostgreSQL
* **Transformation**: dbt Core
* **Dashboarding**: Streamlit
* **Containerization**: Docker
* **Local Development**: Astro CLI

---

## Getting Started

### Prerequisites

* Docker Desktop
* Astro CLI
* A Polygon API Key _(Note: this project requires a paid API key with unlimited API calls)_

### Configuration

1. **Environment Variables**: Create a file named `.env` in the project root. Copy the contents of `.env.example` into it and fill in your `POLYGON_API_KEY`. The rest of the variables are pre-configured for the local environment.
2. **dbt Profile**: The `dbt/profiles.yml` file is configured to read credentials from the `.env` file. No changes are needed.

### Running the Project

1. **Build the Streamlit Image**: Before starting the Astro cluster, you need to build the custom Docker image for the Streamlit dashboard. Run the following command from your project's root directory:

    ``` bash
    docker build -t streamlit-app ./streamlit
    ```

2. **Start the environment** with a single command from your project's root directory:

    ``` bash
    astro dev start
    ```

3. **Create the Minio Bucket via the UI**:
    * Navigate to the Minio console at [http://localhost:9001](http://localhost:9001).
    * Log in with the credentials from your `.env` file (default is `minioadmin` / `minioadmin`).
    * Click the **Create a Bucket** button, enter a bucket name (`test` is the default), and click **Create Bucket**.

4. **Run the Full Pipeline**: In the Airflow UI (http://localhost:8080), un-pause and trigger the `stocks_polygon_ingest` DAG. This will kick off the entire data pipeline.

---

## Future Work & Scalability

This project serves as a strong foundation for a robust financial data platform. The next steps for expanding this project include:

* [x] **Implement an Incremental Loading Strategy**: Evolve the data loading pattern from "truncate and load" to an incremental approach. This will preserve historical data and significantly improve performance by only processing new or updated records on each run.
* [x] **Build Out dbt Marts Layer**: With a robust data foundation in place, the final step is to create the analytics layer. This involves building dbt models for key financial indicators (e.g., moving averages, volatility metrics) that will directly feed into back-testing trading strategies.
* [x] **Develop a Data Visualization GUI with Streamlit**: Build an interactive dashboard using Streamlit to display the stock data and serve as the user interface for backtesting analysis. Streamlit is recommended for its speed of development, allowing for rapid prototyping of a powerful, Python-based GUI.
* [x] **Build Out the Streamlit Dashboard**: Expand the Streamlit application to include more advanced features, such as:
  * Backtesting Scenarios: Allow users to define and run backtesting scenarios for different trading strategies.
  * Customizable Visualizations: Add more charting options and allow users to customize the displayed metrics and timeframes.
* [ ] **Add Data Quality Monitoring**: Implement more advanced data quality checks and alerting (dbt tests) to monitor the health of the data pipeline and ensure the reliability of the data.

## Documentation

For more detailed information on the tools and technologies used in this project, please refer to their official documentation:

* **[Apache Airflow Documentation](https://airflow.apache.org/docs/)**
* **[Astro CLI Documentation](https://www.astronomer.io/docs/astro/cli/overview)**
* **[Docker Documentation](https://docs.docker.com/)**
* **[Minio Documentation](https://docs.min.io/)**
* **[PostgreSQL Documentation](https://www.postgresql.org/docs/)**
* **[Streamlit Documentation](https://docs.streamlit.io/)**
* **[dbt Documentation](https://docs.getdbt.com/)**
