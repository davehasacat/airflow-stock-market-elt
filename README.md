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

The screenshots below show a successful, end-to-end run of the entire orchestrated pipeline in the Airflow UI, demonstrating the successful execution and data-driven scheduling of all three DAGs.

<img width="1822" height="527" alt="Capture2" src="https://github.com/user-attachments/assets/415b89d8-5204-4dfb-98b9-7459e5c46c73" />

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

<img width="1818" height="999" alt="Capture" src="https://github.com/user-attachments/assets/3ae2e939-03bd-46e2-b8c9-15c15c1ec07f" />

## Using the Interactive Dashboard

The Streamlit dashboard allows you to interactively backtest a momentum trading strategy. Here's how to use it:

  1. **Select a Ticker**: Use the dropdown menu in the sidebar to choose a stock you want to analyze.
  2. **Configure the Strategy**: In the sidebar, adjust the parameters for the backtest:
       * **Moving Averages**: Set the short-term and long-term windows.
       * **RSI**: Configure the RSI period and the "overbought" threshold to filter signals.
  3. **Run the Backtest**: Click the "Run Backtest" button.
  4. **Analyze Results**: The dashboard will display the backtest results, including:
       * A chart showing your portfolio's value over time.
       * Performance metrics like Total Return and the Sharpe Ratio.

This allows you to experiment with different parameters to see how they affect the strategy's historical performance.

<img width="1791" height="905" alt="Capture2" src="https://github.com/user-attachments/assets/155cae7d-d18a-4b0c-a951-2ef32eaf0f87" />

<img width="1769" height="642" alt="Capture3" src="https://github.com/user-attachments/assets/f955d416-4f9a-4781-8704-6e80f8622b90" />

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
* [ ] **Build Out the Streamlit Dashboard**: Expand the Streamlit application to include more advanced features, such as:
  - Backtesting Scenarios: Allow users to define and run backtesting scenarios for different trading strategies.
  - Customizable Visualizations: Add more charting options and allow users to customize the displayed metrics and timeframes.
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
