# Airflow Stocks ELT Project

This project is a complete, containerized ELT (Extract, Load, Transform) environment designed for processing stock data from the [Polygon.io](https://polygon.io/) API. It uses a modern data stack to orchestrate a highly parallelized and scalable data pipeline, manage transformations with dbt, and store data in a Postgres data warehouse, providing a robust foundation for financial analysis and backtesting.

## Pipeline Architecture

The ELT process is orchestrated by three modular and event-driven Airflow DAGs that form a seamless, automated workflow. The architecture is designed for high throughput and scalability, capable of ingesting and processing data for thousands of stock tickers efficiently.

1. **`stocks_polygon_ingest`**: This DAG fetches a complete list of all available stock tickers from the Polygon.io API. It then splits the tickers into small, manageable batches and dynamically creates parallel tasks to ingest the daily OHLCV data for each ticker, landing the raw JSON files in Minio object storage. This dynamic, batch-based approach allows the DAG to scale to any number of tickers without hitting Airflow's internal limitations.

2. **`stocks_polygon_load`**: Triggered by the completion of the ingest DAG, this DAG takes the list of newly created JSON files in Minio and, using a similar batching strategy, loads the data in parallel into a raw table in the Postgres data warehouse. This ensures that the data loading process is just as scalable as the ingestion.

3. **`stocks_polygon_dbt_transform`**: Once the raw data has been successfully loaded into the warehouse, this DAG is triggered to run the `dbt build` command. This executes all the dbt models, which transform the raw data into a clean, analytics-ready staging model (`int_polygon__stock_bars_daily`), and runs data quality tests to ensure the integrity of the transformed data.

### Proof of Success

The screenshot below shows a successful, end-to-end run of the entire orchestrated pipeline in the Airflow UI, demonstrating the successful execution of all three DAGs.

The data is successfully transformed and available for querying in the data warehouse, as shown by the following query result from the `int_polygon__stock_bars_daily` table:

---

## Tech Stack

* **Orchestration**: **Apache Airflow**
  * Used to schedule, execute, and monitor the data pipelines (DAGs).
* **Containerization**: **Docker**
  * Used to create a consistent, isolated, and reproducible environment for all services.
* **Development CLI**: **Astro CLI**
  * Used to streamline local development and testing of the Airflow environment.
* **Object Storage**: **Minio**
  * Serves as an S3-compatible object storage solution for raw data files.
* **Data Warehouse**: **Postgres**
  * Acts as the central data warehouse where both raw and transformed data is stored.
* **Transformation**: **dbt (Data Build Tool)**
  * Used to transform raw data in the warehouse into clean, reliable, and analytics-ready datasets using SQL.

---

## Getting Started

### Prerequisites

* Docker Desktop
* Astro CLI
* A Polygon API Key _(Note: this project requires a paid API key with unlimited API calls)_

### Configuration

1. **Environment Variables**: Create a file named `.env` in the project root. Copy the contents of `.env.example` (if you've created one) into it and fill in your `POLYGON_API_KEY`. The rest of the variables are pre-configured for the local environment.
2. **dbt Profile**: The `dbt/profiles.yml` file is configured to read credentials from the `.env` file. No changes are needed.

### Running the Project

1. **Start the environment** with a single command from your project's root directory:

2. **Create the Minio Bucket**:

3. **Run the Full Pipeline**:

---

## Future Work & Scalability

This project serves as a strong foundation for a robust financial data platform. The next steps for expanding this project include:

* [x] **Migrate to Polygon.io for Scalable Ingestion**: Transition from Alpha Vantage to a professional-grade API. This includes refactoring the controller DAG to dynamically fetch the entire list of available stock tickers, allowing the pipeline to automatically scale from a few tickers to thousands without code changes.
* [x] **Implement an Incremental Loading Strategy**: Evolve the data loading pattern from "truncate and load" to an incremental approach. This will preserve historical data and significantly improve performance by only processing new or updated records on each run.
* [ ] **Build Out dbt Marts Layer**: With a robust data foundation in place, the final step is to create the analytics layer. This involves building dbt models for key financial indicators (e.g., moving averages, volatility metrics) that will directly feed into back-testing trading strategies.
* [ ] **Develop a Data Visualization GUI with Streamlit**: Build an interactive dashboard using Streamlit to display the stock data and serve as the user interface for backtesting analysis. Streamlit is recommended for its speed of development, allowing for rapid prototyping of a powerful, Python-based GUI.
* [ ] **Add Data Quality Monitoring**: Implement more advanced data quality checks and alerting (dbt tests) to monitor the health of the data pipeline and ensure the reliability of the data.

## Documentation

For more detailed information on the tools and technologies used in this project, please refer to their official documentation:

* **[Apache Airflow Documentation](https://airflow.apache.org/docs/)**
* **[Astro CLI Documentation](https://www.astronomer.io/docs/astro/cli/overview)**
* **[Docker Documentation](https://docs.docker.com/)**
* **[Minio Documentation](https://docs.min.io/)**
* **[PostgreSQL Documentation](https://www.postgresql.org/docs/)**
* **[dbt Documentation](https://docs.getdbt.com/)**
