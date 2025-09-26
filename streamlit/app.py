import streamlit as st
import pandas as pd
import psycopg2
import os

# --- Database Connection ---
def get_db_connection():
    # Connect to the Postgres service defined in docker-compose.override.yml
    conn = psycopg2.connect(
        host="postgres_dwh",
        database=os.environ.get("POSTGRES_DB"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        port="5432"
    )
    return conn

# --- Page Configuration ---
st.set_page_config(
    page_title="Stock Market ELT Dashboard",
    page_icon="✅",
    layout="wide",
)

# --- App Title ---
st.title("Stock Market ELT Dashboard")

# --- Data Loading ---
@st.cache_data
def load_data():
    conn = get_db_connection()
    query = "SELECT * FROM public.fct_polygon__stock_bars_performance"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

try:
    df = load_data()

    # --- Sidebar Filters ---
    st.sidebar.header("Filters")
    tickers = df["ticker"].unique()
    if len(tickers) > 0:
        selected_ticker = st.sidebar.selectbox("Select Ticker", tickers)

        # --- Filtered Data ---
        df_selection = df[df["ticker"] == selected_ticker]

        # --- Main Page ---
        st.header(f"Displaying data for {selected_ticker}")

        st.dataframe(df_selection)

        st.line_chart(df_selection.rename(columns={'trade_date':'index'}).set_index('index')['close_price'])
    else:
        st.warning("No data found in the `fct_polygon__stock_bars_performance` table. Please run the Airflow pipeline to populate the data.")

except Exception as e:
    st.error(f"Failed to connect to the database and load data. Please ensure the Airflow pipeline has run successfully and the database is populated. Error: {e}")
