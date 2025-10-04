import pandas as pd
import psycopg2
import os

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    conn = psycopg2.connect(
        host="postgres_dwh",
        database=os.environ.get("POSTGRES_DB"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        port="5432"
    )
    return conn

def load_data():
    """Loads the stock market data from the database."""
    conn = get_db_connection()
    query = "SELECT * FROM public.fct_polygon__stock_bars_performance ORDER BY trade_date ASC"
    df = pd.read_sql(query, conn)
    conn.close()
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df.set_index('trade_date', inplace=True)
    return df
