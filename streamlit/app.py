import streamlit as st
import pandas as pd
import psycopg2
import os
import numpy as np # Import numpy for calculations

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
    page_icon="📈",
    layout="wide",
)

# --- App Title ---
st.title("Stock Market ELT Dashboard")

# --- Data Loading ---
@st.cache_data
def load_data():
    conn = get_db_connection()
    # Ensure the data is ordered by date for calculations
    query = "SELECT * FROM public.fct_polygon__stock_bars_performance_sp500 ORDER BY trade_date ASC"
    df = pd.read_sql(query, conn)
    conn.close()
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df.set_index('trade_date', inplace=True)
    return df

# --- Technical Indicator Calculation ---
def calculate_rsi(data, window=14):
    """Calculates the Relative Strength Index (RSI)"""
    delta = data['close_price'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# --- Backtesting Engine ---
def run_backtest(df, short_window, long_window, rsi_period, rsi_overbought):
    """Runs the momentum trading strategy backtest."""

    # 1. Prepare data and signals
    signals = pd.DataFrame(index=df.index)
    signals['price'] = df['close_price']
    signals['signal'] = 0.0

    # Calculate MAs and RSI
    signals[f'ma_{short_window}'] = df['close_price'].rolling(window=short_window, min_periods=1, center=False).mean()
    signals[f'ma_{long_window}'] = df['close_price'].rolling(window=long_window, min_periods=1, center=False).mean()
    signals['rsi'] = calculate_rsi(df, rsi_period)

    # 2. Generate signals
    # Golden Cross (Buy Signal)
    signals['signal'][short_window:] = np.where(
        (signals[f'ma_{short_window}'][short_window:] > signals[f'ma_{long_window}'][short_window:]) & 
        (signals['rsi'][short_window:] < rsi_overbought), 1.0, 0.0)   

    # Generate trading orders (positions)
    signals['positions'] = signals['signal'].diff()

    # 3. Simulate portfolio
    initial_capital = float(10000.0)
    positions = pd.DataFrame(index=signals.index).fillna(0.0)
    positions['stock'] = 100 * signals['signal'] # Buy 100 shares on signal

    portfolio = positions.multiply(df['close_price'], axis=0)
    pos_diff = positions.diff()

    portfolio['holdings'] = (positions.multiply(df['close_price'], axis=0)).sum(axis=1)
    portfolio['cash'] = initial_capital - (pos_diff.multiply(df['close_price'], axis=0)).sum(axis=1).cumsum()
    portfolio['total'] = portfolio['cash'] + portfolio['holdings']
    portfolio['returns'] = portfolio['total'].pct_change()

    return portfolio, signals


try:
    df_all = load_data()

    # --- Sidebar Filters ---
    st.sidebar.header("Filters")
    tickers = df_all["ticker"].unique()

    if len(tickers) > 0:
        selected_ticker = st.sidebar.selectbox("Select Ticker", tickers)
        df_selection = df_all[df_all["ticker"] == selected_ticker].copy()

        # --- Main Page ---
        st.header(f"Displaying data for {selected_ticker}")

        # Display raw data and chart
        st.subheader("Raw Price Data")
        st.dataframe(df_selection)
        st.line_chart(df_selection['close_price'])

        # --- Backtesting Section ---
        st.sidebar.header("Momentum Strategy Backtest")

        # Get strategy parameters from user
        short_ma = st.sidebar.number_input("Short-term MA (days)", 5, 50, 20)
        long_ma = st.sidebar.number_input("Long-term MA (days)", 20, 200, 50)
        rsi_p = st.sidebar.number_input("RSI Period (days)", 7, 30, 14)
        rsi_ob = st.sidebar.number_input("RSI Overbought Threshold", 60, 90, 70)

        if st.sidebar.button("Run Backtest"):
            st.header("Backtest Results")

            # Run the backtest
            portfolio, signals = run_backtest(df_selection, short_ma, long_ma, rsi_p, rsi_ob)

            # Display portfolio value over time
            st.subheader("Portfolio Value Over Time")
            st.line_chart(portfolio['total'])

            # Display signals on price chart
            st.subheader("Trading Signals")

            # Create a new DataFrame for charting signals
            chart_df = pd.DataFrame(index=signals.index)
            chart_df['price'] = signals['price']
            chart_df[f'ma_{short_ma}'] = signals[f'ma_{short_ma}']
            chart_df[f'ma_{long_ma}'] = signals[f'ma_{long_ma}']

            # Add markers for buy and sell signals
            buy_signals = signals.loc[signals['positions'] == 1.0]
            sell_signals = signals.loc[signals['positions'] == -1.0]

            # Plot the base chart
            st.line_chart(chart_df)

            # Overlay buy and sell markers (note: Streamlit doesn't directly support scatter on line_chart, this is a conceptual representation)
            if not buy_signals.empty:
                st.write("Buy Signals (Green)")
                st.dataframe(buy_signals)
            if not sell_signals.empty:
                st.write("Sell Signals (Red)")
                st.dataframe(sell_signals)


            # --- Display Key Metrics ---
            st.subheader("Performance Metrics")
            returns = portfolio['returns'].dropna()
            sharpe_ratio = (returns.mean() / returns.std()) * np.sqrt(252) # Annualized

            col1, col2, col3 = st.columns(3)
            col1.metric("Total Return", f"{((portfolio['total'][-1] / portfolio['total'][0]) - 1) * 100:.2f}%")
            col2.metric("Sharpe Ratio", f"{sharpe_ratio:.2f}")
            col3.metric("Number of Trades", f"{int(signals['positions'].abs().sum() / 2)}")

    else:
        st.warning("No data found in the `fct_polygon__stock_bars_performance_sp500` table. Please run the Airflow pipeline to populate the data.")

except Exception as e:
    st.error(f"Failed to connect to the database and load data. Please ensure the Airflow pipeline has run successfully and the database is populated. Error: {e}")
