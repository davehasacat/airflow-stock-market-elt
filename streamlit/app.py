import streamlit as st
import pandas as pd
import psycopg2
import os
import numpy as np
import plotly.graph_objects as go # Import Plotly

# --- Database Connection ---
def get_db_connection():
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
    query = "SELECT * FROM public.fct_polygon__stock_bars_performance ORDER BY trade_date ASC"
    df = pd.read_sql(query, conn)
    conn.close()
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df.set_index('trade_date', inplace=True)
    return df

# --- Technical Indicator Calculation ---
def calculate_rsi(data, window=14):
    delta = data['close_price'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# --- Backtesting Engine ---
def run_backtest(df, short_window, long_window, rsi_period, rsi_overbought):
    signals = pd.DataFrame(index=df.index)
    signals['price'] = df['close_price']
    signals['volume'] = df['volume']
    signals['signal'] = 0.0

    signals[f'ma_{short_window}'] = df['close_price'].rolling(window=short_window, min_periods=1, center=False).mean()
    signals[f'ma_{long_window}'] = df['close_price'].rolling(window=long_window, min_periods=1, center=False).mean()
    signals['rsi'] = calculate_rsi(df, rsi_period)

    signals['signal'][short_window:] = np.where(
        (signals[f'ma_{short_window}'][short_window:] > signals[f'ma_{long_window}'][short_window:]) & 
        (signals['rsi'][short_window:] < rsi_overbought), 1.0, 0.0)   

    signals['positions'] = signals['signal'].diff()

    initial_capital = float(10000.0)
    positions = pd.DataFrame(index=signals.index).fillna(0.0)
    positions['stock'] = 100 * signals['signal']

    portfolio = positions.multiply(df['close_price'], axis=0)
    pos_diff = positions.diff()

    portfolio['holdings'] = (positions.multiply(df['close_price'], axis=0)).sum(axis=1)
    portfolio['cash'] = initial_capital - (pos_diff.multiply(df['close_price'], axis=0)).sum(axis=1).cumsum()
    portfolio['total'] = portfolio['cash'] + portfolio['holdings']
    portfolio['returns'] = portfolio['total'].pct_change()

    return portfolio, signals

# --- Main App Logic ---
try:
    df_all = load_data()

    st.sidebar.header("Filters")
    tickers = df_all["ticker"].unique()

    if len(tickers) > 0:
        selected_ticker = st.sidebar.selectbox("Select Ticker", tickers)
        df_ticker = df_all[df_all["ticker"] == selected_ticker].copy()

        # --- Timeframe Selection ---
        st.sidebar.header("Timeframe Selection")
        min_date = df_ticker.index.min().date()
        max_date = df_ticker.index.max().date()
        start_date = st.sidebar.date_input("Start Date", min_date, min_value=min_date, max_value=max_date)
        end_date = st.sidebar.date_input("End Date", max_date, min_value=min_date, max_value=max_date)

        df_selection = df_ticker.loc[start_date:end_date]

        st.header(f"Displaying data for {selected_ticker}")

        # --- Candlestick Chart with Plotly ---
        st.subheader("Price Data")
        fig = go.Figure(data=[go.Candlestick(x=df_selection.index,
                        open=df_selection['open_price'],
                        high=df_selection['high_price'],
                        low=df_selection['low_price'],
                        close=df_selection['close_price'],
                        name='Price')])
        fig.update_layout(title=f'{selected_ticker} Price Action', xaxis_title='Date', yaxis_title='Price', xaxis_rangeslider_visible=False)
        st.plotly_chart(fig, use_container_width=True)


        # --- Backtesting Section ---
        st.sidebar.header("Momentum Strategy Backtest")
        short_ma = st.sidebar.number_input("Short-term MA (days)", 5, 50, 20)
        long_ma = st.sidebar.number_input("Long-term MA (days)", 20, 200, 50)
        rsi_p = st.sidebar.number_input("RSI Period (days)", 7, 30, 14)
        rsi_ob = st.sidebar.number_input("RSI Overbought Threshold", 60, 90, 70)

        # --- Customizable Metrics ---
        st.sidebar.header("Chart Options")
        available_metrics = [f'MA {short_ma}', f'MA {long_ma}', 'RSI', 'Volume']
        selected_metrics = st.sidebar.multiselect("Select metrics to display:", available_metrics, default=[f'MA {short_ma}', f'MA {long_ma}'])

        if st.sidebar.button("Run Backtest"):
            st.header("Backtest Results")

            portfolio, signals = run_backtest(df_selection, short_ma, long_ma, rsi_p, rsi_ob)

            st.subheader("Portfolio Value Over Time")
            st.line_chart(portfolio['total'])

            # --- Dynamic Trading Signals Chart ---
            st.subheader("Trading Signals & Metrics")
            signals_fig = go.Figure()
            signals_fig.add_trace(go.Scatter(x=signals.index, y=signals['price'], mode='lines', name='Price'))

            # Plot selected metrics
            if f'MA {short_ma}' in selected_metrics:
                signals_fig.add_trace(go.Scatter(x=signals.index, y=signals[f'ma_{short_ma}'], mode='lines', name=f'MA {short_ma}'))
            if f'MA {long_ma}' in selected_metrics:
                signals_fig.add_trace(go.Scatter(x=signals.index, y=signals[f'ma_{long_ma}'], mode='lines', name=f'MA {long_ma}'))

            # Add buy/sell markers
            buy_signals = signals.loc[signals['positions'] == 1.0]
            sell_signals = signals.loc[signals['positions'] == -1.0]
            signals_fig.add_trace(go.Scatter(x=buy_signals.index, y=buy_signals['price'], mode='markers', name='Buy Signal', marker=dict(color='green', size=10, symbol='triangle-up')))
            signals_fig.add_trace(go.Scatter(x=sell_signals.index, y=sell_signals['price'], mode='markers', name='Sell Signal', marker=dict(color='red', size=10, symbol='triangle-down')))

            st.plotly_chart(signals_fig, use_container_width=True)

            # Separate charts for RSI and Volume if selected
            if 'RSI' in selected_metrics:
                st.subheader("Relative Strength Index (RSI)")
                st.line_chart(signals['rsi'])
            if 'Volume' in selected_metrics:
                st.subheader("Volume")
                st.bar_chart(signals['volume'])

            st.subheader("Performance Metrics")
            returns = portfolio['returns'].dropna()
            sharpe_ratio = (returns.mean() / returns.std()) * np.sqrt(252)

            col1, col2, col3 = st.columns(3)
            col1.metric("Total Return", f"{((portfolio['total'][-1] / portfolio['total'][0]) - 1) * 100:.2f}%")
            col2.metric("Sharpe Ratio", f"{sharpe_ratio:.2f}")
            col3.metric("Number of Trades", f"{int(signals['positions'].abs().sum() / 2)}")

    else:
        st.warning("No data found. Please run the Airflow pipeline.")

except Exception as e:
    st.error(f"An error occurred: {e}")
