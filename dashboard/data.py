import pandas as pd
import psycopg2
import os
import numpy as np

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

# --- Data Loading ---
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

def calculate_bollinger_bands(data, window=20, num_std_dev=2):
    rolling_mean = data['close_price'].rolling(window=window).mean()
    rolling_std = data['close_price'].rolling(window=window).std()
    upper_band = rolling_mean + (rolling_std * num_std_dev)
    lower_band = rolling_mean - (rolling_std * num_std_dev)
    return upper_band, lower_band

# --- Backtesting Engines ---
def run_momentum_backtest(df, short_window, long_window, rsi_period, rsi_overbought):
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
    return signals

def run_mean_reversion_backtest(df, window, num_std_dev):
    signals = pd.DataFrame(index=df.index)
    signals['price'] = df['close_price']
    signals['volume'] = df['volume']
    signals['signal'] = 0.0
    signals['upper_band'], signals['lower_band'] = calculate_bollinger_bands(df, window, num_std_dev)
    signals['signal'] = np.where(signals['price'] < signals['lower_band'], 1.0, signals['signal'])
    signals['signal'] = np.where(signals['price'] > signals['upper_band'], 0.0, signals['signal'])
    signals['signal'] = signals['signal'].ffill()
    signals['positions'] = signals['signal'].diff()
    return signals

def calculate_portfolio(signals, df):
    initial_capital = float(10000.0)
    positions = pd.DataFrame(index=signals.index).fillna(0.0)
    positions['stock'] = 100 * signals['signal']
    portfolio = positions.multiply(df['close_price'], axis=0)
    pos_diff = positions.diff()
    portfolio['holdings'] = (positions.multiply(df['close_price'], axis=0)).sum(axis=1)
    portfolio['cash'] = initial_capital - (pos_diff.multiply(df['close_price'], axis=0)).sum(axis=1).cumsum()
    portfolio['total'] = portfolio['cash'] + portfolio['holdings']
    portfolio['returns'] = portfolio['total'].pct_change()
    return portfolio

# --- Load data once ---
df_all = load_data()
tickers = df_all["ticker"].unique()
