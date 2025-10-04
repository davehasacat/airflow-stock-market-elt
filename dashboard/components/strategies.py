import pandas as pd
import numpy as np

def calculate_rsi(data, window=14):
    """Calculates the Relative Strength Index (RSI)."""
    delta = data['close_price'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_macd(data, short_window=12, long_window=26, signal_window=9):
    """Calculates the MACD and its signal line."""
    short_ema = data['close_price'].ewm(span=short_window, adjust=False).mean()
    long_ema = data['close_price'].ewm(span=long_window, adjust=False).mean()
    macd_line = short_ema - long_ema
    signal_line = macd_line.ewm(span=signal_window, adjust=False).mean()
    return macd_line, signal_line

def calculate_bollinger_bands(data, window=20, num_std_dev=2):
    """Calculates the Bollinger Bands."""
    rolling_mean = data['close_price'].rolling(window=window).mean()
    rolling_std = data['close_price'].rolling(window=window).std()
    upper_band = rolling_mean + (rolling_std * num_std_dev)
    lower_band = rolling_mean - (rolling_std * num_std_dev)
    return upper_band, lower_band

def run_momentum_backtest(df, short_window, long_window, rsi_period, rsi_overbought):
    """Runs a momentum-based backtesting strategy."""
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
    """Runs a mean-reversion backtesting strategy."""
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

def run_macd_backtest(df, short_window, long_window, signal_window):
    """Runs a MACD-based backtesting strategy."""
    signals = pd.DataFrame(index=df.index)
    signals['price'] = df['close_price']
    signals['volume'] = df['volume']
    signals['signal'] = 0.0
    signals['macd'], signals['signal_line'] = calculate_macd(df, short_window, long_window, signal_window)
    # Generate buy signal when MACD crosses above signal line
    signals['signal'] = np.where(signals['macd'] > signals['signal_line'], 1.0, 0.0)
    signals['positions'] = signals['signal'].diff()
    return signals

def run_rsi_backtest(df, rsi_period, rsi_oversold, rsi_overbought):
    """Runs an RSI-based backtesting strategy."""
    signals = pd.DataFrame(index=df.index)
    signals['price'] = df['close_price']
    signals['volume'] = df['volume']
    signals['signal'] = 0.0
    signals['rsi'] = calculate_rsi(df, rsi_period)
    # Buy when RSI crosses above the oversold threshold
    signals['signal'] = np.where(signals['rsi'] > rsi_oversold, 1.0, 0.0)
    # Sell when RSI crosses below the overbought threshold
    signals['signal'] = np.where(signals['rsi'] < rsi_overbought, 0.0, signals['signal'])
    signals['positions'] = signals['signal'].diff()
    return signals

def calculate_portfolio(signals, df):
    """Calculates the portfolio value over time."""
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
