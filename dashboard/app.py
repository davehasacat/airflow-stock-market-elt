import dash
from dash import dcc, html, Input, Output
import pandas as pd
import psycopg2
import os
import numpy as np
import plotly.graph_objects as go

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

# --- Main App Logic ---
df_all = load_data()
tickers = df_all["ticker"].unique()

app = dash.Dash(__name__)
server = app.server

app.layout = html.Div([
    html.H1("Stock Market ELT Dashboard"),
    html.Div([
        html.Div([
            html.Label("Select Ticker"),
            dcc.Dropdown(id='ticker-dropdown', options=[{'label': i, 'value': i} for i in tickers], value=tickers[0]),
            html.Label("Timeframe Selection"),
            dcc.DatePickerRange(
                id='date-picker-range',
                min_date_allowed=df_all.index.min().date(),
                max_date_allowed=df_all.index.max().date(),
                start_date=df_all.index.min().date(),
                end_date=df_all.index.max().date()
            ),
            html.Label("Backtesting Strategy"),
            dcc.Dropdown(id='strategy-dropdown', options=[{'label': 'Momentum', 'value': 'Momentum'}, {'label': 'Mean Reversion', 'value': 'Mean Reversion'}], value='Momentum'),
            html.Div(id='momentum-params', children=[
                html.Label("Short-term MA (days)"),
                dcc.Input(id='short-ma', type='number', value=20),
                html.Label("Long-term MA (days)"),
                dcc.Input(id='long-ma', type='number', value=50),
                html.Label("RSI Period (days)"),
                dcc.Input(id='rsi-p', type='number', value=14),
                html.Label("RSI Overbought Threshold"),
                dcc.Input(id='rsi-ob', type='number', value=70),
            ], style={'display': 'block'}),
            html.Div(id='mean-reversion-params', children=[
                html.Label("Bollinger Band Window (days)"),
                dcc.Input(id='bb-window', type='number', value=20),
                html.Label("Bollinger Band Std Dev"),
                dcc.Input(id='bb-std-dev', type='number', value=2.0, step=0.5),
            ], style={'display': 'none'}),
            html.Button('Run Backtest', id='run-backtest', n_clicks=0),
        ], style={'width': '25%', 'display': 'inline-block', 'vertical-align': 'top'}),
        html.Div(id='main-content', style={'width': '75%', 'display': 'inline-block'}),
    ])
])

@app.callback(
    Output('momentum-params', 'style'),
    Output('mean-reversion-params', 'style'),
    Input('strategy-dropdown', 'value'))
def toggle_params(strategy):
    if strategy == 'Momentum':
        return {'display': 'block'}, {'display': 'none'}
    return {'display': 'none'}, {'display': 'block'}

@app.callback(
    Output('main-content', 'children'),
    Input('run-backtest', 'n_clicks'),
    [dash.dependencies.State('ticker-dropdown', 'value'),
     dash.dependencies.State('date-picker-range', 'start_date'),
     dash.dependencies.State('date-picker-range', 'end_date'),
     dash.dependencies.State('strategy-dropdown', 'value'),
     dash.dependencies.State('short-ma', 'value'),
     dash.dependencies.State('long-ma', 'value'),
     dash.dependencies.State('rsi-p', 'value'),
     dash.dependencies.State('rsi-ob', 'value'),
     dash.dependencies.State('bb-window', 'value'),
     dash.dependencies.State('bb-std-dev', 'value')])
def update_content(n_clicks, selected_ticker, start_date, end_date, strategy,
                   short_ma, long_ma, rsi_p, rsi_ob, bb_window, bb_std_dev):
    df_ticker = df_all[df_all["ticker"] == selected_ticker].copy()
    df_selection = df_ticker.loc[start_date:end_date]

    price_chart = dcc.Graph(figure=go.Figure(data=[go.Candlestick(x=df_selection.index,
                                             open=df_selection['open_price'],
                                             high=df_selection['high_price'],
                                             low=df_selection['low_price'],
                                             close=df_selection['close_price'],
                                             name='Price')]).update_layout(title=f'{selected_ticker} Price Action',
                                                                         xaxis_title='Date', yaxis_title='Price'))

    if n_clicks > 0:
        if strategy == "Momentum":
            signals = run_momentum_backtest(df_selection, short_ma, long_ma, rsi_p, rsi_ob)
        else: # Mean Reversion
            signals = run_mean_reversion_backtest(df_selection, bb_window, bb_std_dev)

        portfolio = calculate_portfolio(signals, df_selection)
        portfolio_chart = dcc.Graph(figure=go.Figure(data=[go.Scatter(x=portfolio.index, y=portfolio['total'], mode='lines', name='Portfolio Value')]).update_layout(title='Portfolio Value Over Time'))

        signals_fig = go.Figure()
        signals_fig.add_trace(go.Scatter(x=signals.index, y=signals['price'], mode='lines', name='Price'))
        if strategy == "Momentum":
            signals_fig.add_trace(go.Scatter(x=signals.index, y=signals[f'ma_{short_ma}'], mode='lines', name=f'MA {short_ma}'))
            signals_fig.add_trace(go.Scatter(x=signals.index, y=signals[f'ma_{long_ma}'], mode='lines', name=f'MA {long_ma}'))
        else: # Mean Reversion
            signals_fig.add_trace(go.Scatter(x=signals.index, y=signals['upper_band'], mode='lines', name='Upper Band', line=dict(color='gray', dash='dash')))
            signals_fig.add_trace(go.Scatter(x=signals.index, y=signals['lower_band'], mode='lines', name='Lower Band', line=dict(color='gray', dash='dash')))

        buy_signals = signals.loc[signals['positions'] == 1.0]
        sell_signals = signals.loc[signals['positions'] == -1.0]
        signals_fig.add_trace(go.Scatter(x=buy_signals.index, y=buy_signals['price'], mode='markers', name='Buy Signal', marker=dict(color='green', size=10, symbol='triangle-up')))
        signals_fig.add_trace(go.Scatter(x=sell_signals.index, y=sell_signals['price'], mode='markers', name='Sell Signal', marker=dict(color='red', size=10, symbol='triangle-down')))
        trading_signals_chart = dcc.Graph(figure=signals_fig.update_layout(title='Trading Signals & Metrics'))

        returns = portfolio['returns'].dropna()
        sharpe_ratio = (returns.mean() / returns.std()) * np.sqrt(252) if returns.std() > 0 else 0
        total_return = f"{((portfolio['total'][-1] / portfolio['total'][0]) - 1) * 100:.2f}%"
        num_trades = f"{int(signals['positions'].abs().sum() / 2)}"

        return [
            html.H2(f"Displaying data for {selected_ticker}"),
            price_chart,
            html.H2("Backtest Results"),
            portfolio_chart,
            trading_signals_chart,
            html.H3("Performance Metrics"),
            html.P(f"Total Return: {total_return}"),
            html.P(f"Sharpe Ratio: {sharpe_ratio:.2f}"),
            html.P(f"Number of Trades: {num_trades}")
        ]

    return [html.H2(f"Displaying data for {selected_ticker}"), price_chart]

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8501, debug=True)
