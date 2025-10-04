from dash import dcc, html

def create_layout(tickers, df_all):
    """Creates the layout for the Dash application."""
    return html.Div([
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
                dcc.Dropdown(
                    id='strategy-dropdown',
                    options=[
                        {'label': 'Momentum', 'value': 'Momentum'},
                        {'label': 'Mean Reversion', 'value': 'Mean Reversion'},
                        {'label': 'MACD', 'value': 'MACD'},
                        {'label': 'RSI', 'value': 'RSI'}
                    ],
                    value='Momentum'
                ),
                # --- Momentum Parameters ---
                html.Div(id='momentum-params', children=[
                    html.Label("Short-term MA (days)"),
                    dcc.Input(id='short-ma', type='number', value=20),
                    html.Label("Long-term MA (days)"),
                    dcc.Input(id='long-ma', type='number', value=50),
                    html.Label("RSI Period (days)"),
                    dcc.Input(id='rsi-p-momentum', type='number', value=14),
                    html.Label("RSI Overbought Threshold"),
                    dcc.Input(id='rsi-ob-momentum', type='number', value=70),
                ], style={'display': 'block'}),
                # --- Mean Reversion Parameters ---
                html.Div(id='mean-reversion-params', children=[
                    html.Label("Bollinger Band Window (days)"),
                    dcc.Input(id='bb-window', type='number', value=20),
                    html.Label("Bollinger Band Std Dev"),
                    dcc.Input(id='bb-std-dev', type='number', value=2.0, step=0.5),
                ], style={'display': 'none'}),
                # --- MACD Parameters ---
                html.Div(id='macd-params', children=[
                    html.Label("Short-term EMA (days)"),
                    dcc.Input(id='macd-short', type='number', value=12),
                    html.Label("Long-term EMA (days)"),
                    dcc.Input(id='macd-long', type='number', value=26),
                    html.Label("Signal Line EMA (days)"),
                    dcc.Input(id='macd-signal', type='number', value=9),
                ], style={'display': 'none'}),
                # --- RSI Parameters ---
                html.Div(id='rsi-params', children=[
                    html.Label("RSI Period (days)"),
                    dcc.Input(id='rsi-p-rsi', type='number', value=14),
                    html.Label("RSI Oversold Threshold"),
                    dcc.Input(id='rsi-os-rsi', type='number', value=30),
                    html.Label("RSI Overbought Threshold"),
                    dcc.Input(id='rsi-ob-rsi', type='number', value=70),
                ], style={'display': 'none'}),

                html.Button('Run Backtest', id='run-backtest', n_clicks=0),
            ], style={'width': '25%', 'display': 'inline-block', 'vertical-align': 'top'}),
            html.Div(id='main-content', style={'width': '75%', 'display': 'inline-block'}),
        ])
    ])
