from dash import dcc, html
import dash_bootstrap_components as dbc
from .data import tickers, df_all # Use relative import

layout = dbc.Container([
    dbc.Row(dbc.Col(html.H1("Stock Market ELT Dashboard"), width=12)),
    html.Hr(),
    dbc.Row([
        # Sidebar
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Controls", className="card-title"),
                    html.Label("Select Ticker"),
                    dcc.Dropdown(id='ticker-dropdown', options=[{'label': i, 'value': i} for i in tickers], value=tickers[0]),
                    html.Br(),
                    html.Label("Timeframe Selection"),
                    dcc.DatePickerRange(
                        id='date-picker-range',
                        min_date_allowed=df_all.index.min().date(),
                        max_date_allowed=df_all.index.max().date(),
                        start_date=df_all.index.min().date(),
                        end_date=df_all.index.max().date(),
                        className="d-block"
                    ),
                    html.Br(),
                    html.Label("Backtesting Strategy"),
                    dcc.Dropdown(id='strategy-dropdown', options=[{'label': 'Momentum', 'value': 'Momentum'}, {'label': 'Mean Reversion', 'value': 'Mean Reversion'}], value='Momentum'),
                    html.Br(),
                    # Momentum Parameters
                    html.Div(id='momentum-params', children=[
                        dbc.Label("Short-term MA (days)"),
                        dbc.Input(id='short-ma', type='number', value=20),
                        dbc.Label("Long-term MA (days)"),
                        dbc.Input(id='long-ma', type='number', value=50),
                        dbc.Label("RSI Period (days)"),
                        dbc.Input(id='rsi-p', type='number', value=14),
                        dbc.Label("RSI Overbought Threshold"),
                        dbc.Input(id='rsi-ob', type='number', value=70),
                    ], style={'display': 'block'}),
                    # Mean Reversion Parameters
                    html.Div(id='mean-reversion-params', children=[
                        dbc.Label("Bollinger Band Window (days)"),
                        dbc.Input(id='bb-window', type='number', value=20),
                        dbc.Label("Bollinger Band Std Dev"),
                        dbc.Input(id='bb-std-dev', type='number', value=2.0, step=0.5),
                    ], style={'display': 'none'}),
                    html.Br(),
                    dbc.Button('Run Backtest', id='run-backtest', n_clicks=0, color="primary", className="d-grid gap-2 col-6 mx-auto"),
                ])
            ])
        ], width=3),
        # Main Content
        dbc.Col([
            dcc.Loading(id="loading-spinner", type="circle", children=html.Div(id='main-content'))
        ], width=9)
    ])
], fluid=True)
