from dash import dcc, html, Input, Output, State
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import pandas as pd
from . import strategies
from .data import get_db_connection

def register_callbacks(app, df_all):
    """Registers all callbacks for the Dash application."""

    # --- Main Callback to Render Tab Content ---
    @app.callback(
        Output('tabs-content-container', 'children'),
        Input('app-tabs', 'value')
    )
    def render_tab_content(tab):
        if tab == 'tab-backtesting':
            tickers = df_all["ticker"].unique()
            return create_backtesting_layout(tickers, df_all)
        elif tab == 'tab-data-quality':
            return create_data_quality_layout()

    # --- Backtesting Callbacks ---
    @app.callback(
        Output('momentum-params', 'style'),
        Output('mean-reversion-params', 'style'),
        Output('macd-params', 'style'),
        Output('rsi-params', 'style'),
        Input('strategy-dropdown', 'value'),
        prevent_initial_call=True
    )
    def toggle_params(strategy):
        momentum_style = {'display': 'block'} if strategy == 'Momentum' else {'display': 'none'}
        mean_reversion_style = {'display': 'block'} if strategy == 'Mean Reversion' else {'display': 'none'}
        macd_style = {'display': 'block'} if strategy == 'MACD' else {'display': 'none'}
        rsi_style = {'display': 'block'} if strategy == 'RSI' else {'display': 'none'}
        return momentum_style, mean_reversion_style, macd_style, rsi_style

    @app.callback(
        Output('main-content', 'children'),
        Input('run-backtest', 'n_clicks'),
        [State('ticker-dropdown', 'value'),
         State('date-picker-range', 'start_date'),
         State('date-picker-range', 'end_date'),
         State('strategy-dropdown', 'value'),
         State('short-ma', 'value'), State('long-ma', 'value'),
         State('rsi-p-momentum', 'value'), State('rsi-ob-momentum', 'value'),
         State('bb-window', 'value'), State('bb-std-dev', 'value'),
         State('macd-short', 'value'), State('macd-long', 'value'), State('macd-signal', 'value'),
         State('rsi-p-rsi', 'value'), State('rsi-os-rsi', 'value'), State('rsi-ob-rsi', 'value')],
        prevent_initial_call=True
    )
    def update_backtesting_content(n_clicks, selected_ticker, start_date, end_date, strategy,
                                   short_ma, long_ma, rsi_p_momentum, rsi_ob_momentum,
                                   bb_window, bb_std_dev,
                                   macd_short, macd_long, macd_signal,
                                   rsi_p_rsi, rsi_os_rsi, rsi_ob_rsi):
        df_ticker = df_all[df_all["ticker"] == selected_ticker].copy()
        df_selection = df_ticker.loc[start_date:end_date]

        price_fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03,
                                  subplot_titles=('Price', 'Volume'), row_heights=[0.7, 0.3])
        price_fig.add_trace(go.Candlestick(x=df_selection.index, open=df_selection['open_price'],
                                           high=df_selection['high_price'], low=df_selection['low_price'],
                                           close=df_selection['close_price'], name='Price'), row=1, col=1)
        price_fig.add_trace(go.Bar(x=df_selection.index, y=df_selection['volume'], name='Volume'), row=2, col=1)
        price_fig.update_layout(title_text=f'{selected_ticker} Price Action', xaxis_rangeslider_visible=False, hovermode='x unified')
        price_chart = dcc.Graph(figure=price_fig)

        if n_clicks == 0:
            return [html.H2(f"Displaying data for {selected_ticker}"), price_chart]

        signals = pd.DataFrame()
        if strategy == "Momentum":
            signals = strategies.run_momentum_backtest(df_selection, short_ma, long_ma, rsi_p_momentum, rsi_ob_momentum)
        elif strategy == "Mean Reversion":
            signals = strategies.run_mean_reversion_backtest(df_selection, bb_window, bb_std_dev)
        elif strategy == "MACD":
            signals = strategies.run_macd_backtest(df_selection, macd_short, macd_long, macd_signal)
        elif strategy == "RSI":
            signals = strategies.run_rsi_backtest(df_selection, rsi_p_rsi, rsi_os_rsi, rsi_ob_rsi)

        portfolio = strategies.calculate_portfolio(signals, df_selection)
        portfolio_chart = dcc.Graph(figure=go.Figure(data=[go.Scatter(x=portfolio.index, y=portfolio['total'], mode='lines', name='Portfolio Value')]).update_layout(title='Portfolio Value Over Time', hovermode='x unified'))
        
        signals_fig = go.Figure()
        signals_fig.add_trace(go.Scatter(x=signals.index, y=signals['price'], mode='lines', name='Price'))
        if strategy == "Momentum":
            signals_fig.add_trace(go.Scatter(x=signals.index, y=signals[f'ma_{short_ma}'], mode='lines', name=f'MA {short_ma}'))
            signals_fig.add_trace(go.Scatter(x=signals.index, y=signals[f'ma_{long_ma}'], mode='lines', name=f'MA {long_ma}'))
        elif strategy == "Mean Reversion":
            signals_fig.add_trace(go.Scatter(x=signals.index, y=signals['upper_band'], mode='lines', name='Upper Band', line=dict(color='gray', dash='dash')))
            signals_fig.add_trace(go.Scatter(x=signals.index, y=signals['lower_band'], mode='lines', name='Lower Band', line=dict(color='gray', dash='dash')))
        elif strategy == "MACD":
            signals_fig.add_trace(go.Scatter(x=signals.index, y=signals['macd'], mode='lines', name='MACD'))
            signals_fig.add_trace(go.Scatter(x=signals.index, y=signals['signal_line'], mode='lines', name='Signal Line'))

        buy_signals = signals[signals['positions'] == 1.0]
        sell_signals = signals[signals['positions'] == -1.0]
        signals_fig.add_trace(go.Scatter(x=buy_signals.index, y=buy_signals['price'], mode='markers', name='Buy Signal', marker=dict(color='green', size=10, symbol='triangle-up')))
        signals_fig.add_trace(go.Scatter(x=sell_signals.index, y=sell_signals['price'], mode='markers', name='Sell Signal', marker=dict(color='red', size=10, symbol='triangle-down')))
        signals_fig.update_layout(title='Trading Signals & Metrics', hovermode='x unified')
        trading_signals_chart = dcc.Graph(figure=signals_fig)

        returns = portfolio['returns'].dropna()
        sharpe_ratio = (returns.mean() / returns.std()) * np.sqrt(252) if returns.std() > 0 else 0
        total_return = f"{((portfolio['total'].iloc[-1] / portfolio['total'].iloc[0]) - 1) * 100:.2f}%" if not portfolio.empty and portfolio['total'].iloc[0] != 0 else "N/A"
        num_trades = f"{int(signals['positions'].abs().sum() / 2)}"

        return [
            html.H2(f"Displaying data for {selected_ticker}"), price_chart, html.H2("Backtest Results"),
            dcc.Tabs(id="results-tabs", children=[
                dcc.Tab(label='Portfolio Value', children=[portfolio_chart]),
                dcc.Tab(label='Trading Signals', children=[trading_signals_chart]),
                dcc.Tab(label='Performance Metrics', children=[
                    html.Div([
                        html.H3("Performance Metrics"),
                        html.P(f"Total Return: {total_return}"),
                        html.P(f"Sharpe Ratio: {sharpe_ratio:.2f}"),
                        html.P(f"Number of Trades: {num_trades}")
                    ], style={'padding': '20px'})
                ])
            ])
        ]

    # --- Data Quality Callbacks ---
    @app.callback(
        Output('data-quality-content', 'children'),
        Input('dq-interval-component', 'n_intervals'),
        prevent_initial_call=True
    )
    def update_data_quality_content(n):
        conn = get_db_connection()
        try:
            query = "SELECT * FROM public.dbt_test_results ORDER BY run_generated_at DESC"
            df = pd.read_sql(query, conn)
        except Exception:
            return html.Div([
                html.H4("Error: Could not retrieve data quality results."),
                html.P("Please ensure the 'dbt_test_runner' DAG has run and the 'dbt_test_results' table exists.")
            ])
        finally:
            conn.close()

        if df.empty:
            return html.H4("No data quality test results found.")

        latest_run_time = df['run_generated_at'].max()
        df_latest = df[df['run_generated_at'] == latest_run_time]
        
        status_counts = df_latest['status'].value_counts()
        pie_fig = px.pie(values=status_counts.values, names=status_counts.index,
                         title=f"Latest Test Run Status ({pd.to_datetime(latest_run_time).strftime('%Y-%m-%d %H:%M')})",
                         color_discrete_map={'pass': '#2ca02c', 'fail': '#d62728', 'error': '#d62728'})

        df['run_date'] = pd.to_datetime(df['run_generated_at']).dt.date
        runs_by_day = df.groupby(['run_date', 'status']).size().reset_index(name='count')
        bar_fig = px.bar(runs_by_day, x='run_date', y='count', color='status', title='Test Results Over Time',
                         color_discrete_map={'pass': '#2ca02c', 'fail': '#d62728', 'error': '#d62728'})

        df_display = df_latest[['test_name', 'status', 'failures']].head(20)
        table = html.Table([
            html.Thead(html.Tr([html.Th(col) for col in df_display.columns])),
            html.Tbody([html.Tr([html.Td(df_display.iloc[i][col]) for col in df_display.columns]) for i in range(len(df_display))])
        ], className='table')

        return html.Div([
            html.Div(dcc.Graph(figure=pie_fig), style={'width': '49%', 'display': 'inline-block'}),
            html.Div(dcc.Graph(figure=bar_fig), style={'width': '49%', 'display': 'inline-block', 'float': 'right'}),
            html.H3("Latest Test Run Details"), table
        ])

# --- Helper Functions to Create Layouts ---
def create_backtesting_layout(tickers, df_all):
    """Creates the layout for the Backtesting tab."""
    return html.Div([
        html.Div([
            html.Div([
                html.Label("Select Ticker"),
                dcc.Dropdown(id='ticker-dropdown', options=[{'label': i, 'value': i} for i in tickers], value=tickers[0] if len(tickers) > 0 else None),
                html.Label("Timeframe Selection"),
                dcc.DatePickerRange(
                    id='date-picker-range',
                    min_date_allowed=df_all.index.min().date(), max_date_allowed=df_all.index.max().date(),
                    start_date=df_all.index.min().date(), end_date=df_all.index.max().date()
                ),
                html.Label("Backtesting Strategy"),
                dcc.Dropdown(id='strategy-dropdown', options=[
                    {'label': 'Momentum', 'value': 'Momentum'}, {'label': 'Mean Reversion', 'value': 'Mean Reversion'},
                    {'label': 'MACD', 'value': 'MACD'}, {'label': 'RSI', 'value': 'RSI'}], value='Momentum'
                ),
                html.Div(id='momentum-params', children=[
                    html.Label("Short-term MA (days)"), dcc.Input(id='short-ma', type='number', value=20),
                    html.Label("Long-term MA (days)"), dcc.Input(id='long-ma', type='number', value=50),
                    html.Label("RSI Period (days)"), dcc.Input(id='rsi-p-momentum', type='number', value=14),
                    html.Label("RSI Overbought Threshold"), dcc.Input(id='rsi-ob-momentum', type='number', value=70),
                ], style={'display': 'block'}),
                html.Div(id='mean-reversion-params', children=[
                    html.Label("Bollinger Band Window (days)"), dcc.Input(id='bb-window', type='number', value=20),
                    html.Label("Bollinger Band Std Dev"), dcc.Input(id='bb-std-dev', type='number', value=2.0, step=0.5),
                ], style={'display': 'none'}),
                html.Div(id='macd-params', children=[
                    html.Label("Short-term EMA (days)"), dcc.Input(id='macd-short', type='number', value=12),
                    html.Label("Long-term EMA (days)"), dcc.Input(id='macd-long', type='number', value=26),
                    html.Label("Signal Line EMA (days)"), dcc.Input(id='macd-signal', type='number', value=9),
                ], style={'display': 'none'}),
                html.Div(id='rsi-params', children=[
                    html.Label("RSI Period (days)"), dcc.Input(id='rsi-p-rsi', type='number', value=14),
                    html.Label("RSI Oversold Threshold"), dcc.Input(id='rsi-os-rsi', type='number', value=30),
                    html.Label("RSI Overbought Threshold"), dcc.Input(id='rsi-ob-rsi', type='number', value=70),
                ], style={'display': 'none'}),
                html.Button('Run Backtest', id='run-backtest', n_clicks=0),
            ], style={'width': '25%', 'display': 'inline-block', 'vertical-align': 'top'}),
            dcc.Loading(id="loading-spinner", type="circle", children=html.Div(id='main-content', style={'width': '75%', 'display': 'inline-block'}))
        ])
    ])

def create_data_quality_layout():
    """Creates the layout for the Data Quality monitoring tab."""
    return html.Div([
        html.H2("Data Quality Monitoring Dashboard"),
        html.P("This dashboard displays the results of dbt tests run against the data warehouse."),
        dcc.Interval(id='dq-interval-component', interval=60*1000, n_intervals=0),
        html.Div(id='data-quality-content')
    ])
