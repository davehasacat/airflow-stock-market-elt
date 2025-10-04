from dash import dcc, html, Input, Output, State
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from . import strategies
import pandas as pd

def register_callbacks(app, df_all):
    """Registers all callbacks for the Dash application."""

    @app.callback(
        Output('momentum-params', 'style'),
        Output('mean-reversion-params', 'style'),
        Output('macd-params', 'style'),
        Output('rsi-params', 'style'),
        Input('strategy-dropdown', 'value'))
    def toggle_params(strategy):
        """Shows and hides parameter sections based on the selected strategy."""
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
         # Momentum States
         State('short-ma', 'value'),
         State('long-ma', 'value'),
         State('rsi-p-momentum', 'value'),
         State('rsi-ob-momentum', 'value'),
         # Mean Reversion States
         State('bb-window', 'value'),
         State('bb-std-dev', 'value'),
         # MACD States
         State('macd-short', 'value'),
         State('macd-long', 'value'),
         State('macd-signal', 'value'),
         # RSI States
         State('rsi-p-rsi', 'value'),
         State('rsi-os-rsi', 'value'),
         State('rsi-ob-rsi', 'value'),
         ])
    def update_content(n_clicks, selected_ticker, start_date, end_date, strategy,
                       short_ma, long_ma, rsi_p_momentum, rsi_ob_momentum,
                       bb_window, bb_std_dev,
                       macd_short, macd_long, macd_signal,
                       rsi_p_rsi, rsi_os_rsi, rsi_ob_rsi):

        df_ticker = df_all[df_all["ticker"] == selected_ticker].copy()
        df_selection = df_ticker.loc[start_date:end_date]

        # Create a figure with a secondary y-axis for volume
        price_fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                                  vertical_spacing=0.03, subplot_titles=('Price', 'Volume'),
                                  row_heights=[0.7, 0.3])

        # Add Candlestick trace
        price_fig.add_trace(go.Candlestick(x=df_selection.index,
                                           open=df_selection['open_price'],
                                           high=df_selection['high_price'],
                                           low=df_selection['low_price'],
                                           close=df_selection['close_price'],
                                           name='Price'), row=1, col=1)

        # Add Volume trace
        price_fig.add_trace(go.Bar(x=df_selection.index, y=df_selection['volume'], name='Volume'), row=2, col=1)

        # Update layout for a cleaner look
        price_fig.update_layout(
            title_text=f'{selected_ticker} Price Action',
            xaxis_rangeslider_visible=False,
            hovermode='x unified'
        )
        price_chart = dcc.Graph(figure=price_fig)

        if n_clicks == 0:
            return [html.H2(f"Displaying data for {selected_ticker}"), price_chart]

        # Backtesting Logic
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

        # Create Output Components
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

        # Organize results into tabs
        return [
            html.H2(f"Displaying data for {selected_ticker}"),
            price_chart,
            html.H2("Backtest Results"),
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
