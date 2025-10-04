from dash import dcc, html, Input, Output
import plotly.graph_objects as go
import numpy as np
from . import strategies

def register_callbacks(app, df_all):
    """Registers all callbacks for the Dash application."""

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
        [Input('ticker-dropdown', 'value'),
         Input('date-picker-range', 'start_date'),
         Input('date-picker-range', 'end_date'),
         Input('strategy-dropdown', 'value'),
         Input('short-ma', 'value'),
         Input('long-ma', 'value'),
         Input('rsi-p', 'value'),
         Input('rsi-ob', 'value'),
         Input('bb-window', 'value'),
         Input('bb-std-dev', 'value')])
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
                signals = strategies.run_momentum_backtest(df_selection, short_ma, long_ma, rsi_p, rsi_ob)
            else: # Mean Reversion
                signals = strategies.run_mean_reversion_backtest(df_selection, bb_window, bb_std_dev)

            portfolio = strategies.calculate_portfolio(signals, df_selection)
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
