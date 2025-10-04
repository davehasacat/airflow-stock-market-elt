from dash import Input, Output, State, dcc, html
import plotly.graph_objects as go
import numpy as np
from .app import app
from .data import df_all, run_momentum_backtest, run_mean_reversion_backtest, calculate_portfolio

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
    [State('ticker-dropdown', 'value'),
     State('date-picker-range', 'start_date'),
     State('date-picker-range', 'end_date'),
     State('strategy-dropdown', 'value'),
     State('short-ma', 'value'),
     State('long-ma', 'value'),
     State('rsi-p', 'value'),
     State('rsi-ob', 'value'),
     State('bb-window', 'value'),
     State('bb-std-dev', 'value')])
def update_content(n_clicks, selected_ticker, start_date, end_date, strategy,
                   short_ma, long_ma, rsi_p, rsi_ob, bb_window, bb_std_dev):
    df_ticker = df_all[df_all["ticker"] == selected_ticker].copy()
    df_selection = df_ticker.loc[start_date:end_date]

    price_chart_fig = go.Figure(data=[go.Candlestick(x=df_selection.index,
                                             open=df_selection['open_price'],
                                             high=df_selection['high_price'],
                                             low=df_selection['low_price'],
                                             close=df_selection['close_price'],
                                             name='Price')])
    price_chart_fig.update_layout(title=f'{selected_ticker} Price Action', xaxis_title='Date', yaxis_title='Price')
    price_chart = dcc.Graph(figure=price_chart_fig)

    if n_clicks == 0:
        return [html.H2(f"Displaying data for {selected_ticker}"), price_chart]

    if strategy == "Momentum":
        signals = run_momentum_backtest(df_selection, short_ma, long_ma, rsi_p, rsi_ob)
    else: # Mean Reversion
        signals = run_mean_reversion_backtest(df_selection, bb_window, bb_std_dev)

    portfolio = calculate_portfolio(signals, df_selection)

    portfolio_chart_fig = go.Figure(data=[go.Scatter(x=portfolio.index, y=portfolio['total'], mode='lines', name='Portfolio Value')])
    portfolio_chart_fig.update_layout(title='Portfolio Value Over Time')
    portfolio_chart = dcc.Graph(figure=portfolio_chart_fig)

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
    signals_fig.update_layout(title='Trading Signals & Metrics')
    trading_signals_chart = dcc.Graph(figure=signals_fig)

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
