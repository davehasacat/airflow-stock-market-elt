from dash import dcc, html

def create_layout(tickers, df_all):
    """Creates the main layout for the Dash application with tabs."""
    return html.Div([
        html.H1("Stock Market ELT Dashboard"),
        
        dcc.Tabs(id="app-tabs", value='tab-backtesting', children=[
            dcc.Tab(label='Backtesting', value='tab-backtesting'),
            dcc.Tab(label='Data Quality', value='tab-data-quality'),
        ]),
        
        # This Div will render the content of the selected tab
        html.Div(id='tabs-content-container')
    ])
