import dash
from components.data import load_data
from components.layout import create_layout
from components.callbacks import register_callbacks

# --- Main App Logic ---
df_all = load_data()
tickers = df_all["ticker"].unique()

app = dash.Dash(__name__, suppress_callback_exceptions=True)
server = app.server

app.layout = create_layout(tickers, df_all)
register_callbacks(app, df_all)

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8501, debug=True)
