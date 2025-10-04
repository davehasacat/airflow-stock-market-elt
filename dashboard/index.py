from .app import app
from .layout import layout
from . import callbacks # This registers the callbacks

app.layout = layout

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8501, debug=True)
