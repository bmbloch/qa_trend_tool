import dash
import dash_bootstrap_components as dbc

trend = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], title='Trend Review')

trend.config.suppress_callback_exceptions = True

server = trend.server
server.config['SECRET_KEY'] = 'twyt1cubt!eswip7892'