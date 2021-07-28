import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_core_components as dcc

from dash.exceptions import PreventUpdate
from dash.dependencies import Input, Output, State
from datetime import datetime
import math
import numpy as np
import pandas as pd
from pathlib import Path
import os
from os import listdir
from os.path import isfile, join

def get_home():
    if os.name == "nt": return "//odin/reisadmin/"
    else: return "/home/"

# Layout for login page
def get_login_layout():

    root = Path("{}central/square/data/zzz-bb-test2/python/trend/ind/".format(get_home()))
    dirlist = [item for item in os.listdir(root) if os.path.isdir(os.path.join(root, item)) ]
    latest_year = 0
    latest_month = 0
    month_to_display = 0
    for folder in dirlist:
        if (int(folder[0:4]) == latest_year and int(folder[-1:]) >= latest_month) or int(folder[0:4]) > latest_year:
            latest_year = int(folder[0:4])
            if len(folder) == 7:
                latest_month = int(folder[-2:])
            else:
                latest_month = int(folder[-1:])

    return html.Div([
            dcc.Location(id='login-url',pathname='/login',refresh=False),
            dbc.Container([
                dbc.Row(
                    dbc.Col(
                        dbc.Card([
                            html.H4('Login',className='card-title'),
                            dbc.Input(id='login-username',placeholder='User'),
                            dbc.Input(id='login-password',placeholder='Password',type='password'),
                            html.Div([
                                html.Div([
                                    dcc.Dropdown(id='sector_input', 
                                            options=[{'value': 'apt', 'label': 'Apartment'}, {'value': 'ind', 'label': 'Industrial'},
                                                    {'value': 'off', 'label': 'Office'}, {'value': 'ret', 'label': 'Retail'}],
                                            multi=False,
                                            value=None,
                                            placeholder="Sector to load:"
                                            ),
                                        ], style={'display': 'inline-block', 'width': '50%'}),
                                html.Div([
                                    dcc.Dropdown(id='flag_flow_input', 
                                                options=[{'value': 'sub', 'label': 'Sub By Sub'}, 
                                                        {'value': 'cat', 'label': 'By Flag Cat'},
                                                        {'value': 'flag', 'label': 'By Flag'}],
                                                multi=False,
                                                value="sub",
                                                placeholder="Choose flag cycle:"
                                                ),
                                            ], style={'display': 'inline-block', 'width': '50%', 'padding-left': '10px'}),
                                ], style={'display': 'block', 'padding-top': '15px'}),
                            html.Br(),
                            html.Div([
                                html.Div([
                                    html.P(id='Trend Year Header', 
                                        style={'color': 'black', 'fontSize': 12},
                                        children=["Trend Year"]
                                        )
                                    ], style={'padding-left': '40px', 'display': 'inline-block'}),
                                html.Div([
                                    html.P(id='Trend Month Header', 
                                        style={'color': 'black', 'fontSize': 12},
                                        children=["Trend Month"]
                                        )
                                    ], style={'padding-left': '100px', 'display': 'inline-block'}),
                                ], style={'display': 'block'}),
                            html.Div([
                                html.Div([
                                    dcc.Dropdown(id='login-curryr', 
                                                options=[{'value': latest_year - 1, 'label': latest_year - 1}, 
                                                            {'value': latest_year, 'label': latest_year},
                                                            {'value': latest_year + 1, 'label': latest_year + 1}],
                                                multi=False,
                                                value=latest_year,
                                                ),
                                                    ], style={'width': '30%', 'display': 'inline-block'}),
                                html.Div([
                                    dcc.Dropdown(id='login-currmon', 
                                                options=[{'value': 1, 'label': 1}, 
                                                            {'value': 2, 'label': 2},
                                                            {'value': 3, 'label': 3},
                                                            {'value': 4, 'label': 4},
                                                            {'value': 5, 'label': 5},
                                                            {'value': 6, 'label': 6},
                                                            {'value': 7, 'label': 7},
                                                            {'value': 8, 'label': 8},
                                                            {'value': 9, 'label': 9},
                                                            {'value': 10, 'label': 10},
                                                            {'value': 11, 'label': 11},
                                                            {'value': 12, 'label': 12}],
                                                multi=False,
                                                value=latest_month,
                                                ),
                                                    ], style={'width': '30%', 'display': 'inline-block', 'padding-left': '20px'}),
                                html.Div([
                                    dcc.Checklist(
                                        id='msq_load',
                                        value=["Y"],
                                        options=[
                                            {'label': ' MSQs Updated', 'value': 'Y'},
                                            ],
                                        labelStyle={'display': 'block', 'margin': '0 10px 0 10px'}), 
                                    ],  style={'padding-left': '10px', 'width': '40%', 'display': 'inline-block', 'vertical-align': 'top'}),
                            ], style={'display': 'block'}),
                                
                            html.Br(),
                            dbc.Button('Submit',id='login-button',color='success',block=True),
                            html.Br(),
                            html.Div(id='login-alert'),
                        ],
                        body=True
                    ),
                    width=6
                ),
                justify='center'
                    ),
                ]),
            ])