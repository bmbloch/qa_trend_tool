import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_core_components as dcc
import dash_table
import dash_daq as daq

def get_app_layout():
    return \
        html.Div([
            dcc.Location(id='home-url',pathname='/home'),
        html.Div([    
            html.Div(id='store_shim_finals', style={'display': 'none'}),
            dcc.Store(id='store_all_buttons', data=0),
            dcc.Store(id='store_submit_button', data=0),
            dcc.Store(id='store_preview_button', data=0),
            dcc.Store(id='store_init_flags', data=0),
            dcc.Store(id='store_scatter_check', data=0),
            dcc.Store(id='store_orig_cols'),
            dcc.Store(id='store_flag_cols'),
            dcc.Store(id='curryr'),
            dcc.Store(id='currmon'),
            dcc.Store(id='store_msq_load'),
            dcc.Store(id='flag_list'),
            dcc.Store(id='p_skip_list'),
            dcc.Store(id='identity_val'),
            dcc.Store(id='has_flag'),
            dcc.Store(id='input_file'),
            dcc.Store(id='store_user'),
            dcc.Store(id='init_trigger', data=False),
            dcc.Store(id='out_flag_trigger'),
            dcc.Store(id='comment_trigger'),
            dcc.Store(id='download_trigger'),
            dcc.Store(id='display_trigger'),
            dcc.Store(id='finalize_trigger'),
            dcc.Store(id='store_flag_resolve'),
            dcc.Store(id='store_flag_unresolve'),
            dcc.Store(id='store_flag_new'),
            dcc.Store(id='store_flag_skips'),
            dcc.Store(id='v_threshold'),
            dcc.Store(id='r_threshold'),
            dcc.Store(id='v_threshold_true'),
            dcc.Store(id='r_threshold_true'),
            dcc.Store(id='ncsur_props'),
            dcc.Store(id='surv_avail_props'),
            dcc.Store(id='all_avail_props'),
            dcc.Store(id='surv_rg_props'),
            dcc.Store(id='all_rg_props'),
            dcc.Store(id='newnc_props'),
            dcc.Store(id='first_roll', data=True),
            dcc.Store(id='first_scatter', data=True),
            dcc.Store(id='first_ts', data=True),
            dcc.Store(id='first_update', data=True),
            dcc.Store(id='flag_flow'),
            dcc.Store('sector'),
            dcc.ConfirmDialog(id='manual_message'),
            dcc.Tabs(id ='tab_clicked', value ='home', children=[
                dcc.Tab(label='Home', value='home', children=[
                    html.Div([
                        dbc.Alert(
                            "Something is wrong with the input file. Double check and re-start the program",
                            id = "file_load_alert",
                            dismissable=True,
                            is_open=False,
                            fade=False,
                            color='danger',
                        )
                    ], style={'text-align': 'center', 'vertical-align': 'middle'}),
                    html.Div([
                        dcc.ConfirmDialog(
                        id='confirm_finalizer',
                        displayed=False,
                        message="Clicking OK will finalize the trend and overwrite any existing finalized files previously created for this month"
                        ),
                    ]),
                    html.Div([
                        dbc.Alert(
                            html.P(id='logic_alert_text'),
                            id = "finalizer_logic_alert",
                            dismissable=True,
                            is_open=False,
                            fade=False,
                            color='danger',
                        )
                    ], style={'text-align': 'center', 'vertical-align': 'middle'}),
                    html.Div([
                        html.Div([
                            dcc.Dropdown(
                                id='dropsum',
                                        ),
                            dash_table.DataTable(
                                id='sum_table',
                                merge_duplicate_headers=True,
                                style_header={'fontWeight': 'bold', 'textAlign': 'center'},
                                                ),
                                ], style={'display': 'none'}, id='sum_table_container'),
                        html.Div([
                            dash_table.DataTable(
                                id = 'countdown',
                                style_header={'fontWeight': 'bold', 'textAlign': 'center'},
                                merge_duplicate_headers=True,
                                                ),
                                ], style={'display': 'none'}, id='countdown_container'),
                        html.Div([
                            dcc.Dropdown(
                                id='dropflag',
                                        ),
                            dash_table.DataTable(
                                id='flag_filt',
                                merge_duplicate_headers=True,
                                style_header={'fontWeight': 'bold', 'textAlign': 'center'},
                                page_action='none',
                                fixed_rows={'headers': True},
                                style_cell={'textAlign': 'center'},
                                style_cell_conditional=[
                                            {'if': {'column_id': 'Submarkets With Flag'},
                                                    'width': '50%'},
                                            {'if': {'column_id': 'Flag Ranking'},
                                                    'width': '50%'},
                                                    ],
                                                ),
                                ], style={'display': 'none'}, id='flag_filt_container'),
                        ], style={'width': '35%', 'display': 'inline-block', 'padding-left': '30px', 'padding-top': '78px'}),
                    html.Div([
                        html.Div([
                            html.Div([  
                                dbc.Row(
                                dbc.Col(
                                    dbc.Button('Export Data',id='download-button',color='primary',block=True,size='sm'),
                                    width=20
                                        ),
                                    justify='center'
                                        ),
                                    ], style={'display': 'inline-block', 'padding': '20px'}),  
                            html.Div([
                                dbc.Row(
                                dbc.Col(
                                    dbc.Button('Export Flags',id='flag-button',color='warning',block=True,size='sm'),
                                    width=20
                                        ),
                                    justify='center'
                                        ),
                                    ], style={'display': 'inline-block', 'padding': '20px'}),
                            html.Div([
                                dbc.Row(
                                dbc.Col(
                                    dbc.Button('Finalize Trend',id='finalize-button',color='success',block=True,size='sm'),
                                    width=20
                                        ),
                                    justify='center'
                                        ),
                                    ], style={'display': 'inline-block', 'padding': '20px'}),
                            html.Div([
                                dbc.Row(
                                dbc.Col(
                                    dbc.Button('Logout',id='logout-button',color='danger',block=True,size='sm'),
                                    width=20
                                        ),
                                    justify='center'
                                        ),
                                    ], style={'display': 'inline-block', 'padding': '20px'}),
                            ], style={'display': 'block', 'padding-left': '325px'}),
                        html.Div([
                            html.Div([
                                html.Div([
                                    dash_table.DataTable(
                                        id='rank_table_met',
                                        merge_duplicate_headers=True,
                                        style_header={'fontWeight': 'bold', 'textAlign': 'center'},
                                        style_cell_conditional=[
                                                {'if': {'column_id': 'Subsector'},
                                                        'width': '15%'},
                                                {'if': {'column_id': 'Metcode'},
                                                        'width': '15%'},
                                                {'if': {'column_id': '% Trend Rows W Flag'},
                                                        'width': '15%'},
                                                        ],
                                                        ),
                                        ], style={'display': 'inline-block', 'width': '48%'}),
                                html.Div([
                                    dash_table.DataTable(
                                        id='rank_table_sub',
                                        merge_duplicate_headers=True,
                                        style_header={'fontWeight': 'bold', 'textAlign': 'center'},
                                        style_cell_conditional=[
                                                {'if': {'column_id': 'Subsector'},
                                                        'width': '15%'},
                                                {'if': {'column_id': 'Metcode'},
                                                        'width': '15%'},
                                                {'if': {'column_id': 'Subid'},
                                                        'width': '15%'},
                                                {'if': {'column_id': '% Trend Rows W Flag'},
                                                        'width': '15%'},
                                                        ],
                                                        ),
                                        ], style={'display': 'inline-block', 'width': '48%', 'padding-left': '50px'}),
                            ], style={'display': 'none'}, id='rank_table_container'),
                            html.Div([
                                dash_table.DataTable(
                                    id='nat_metrics_vac',
                                    merge_duplicate_headers=True,
                                    style_header={'fontWeight': 'bold', 'textAlign': 'center', 'whiteSpace': 'normal'},
                                    style_cell_conditional=[
                                            {'if': {'column_id': 'Surveyed Abs'},
                                                    'width': '15%'},
                                            {'if': {'column_id': 'Months To Last Surv'},
                                                    'width': '15%'},
                                            {'if': {'column_id': 'Survey Cover Pct'},
                                                    'width': '15%'},
                                            {'if': {'column_id': '30 Perc Cov Pct'},
                                                    'width': '15%'},
                                            {'if': {'column_id': 'vacdrops'},
                                                    'width': '15%'},
                                            {'if': {'column_id': 'vacflats'},
                                                    'width': '15%'},
                                            {'if': {'column_id': 'vacincrs'},
                                                    'width': '15%'},
                                                    ],
                                                    ),
                                    ], style={'display': 'none'}, id='nat_metrics_vac_container'),
                            html.Div([
                                dash_table.DataTable(
                                    id='nat_metrics_rent',
                                    merge_duplicate_headers=True,
                                    style_header={'fontWeight': 'bold', 'textAlign': 'center', 'whiteSpace': 'normal'},
                                    style_cell_conditional=[
                                            {'if': {'column_id': 'Surveyed Month Rent Chg'},
                                                    'width': '15%'},
                                            {'if': {'column_id': 'Months To Last Surv'},
                                                    'width': '15%'},
                                            {'if': {'column_id': 'Survey Cover Pct'},
                                                    'width': '15%'},
                                            {'if': {'column_id': '30 Perc Cov Pct'},
                                                    'width': '15%'},
                                            {'if': {'column_id': 'rentdrops'},
                                                    'width': '15%'},
                                            {'if': {'column_id': 'rentflats'},
                                                    'width': '15%'},
                                            {'if': {'column_id': 'rentincrs'},
                                                    'width': '15%'},
                                                    ],
                                                    ),
                                    ], style={'display': 'none'}, id='nat_metrics_rent_container'),
                            ], style={'display': 'block'}),
                    ], style={'width': '65%', 'display': 'inline-block', 'vertical-align': 'top', 'padding-right': '30px', 'padding-left': '150px'}),
                    ]),
                dcc.Tab(label='Data', value='data', children=[
                    html.Div([
                        html.Div([
                            dcc.Dropdown(
                                id='dropman',
                                        ),
                                ], style={'padding-left': '10px', 'width': '9%', 'display': 'inline-block'}),
                        html.Div([
                            dcc.Checklist(
                                id='expand_hist',
                                value=["trunc"],
                                options=[
                                            {'label': ' Full History', 'value': 'full'},
                                            ],
                                labelStyle={'display': 'block'}), 
                            ],  style={'padding-left': '10px', 'width': '6%', 'display': 'inline-block', 'vertical-align': 'top'}),
                        html.Div([
                            dcc.Checklist(
                                id='show_skips',
                                value=["N"],
                                options=[
                                            {'label': ' Show Skips', 'value': 'Y'},
                                            ],
                                labelStyle={'display': 'block'}), 
                            ],  style={'padding-left': '10px', 'width': '6%', 'display': 'inline-block', 'vertical-align': 'top'}),
                        html.Div([
                            dcc.Checklist(
                                id='show_cd',
                                value=["N"],
                                options=[
                                            {'label': ' Show CD Shim', 'value': 'Y'},
                                            ],
                                labelStyle={'display': 'block'}), 
                        ]   ,  style={'display': 'none'}, id='show_cd_container'),
                        html.Div([
                            html.P(id='flag_description_noprev')
                                ], style={'display': 'none'}, id='noprev_container'),
                        html.Div([
                            html.P(id='flag_description_resolved')
                                ], style={'display': 'none'}, id='resolved_container'),
                        html.Div([
                            html.P(id='flag_description_unresolved')
                                ], style={'display': 'none'}, id='unresolved_container'),
                        html.Div([
                            html.P(id='flag_description_new')
                                ], style={'display': 'none'}, id='new_container'),
                        html.Div([
                            html.P(id='flag_description_skipped')
                                ], style={'display': 'none'}, id='skipped_container'),
                        ], style={'display': 'block'}),
                        html.Div([
                            dash_table.DataTable(
                                id='man_view',
                                style_header={'fontWeight': 'bold', 'textAlign': 'center','whiteSpace': 'normal', 'height': 'auto'},
                                merge_duplicate_headers=True,
                                page_action='none',
                                style_table={'height': '510px', 'overflowY': 'auto'},
                                fixed_rows={'headers': True},
                                style_cell_conditional=[
                                    {'if': {'column_id': 'inv shim'},
                                            'width': '3%'},
                                    {'if': {'column_id': 'cons shim'},
                                            'width': '3%'},
                                    {'if': {'column_id': 'conv shim'},
                                            'width': '4%'},
                                    {'if': {'column_id': 'demo shim'},
                                            'width': '4%'},
                                    {'if': {'column_id': 'avail shim'},
                                            'width': '4%'},
                                    {'if': {'column_id': 'mrent shim'},
                                            'width': '4%'},
                                    {'if': {'column_id': 'merent shim'},
                                            'width': '4%'},
                                    {'if': {'column_id': 'abs'},
                                            'width': '3%'},
                                    {'if': {'column_id': 'sq abs'},
                                            'width': '3%'},
                                    {'if': {'column_id': 'cons'},
                                            'width': '3%'},
                                    {'if': {'column_id': 'conv'},
                                            'width': '3%'},
                                    {'if': {'column_id': 'demo'},
                                            'width': '3%'},
                                    {'if': {'column_id': 'sq cons'},
                                            'width': '3%'},
                                    {'if': {'column_id': 'sq vac chg'},
                                            'width': '3%'},
                                        {'if': {'column_id': 'merent'},
                                            'width': '3%'},
                                    {'if': {'column_id': 'Gmerent'},
                                            'width': '3%'},
                                    {'if': {'column_id': 'sq Gmrent'},
                                            'width': '3%'},
                                    {'if': {'column_id': 'month'},
                                            'width': '4%'},
                                    {'if': {'column_id': 'gap'},
                                            'width': '3%'},
                                    {'if': {'column_id': 'gap chg'},
                                            'width': '3%'},
                                            ],
                                            ),
                                ], style={'display': 'none'}, id='man_view_container'),
                            html.Div([
                                html.Div([
                                    html.Div([ 
                                        dbc.Row(
                                            dbc.Col(
                                                dbc.Button('Submit Fix',id='submit-button',color='success',block=True,size='sm'),
                                                    width=20
                                                    ),
                                            justify='center'
                                                    ),
                                            ], style={'display': 'none'}, id='submit_button_container'),
                                    html.Div([
                                        dcc.RadioItems(
                                            id='subsequent_fix',
                                            value='r',
                                            options=[
                                                        {'label': 'rol', 'value': 'r'},
                                                        {'label': 'sq', 'value': 's'},
                                                    ],
                                            labelStyle={'display': 'inline-block', 'margin': '0 5px 0 0'}), 
                                            ], style={'display': 'none'}, id='subsequent_change_container'),
                                    ], style={'display': 'inline-block'}),
                                html.Div([ 
                                    dbc.Row(
                                        dbc.Col(
                                            dbc.Button('Preview Fix',id='preview-button',color='warning',block=True,size='sm'),
                                                width=20
                                                ),
                                        justify='center'
                                                ),
                                        ], style={'display': 'none'}, id='preview_button_container'),
                                html.Div([
                                    dcc.Textarea(
                                        id='comment_cons',
                                        style={'width': '110%', 'height': 60},
                                        title="Cons Shim Note",
                                        draggable=False,
                                        spellCheck=False
                                    ),
                                ], style={'display': 'none'}, id='comment_cons_container'),
                                html.Div([
                                    dcc.Textarea(
                                        id='comment_avail',
                                        style={'width': '110%', 'height': 60},
                                        title="Avail Shim Note",
                                        draggable=False,
                                        spellCheck=False
                                    ),
                                ], style={'display': 'none'}, id='comment_avail_container'),
                                html.Div([
                                    dcc.Textarea(
                                        id='comment_mrent',
                                        style={'width': '110%', 'height': 60},
                                        title="Mrent Shim Note",
                                        draggable=False,
                                        spellCheck=False
                                    ),
                                ], style={'display': 'none'}, id='comment_mrent_container'),
                                html.Div([
                                    dcc.Textarea(
                                        id='comment_erent',
                                        style={'width': '110%', 'height': 60},
                                        title="Erent Shim Note",
                                        draggable=False,
                                        spellCheck=False
                                    ),
                                ], style={'display': 'none'}, id='comment_erent_container'),
                            ], style={'display': 'block'}),
                            html.Div([
                            html.Div([
                                dash_table.DataTable(
                                    id='key_metrics',
                                    merge_duplicate_headers=True,
                                    style_header={'fontWeight': 'bold', 'textAlign': 'center', 'whiteSpace': 'normal'},
                                    tooltip_duration=None,
                                                ),
                                    ], style={'display': 'none'}, id='key_metrics_container'),
                            html.Div([
                                dcc.RadioItems(
                                    id='key_met_radios',
                                    options=[
                                                {'label': 'cons', 'value': 'c'},
                                                {'label': 'vac', 'value': 'v'},
                                                {'label': 'mrent', 'value': 'g'},
                                                {'label': 'erent', 'value': 'e'},
                                            ],
                                    value='v',
                                    labelStyle={'display': 'block', 'margin': '0 10px 0 10px'}), 
                                    ], style={'display': 'none'}, id='key_met_radios_container'),
                            ], style={'display': 'block'}),
                            html.Div([
                                dash_table.DataTable(
                                    id='key_metrics_2',
                                    merge_duplicate_headers=True,
                                    style_header={'fontWeight': 'bold', 'textAlign': 'center'},
                                                ),
                                    ], style={'display': 'none'}, id='key_metrics_2_container'),
                            html.Div([
                                html.Div([
                                    dcc.Graph(
                                        id='vac_series',
                                        config={'displayModeBar': False}
                                            )
                                        ], style={'display': 'none'}, id='vac_series_container'),
                                html.Div([
                                    dcc.Graph(
                                        id='rent_series',
                                        config={'displayModeBar': False}
                                            )
                                        ], style={'display': 'none'}, id='rent_series_container'),
                                    ], style={'display': 'block'}),
                            ]),
                dcc.Tab(label='Graphs', value='graphs', children=[
                    html.Div([
                        html.Div([
                            dcc.Dropdown(
                                id='scatter-xaxis-var',
                                value='vac_chg'
                                        )
                                ], style={'width': '10%', 'display': 'inline-block'}),
                        html.Div([
                            dcc.Dropdown(
                                id='scatter-yaxis-var',
                                value='G_mrent'
                                        )
                                ], style={'width': '10%', 'display': 'inline-block'}),
                        html.Div([
                            dcc.RadioItems(
                                id='scatter-type-radios',
                                options=[
                                            {'label': 'Currmon', 'value': 'c'},
                                            {'label': 'ROL', 'value': 'r'},
                                            {'label': 'Square', 'value': 'q'},
                                            {'label': 'Survey', 'value': 's'},
                                        ],
                                value='c',
                                labelStyle={'display': 'inline-block', 'margin': '0 10px 0 10px'}), 
                            ], style={'display': 'inline-block', 'padding-left': '20px', 'vertical-align': 'top'}),
                        html.Div([
                            dcc.Checklist(
                                id='flags_only',
                                value=[],
                                options=[
                                            {'label': ' Flags Only', 'value': 'f'},
                                            ],
                                labelStyle={'display': 'block', 'margin': '0 10px 0 10px'}), 
                                ],  style={'padding-left': '10px', 'display': 'inline-block', 'vertical-align': 'top'}),
                        html.Div([
                                daq.ToggleSwitch(
                                    id='aggreg_level',
                                    label=['Sub', 'Met'],
                                    style={'width': '5px', 'margin': 'auto'},
                                    value=False,
                                    ),
                                ], style={'padding-left': '70px', 'display': 'inline-block', 'vertical-align': 'top'}),
                        ], style={'padding': '10px 5px'}),
                    html.Div([
                        dcc.Graph(
                            id='scatter_graph',
                                )
                        ], style={'display': 'none'}, id='scatter_container'),
                    html.Div([
                        html.Div([
                            dcc.Graph(
                                id='x_time_series',
                                config={'displayModeBar': False}
                                    ),
                                ], style={'display': 'none'}, id='x_ts_container'),
                        html.Div([
                            dcc.Graph(
                                id='y_time_series',
                                config={'displayModeBar': False}
                                    ),
                            ], style={'display': 'none'}, id='y_ts_container'),
                        ], style={'display': 'inline-block', 'width': '49%', 'padding-left': '150px', 'vertical-align': 'top'}),   
                    ]),
                dcc.Tab(label='Rollups', value='rollups', children=[
                    html.Div([
                        html.Div([
                            dcc.Dropdown(
                                id='droproll',
                                ),
                        ], style={'width': '50%', 'padding-left': '10px', 'padding-top': '5px', 'display': 'inline-block'}),
                        html.Div([
                                daq.ToggleSwitch(
                                    id='roll_view',
                                    label=['Single', 'Multi'],
                                    style={'width': '5px', 'margin': 'auto'},
                                    value=False,
                                    ),
                                ], style={'width': '20%', 'padding-left': '10px', 'padding-top': '5px', 'display': 'inline-block', 'vertical-align': 'top'}),
                        html.Div([
                                daq.ToggleSwitch(
                                    id='currmon_filt',
                                    label=['All', 'Currmon'],
                                    value=False,
                                    style={'width': '5px', 'margin': 'auto'},
                                    ),
                            ], style={'width': '20%', 'display': 'inline-block', 'padding-left': '10px', 'padding-top': '5px', 'vertical-align': 'top'}),
                        html.Div([
                            dash_table.DataTable(
                                id='metroll',
                                style_header={'fontWeight': 'bold', 'textAlign': 'center', 'whiteSpace': 'normal', 'height': 'auto'},
                                merge_duplicate_headers=True,
                                style_cell_conditional=[
                                        {'if': {'column_id': 'subsector'},
                                                'width': '5%'},
                                        {'if': {'column_id': 'met'},
                                                'width': '3%'},
                                        {'if': {'column_id': 'subid'},
                                                'width': '3%'},
                                        {'if': {'column_id': 'month'},
                                                'width': '3%'},
                                        {'if': {'column_id': 'inv'},
                                                'width': '5%'},
                                        {'if': {'column_id': 'sq inv'},
                                                'width': '5%'},
                                        {'if': {'column_id': 'cons'},
                                                'width': '4%'},
                                        {'if': {'column_id': 'rol cons'},
                                                'width': '4%'},
                                        {'if': {'column_id': 'sq cons'},
                                                'width': '4%'},
                                        {'if': {'column_id': 'rol vac'},
                                                'width': '4%'},
                                        {'if': {'column_id': 'vac'},
                                                'width': '4%'},
                                        {'if': {'column_id': 'vac chg'},
                                                'width': '4%'},
                                        {'if': {'column_id': 'rol vac chg'},
                                                'width': '4%'},
                                        {'if': {'column_id': 'sq vac'},
                                                'width': '4%'},
                                        {'if': {'column_id': 'sq vac chg'},
                                                'width': '4%'},
                                        {'if': {'column_id': 'mrent'},
                                                'width': '4%'},
                                        {'if': {'column_id': 'sq rent'},
                                                'width': '4%'},
                                        {'if': {'column_id': 'rol Gmrent'},
                                                'width': '4%'},
                                        {'if': {'column_id': 'merent'},
                                                'width': '4%'},
                                         {'if': {'column_id': 'rol Gmerent'},
                                                'width': '4%'},
                                         {'if': {'column_id': 'yr'},
                                                'width': '3%'},
                                        {'if': {'column_id': 'abs'},
                                                'width': '4%'},
                                        {'if': {'column_id': 'rol abs'},
                                                'width': '4%'},
                                        {'if': {'column_id': 'sq abs'},
                                                'width': '4%'},
                                        {'if': {'column_id': 'Gmrent'},
                                                'width': '4%'},
                                        {'if': {'column_id': 'sq Gmrent'},
                                                'width': '4%'},
                                        {'if': {'column_id': 'Gmerent'},
                                                'width': '4%'},
                                         {'if': {'column_id': 'gap'},
                                                'width': '4%'},
                                         {'if': {'column_id': 'gap chg'},
                                                'width': '4%'},
                                        ]
                                ),
                            ], style={'display': 'none'}, id='metroll_container'),
                    ], style={'display': 'block'}),
                    html.Div([
                        html.Div([
                            dcc.Graph(
                                id='vac_series_met',
                                config={'displayModeBar': False}
                                    )
                                ],style={'display': 'none'}, id='vac_series_met_container'),
                        html.Div([
                            dcc.Graph(
                                id='rent_series_met',
                                config={'displayModeBar': False}
                                    )
                                ],style={'display': 'none'}, id='rent_series_met_container'),
                        ], style={'display': 'block'}),
                    html.Div([
                        dcc.RadioItems(
                            id='metro_sorts',
                            options=[
                                        {'label': 'Cons', 'value': 'cons'},
                                        {'label': 'Vac Chg', 'value': 'vac_chg'},
                                        {'label': 'Abs', 'value': 'abs'},
                                        {'label': 'Gmrent', 'value': 'G_mrent'},
                                        {'label': 'Gap Chg', 'value': 'gap_chg'},
                                    ],
                            value='vac_chg',
                            labelStyle={'display': 'inline-block', 'margin': '0 10px 0 10px'}), 
                        ], style={'display': 'none'}, id='metro_sorts_container'),
                    html.Div([
                        html.Div([
                           dash_table.DataTable(
                                id='sub_rank',
                                merge_duplicate_headers=True,
                                style_header={'fontWeight': 'bold', 'textAlign': 'center', 'whiteSpace': 'normal',},
                                sort_action="native",
                                filter_action="native",
                                ),
                            ], style={'display': 'none'}, id='sub_rank_container'),
                        html.Div([
                            dash_table.DataTable(
                                id='met_rank',
                                merge_duplicate_headers=True,
                                style_header={'fontWeight': 'bold', 'textAlign': 'center', 'whiteSpace': 'normal',},
                                sort_action="native",
                                filter_action="native",
                                ),
                            ], style={'display': 'none'}, id='met_rank_container'),
                        ], style={'display': 'block'}),
                    ]),
                ]),
            ])
        ])