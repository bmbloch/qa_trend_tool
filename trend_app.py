import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output, State, MATCH, ALL
from dash import no_update
import plotly.graph_objs as go 
from flask import session, copy_current_request_context
import dash_table
import dash_table.FormatTemplate as FormatTemplate
from dash_table.Format import Format, Scheme
from dash.exceptions import PreventUpdate
import urllib
import os
import numpy as np
import pandas as pd
import csv
from pathlib import Path
import re
from datetime import datetime
import math
pd.set_option('display.max_rows',  1000)
pd.set_option('display.max_columns', 100)
pd.options.display.float_format = '{:.4f}'.format
from IPython.core.display import display, HTML
import timeit

# local imports
from init_load_trend import get_home, initial_load
from authenticate_trend import authenticate_user, validate_login_session
from server_trend import trend, server
from stats_trend import calc_stats
from flags_trend import calc_flags
from support_functions_trend import set_display_cols, display_frame, gen_metrics, drop_cols, rollup, live_flag_count, summarize_flags_ranking, summarize_flags, get_issue, get_diffs, rank_it, flag_examine, create_review_packet
from trend_app_layout import get_app_layout
from login_layout_trend import get_login_layout
#from timer_trend import Timer


# Function that determines the data type - int, float, etc - so that the correct format can be set for the app display
def get_types(sector_val):

    type_dict = {}
    format_dict = {}
    
    type_dict['inv'] = 'numeric'
    type_dict['sq inv'] = 'numeric'
    type_dict['cons'] = 'numeric'
    type_dict['sq cons'] = 'numeric'
    type_dict['vac'] = 'numeric'
    type_dict['vac chg'] = 'numeric'
    type_dict['rol vac'] = 'numeric'
    type_dict['sq vac'] = 'numeric'
    type_dict['rol vac chg'] = 'numeric'
    type_dict['sq vac chg'] = 'numeric'
    type_dict['sq abs'] = 'numeric'
    type_dict['sq rent'] = 'numeric'
    type_dict['Gmrent'] = 'numeric'
    type_dict['rol Gmrent'] = 'numeric'
    type_dict['sq Gmrent'] = 'numeric'
    type_dict['Gmerent'] = 'numeric'
    type_dict['rol Gmerent'] = 'numeric'
    type_dict['gap chg'] = 'numeric'
    type_dict['rol gap chg'] = 'numeric'
    type_dict['abs'] = 'numeric'
    type_dict['sq avail'] = 'numeric'
    type_dict['rol cons'] = 'numeric'
    type_dict['rol abs'] = 'numeric'
    type_dict['inv shim'] = 'numeric'
    type_dict['cons shim'] = 'numeric'
    type_dict['conv shim'] = 'numeric'
    type_dict['demo shim'] = 'numeric'
    type_dict['conv'] = 'numeric'
    type_dict['demo'] = 'numeric'
    type_dict['avail shim'] = 'numeric'
    type_dict['mrent shim'] = 'numeric'
    type_dict['merent shim'] = 'numeric'
    type_dict['yr'] = 'numeric'
    type_dict['month'] = 'numeric'
    type_dict['c met sur totabs'] = 'numeric'
    type_dict['c sub sur totabs'] = 'numeric'
    type_dict['n met sur totabs'] = 'numeric'
    type_dict['n sub sur totabs'] = 'numeric'
    type_dict['nc met sur totabs'] = 'numeric'
    type_dict['nc sub sur totabs'] = 'numeric'
    type_dict['Total Flags'] = 'numeric'
    type_dict['Cons Flags'] = 'numeric'
    type_dict['Vac Flags'] = 'numeric'
    type_dict['Rent Flags'] = 'numeric'
    type_dict['% Currmon Trend Rows W Flag'] = 'numeric'
    type_dict['% Trend Rows W Flag'] = 'numeric'
    type_dict['% Subs W Flag'] = 'numeric'
    type_dict['Subid'] = 'numeric'
    type_dict['mrent'] = 'numeric'
    type_dict['merent'] = 'numeric'
    type_dict['occ'] = 'numeric'
    type_dict['avail'] = 'numeric'
    type_dict['gap'] = 'numeric'
    type_dict['Surveyed Abs'] = 'numeric'
    type_dict['newncrev'] = 'numeric'
    type_dict['avail10d'] = 'numeric'
    type_dict['dqren10d'] = 'numeric'
    type_dict['Months To Last Surv'] = 'numeric'
    type_dict['Survey Cover Pct'] = 'numeric'
    type_dict['vacdrops'] = 'numeric'
    type_dict['vacflats'] = 'numeric'
    type_dict['vacincrs'] = 'numeric'
    type_dict['rentdrops'] = 'numeric'
    type_dict['rentflats'] = 'numeric'
    type_dict['rentincrs'] = 'numeric'
    type_dict['Surveyed Month Rent Chg'] = 'numeric'
    type_dict['ss rent chg'] = 'numeric'
    type_dict['ss vac chg'] = 'numeric'
    type_dict['sub1to99 Grenx'] = 'numeric'
    type_dict['gmrent roldiff'] = 'numeric'
    type_dict['gmerent roldiff'] = 'numeric'
    type_dict['Gmrent 12'] = 'numeric'
    type_dict['sq Gmrent 12'] = 'numeric'
    type_dict['vac chg 12'] = 'numeric'
    type_dict['sqvac chg 12'] = 'numeric'
    type_dict['vac roldiff'] = 'numeric'
    type_dict['met grenx mo wgt'] = 'numeric'
    type_dict['c met grenx mo wgt'] = 'numeric'
    type_dict['n met grenx mo wgt'] = 'numeric'
    type_dict['nc met grenx mo wgt'] = 'numeric'
    type_dict['c sub grenx mo wgt'] = 'numeric'
    type_dict['n sub grenx mo wgt'] = 'numeric'
    type_dict['nc sub grenx mo wgt'] = 'numeric'
    type_dict['sub grenx mo wgt'] = 'numeric'
    type_dict['met sur totabs'] = 'numeric'
    type_dict['sub sur totabs'] = 'numeric'
    type_dict['met sur r cov'] = 'numeric'
    type_dict['sub sur r cov'] = 'numeric'
    type_dict['c met sur r cov'] = 'numeric'
    type_dict['c sub sur r cov'] = 'numeric'
    type_dict['n met sur r cov'] = 'numeric'
    type_dict['n sub sur r cov'] = 'numeric'
    type_dict['nc met sur r cov'] = 'numeric'
    type_dict['nc sub sur r cov'] = 'numeric'
    type_dict['met sur v cov'] = 'numeric'
    type_dict['sub sur v cov'] = 'numeric'
    type_dict['c met sur v cov'] = 'numeric'
    type_dict['c sub sur v cov'] = 'numeric'
    type_dict['n met sur v cov'] = 'numeric'
    type_dict['n sub sur v cov'] = 'numeric'
    type_dict['nc met sur v cov'] = 'numeric'
    type_dict['nc sub sur v cov'] = 'numeric'
    type_dict['met mos lrsurv'] = 'numeric'
    type_dict['sub mos lrsurv'] = 'numeric'
    type_dict['met mos lvsurv'] = 'numeric'
    type_dict['sub mos lvsurv'] = 'numeric'
    type_dict['newncsf'] = 'numeric'
    type_dict['newncava'] = 'numeric'
    type_dict['gap perc 5'] = 'numeric'
    type_dict['gap perc 95'] = 'numeric'
    type_dict['cons roldiff'] = 'numeric'
    type_dict['ncrenlev'] = 'numeric'
    type_dict['nc surabs'] = 'numeric'
    
    type_dict['Subsector'] = 'text'
    type_dict['subsector'] = 'text'
    type_dict['Metcode'] = 'text'
    type_dict['metcode'] = 'text'
    type_dict['met'] = 'text'
    type_dict['subid'] = 'text'
    type_dict['Flag Type'] = 'text'

    format_dict['inv'] = Format(group=",")
    format_dict['sq inv'] = Format(group=",")
    format_dict['sq inv'] = Format(group=",")
    format_dict['inv shim'] = Format(group=",")
    format_dict['cons'] = Format(group=",")
    format_dict['sq cons'] = Format(group=",")
    format_dict['rol cons'] = Format(group=",")
    format_dict['cons shim'] = Format(group=",")
    format_dict['cons roldiff'] = Format(group=",")
    format_dict['conv'] = Format(group=",")
    format_dict['conv shim'] = Format(group=",")
    format_dict['demo'] = Format(group=",")
    format_dict['demo shim'] = Format(group=",")
    format_dict['occ'] = Format(group=",")
    format_dict['avail'] = Format(group=",")
    format_dict['sq avail'] = Format(group=",")
    format_dict['avail shim'] = Format(group=",")
    format_dict['abs'] = Format(group=",")
    format_dict['sq abs'] = Format(group=",")
    format_dict['rol abs'] = Format(group=",")
    format_dict['newncava'] = Format(group=",")
    format_dict['avail10d'] = Format(group=",")
    format_dict['met sur totabs'] = Format(group=",")
    format_dict['sub sur totabs'] = Format(group=",")
    format_dict['c met sur totabs'] = Format(group=",")
    format_dict['c sub sur totabs'] = Format(group=",")
    format_dict['n met sur totabs'] = Format(group=",")
    format_dict['n sub sur totabs'] = Format(group=",")
    format_dict['nc met sur totabs'] = Format(group=",")
    format_dict['nc sub sur totabs'] = Format(group=",")
    format_dict['newncrev'] = Format(group=",")
    format_dict['newncsf'] = Format(group=",")
    format_dict['Surveyed Abs'] = Format(group=",")
    format_dict['nc surabs'] = Format(group=",")
    
    format_dict['vac'] = FormatTemplate.percentage(2)
    format_dict['sq vac'] = FormatTemplate.percentage(2)
    format_dict['vac chg'] = FormatTemplate.percentage(2)
    format_dict['sq vac chg'] = FormatTemplate.percentage(2)
    format_dict['rol vac'] = FormatTemplate.percentage(2)
    format_dict['rol vac chg'] = FormatTemplate.percentage(2)
    format_dict['Gmrent'] = FormatTemplate.percentage(2)
    format_dict['sq Gmrent'] = FormatTemplate.percentage(2)
    format_dict['rol Gmrent'] = FormatTemplate.percentage(2)
    format_dict['Gmerent'] = FormatTemplate.percentage(2)
    format_dict['rol Gmerent'] = FormatTemplate.percentage(2)
    format_dict['gap'] = FormatTemplate.percentage(2)
    format_dict['gap chg'] = FormatTemplate.percentage(2)
    format_dict['rol gap chg'] = FormatTemplate.percentage(2)
    format_dict['Surveyed Month Rent Chg'] = FormatTemplate.percentage(2)
    format_dict['met grenx mo wgt'] = FormatTemplate.percentage(2)
    format_dict['sub grenx mo wgt'] = FormatTemplate.percentage(2)
    format_dict['c met grenx mo wgt'] = FormatTemplate.percentage(2)
    format_dict['c sub grenx mo wgt'] = FormatTemplate.percentage(2)
    format_dict['n met grenx mo wgt'] = FormatTemplate.percentage(2)
    format_dict['n sub grenx mo wgt'] = FormatTemplate.percentage(2)
    format_dict['nc met grenx mo wgt'] = FormatTemplate.percentage(2)
    format_dict['nc sub grenx mo wgt'] = FormatTemplate.percentage(2)
    format_dict['dqren10d'] = FormatTemplate.percentage(2)
    format_dict['ss rent chg'] = FormatTemplate.percentage(2)
    format_dict['ss vac chg'] = FormatTemplate.percentage(2)
    format_dict['sub1to99 Grenx'] = FormatTemplate.percentage(2)
    format_dict['gmrent roldiff'] = FormatTemplate.percentage(2)
    format_dict['gmerent roldiff'] = FormatTemplate.percentage(2)
    format_dict['met grenx mo wgt'] = FormatTemplate.percentage(2)
    format_dict['sub grenx mo wgt'] = FormatTemplate.percentage(2)
    format_dict['gap perc 5'] = FormatTemplate.percentage(2)
    format_dict['gap perc 95'] = FormatTemplate.percentage(2)
    format_dict['Gmrent 12'] = FormatTemplate.percentage(2)
    format_dict['sq Gmrent 12'] = FormatTemplate.percentage(2)
    
    format_dict['Survey Cover Pct'] = FormatTemplate.percentage(1)
    format_dict['% Currmon Trend Rows W Flag'] = FormatTemplate.percentage(1)
    format_dict['% Trend Rows W Flag'] = FormatTemplate.percentage(1)
    format_dict['% Subs W Flag'] = FormatTemplate.percentage(1)
    format_dict['vac roldiff'] = FormatTemplate.percentage(1)
    format_dict['covren'] = FormatTemplate.percentage(1)
    format_dict['covvac'] = FormatTemplate.percentage(1)
    format_dict['met sur r cov'] = FormatTemplate.percentage(1)
    format_dict['sub sur r cov'] = FormatTemplate.percentage(1)
    format_dict['c met sur r cov'] = FormatTemplate.percentage(1)
    format_dict['c sub sur r cov'] = FormatTemplate.percentage(1)
    format_dict['n met sur r cov'] = FormatTemplate.percentage(1)
    format_dict['n sub sur r cov'] = FormatTemplate.percentage(1)
    format_dict['nc met sur r cov'] = FormatTemplate.percentage(1)
    format_dict['nc sub sur r cov'] = FormatTemplate.percentage(1)
    format_dict['met sur v cov'] = FormatTemplate.percentage(1)
    format_dict['sub sur v cov'] = FormatTemplate.percentage(1)
    format_dict['c met sur v cov'] = FormatTemplate.percentage(1)
    format_dict['c sub sur v cov'] = FormatTemplate.percentage(1)
    format_dict['n met sur v cov'] = FormatTemplate.percentage(1)
    format_dict['n sub sur v cov'] = FormatTemplate.percentage(1)
    format_dict['nc met sur v cov'] = FormatTemplate.percentage(1)
    format_dict['nc sub sur v cov'] = FormatTemplate.percentage(1)
    format_dict['vac chg 12'] = FormatTemplate.percentage(1)
    format_dict['sqvac chg 12'] = FormatTemplate.percentage(1)
    

    format_dict['mrent'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['sq rent'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['mrent shim'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['rol mrent'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['merent'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['merent shim'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['rol meren'] = Format(precision=2, scheme=Scheme.fixed)
    format_dict['ncrenlev'] = Format(precision=2, scheme=Scheme.fixed)


    format_dict['yr'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['month'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['Subsector'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['subsector'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['Metcode'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['metcode'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['Subid'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['subid'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['met'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['Flag Type'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['rentdrops'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['rentflats'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['rentincrs'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['vacdrops'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['vacflats'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['vacincrs'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['Total Flags'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['Cons Flags'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['Vac Flags'] = Format(precision=0, scheme=Scheme.fixed)
    format_dict['Rent Flags'] = Format(precision=0, scheme=Scheme.fixed)
    

    format_dict['Months To Last Surv'] = Format(precision=1, scheme=Scheme.fixed)
    format_dict['met mos lrsurv'] = Format(precision=1, scheme=Scheme.fixed)
    format_dict['sub mos lrsurv'] = Format(precision=1, scheme=Scheme.fixed)
    format_dict['met mos lvsurv'] = Format(precision=1, scheme=Scheme.fixed)
    format_dict['sub mos lvsurv'] = Format(precision=1, scheme=Scheme.fixed)


    return type_dict, format_dict

# Function that returns the highlighting style of the various dash datatables
def get_style(type_filt, dataframe_in, curryr, currmon, highlight_cols, highlight_rows):
 
    dataframe = dataframe_in.copy()
    
    if type_filt == "full":
        style = [ 
                        {
                            'if': {'column_id': str(x), 'filter_query': '{{{0}}} < 0'.format(x)},
                            'color': 'red',
                        } for x in dataframe.columns
                
                    ]  + [
                    
                    {'if': 
                        {'row_index': 'odd'}, 
                            'backgroundColor': 'rgb(248, 248, 248)'
                    }
                ] + [
                            
                        {
                            'if': {
                            'filter_query':  '{month} eq ' + str(currmon) + ' && {yr} eq ' + str(curryr)
                                },
                    
                        'backgroundColor': 'yellow'
                        },
                    ] + [
                   {
                        'if': {
                        'column_id': highlight_cols,
                        'row_index': highlight_rows,
                              },
                        'backgroundColor': 'LightGreen',
                    }

                ]
                
    elif type_filt == "partial":
        style = [ 
                    {
                        'if': {'column_id': list(dataframe.columns), 
                        'filter_query': '{{{0}}} < 0'.format(x)
                              },
                        'color': 'red',
                    }
                ]

    elif type_filt == "metrics":
        style = [ 
                    {
                        'if': {'column_id': list(dataframe.columns),
                        'filter_query': '{{{0}}} < 0'.format(x)
                              },
                        'color': 'red',
                    }
                ] + [
                   {
                        'if': {'column_id': highlight_cols
                              },
                            'backgroundColor': 'LightGreen',
                    }

                ]
    return style
    
    
    

def filter_graph(input_dataframe, curryr, currmon, type_value, xaxis_var, yaxis_var, sector_val, flags_only, aggreg_met):
    dataframe = input_dataframe.copy()

    dataframe['vac_chg'] = round(dataframe['vac_chg'], 4)
    dataframe['G_mrent'] = round(dataframe['G_mrent'], 4)

    if type_value == "c":
        dataframe = dataframe[(dataframe['yr'] == curryr) & (dataframe['currmon'] == currmon)]
    elif type_value == "r":
        if xaxis_var == "abs" or xaxis_var == "cons":
            dataframe = dataframe[(dataframe[xaxis_var] != dataframe[yaxis_var]) & (np.isnan(dataframe[yaxis_var]) == False)]
        else:
            dataframe = dataframe[(abs(dataframe[xaxis_var] - dataframe[yaxis_var]) > 0.001) & (np.isnan(dataframe[yaxis_var]) == False)]
    elif type_value == "q" or type_value == "s":
        dataframe = dataframe[(dataframe['yr'] == curryr) & (dataframe['currmon'] == currmon)]
        if xaxis_var == "abs" or xaxis_var == "cons":
            dataframe = dataframe[(dataframe[xaxis_var] != dataframe[yaxis_var]) & (np.isnan(dataframe[yaxis_var]) == False)]
        else:
            dataframe = dataframe[(abs(dataframe[xaxis_var] - dataframe[yaxis_var]) > 0.001) & (np.isnan(dataframe[yaxis_var]) == False)]

    
    if type_value != "c":
        dataframe['diff_val'] = dataframe[xaxis_var] - dataframe[yaxis_var]
    else:
        dataframe['diff_val'] = np.nan

    if len(flags_only) > 0:
        if flags_only[0] == "f" and len(dataframe[dataframe['flagged_status'] == 1]) > 0:
            dataframe = dataframe[dataframe['flagged_status'] == 1]

    if aggreg_met == False:
        dataframe = pd.melt(dataframe, id_vars=['subsector', 'metcode', 'subid', 'yr', 'currmon'])
    elif aggreg_met == True:
        dataframe = pd.melt(dataframe, id_vars=['subsector', 'metcode', 'yr', 'currmon'])

    dataframe = dataframe[(dataframe['variable'] == xaxis_var) | (dataframe['variable'] == yaxis_var) | (dataframe['variable'] == "diff_val") | (dataframe['variable'] == "flagged_status")]

    if aggreg_met == False:
        dataframe['index'] = dataframe['metcode'] + dataframe['subid'].astype(str) + dataframe['subsector']
        dataframe['index_yr'] = dataframe['metcode'] + dataframe['subid'].astype(str) + dataframe['subsector'] + dataframe['yr'].astype(str) + "m" + dataframe['currmon'].astype(str)
    elif aggreg_met == True:
        dataframe['index'] = dataframe['metcode'] + dataframe['subsector']
        dataframe['index_yr'] = dataframe['metcode'] + dataframe['subsector'] + dataframe['yr'].astype(str) + "m" + dataframe['currmon'].astype(str)

    return dataframe

def create_scatter_plot(dataframe, xaxis_var, yaxis_var, type_value, curryr, currmon, sector_val, aggreg_met):
    
    if xaxis_var == "cons" or xaxis_var == "abs":
        if type_value == "c":
            x_data = dataframe[dataframe['variable'] == xaxis_var]['value'].astype(int)
            if xaxis_var == "abs":
                if sector_val == "apt":
                    x_data = x_data.round(0)
                else:    
                    x_data = x_data.round(-3)
        else:
            x_data = dataframe['index_yr'].unique()
        x_tick_format = ','
    else:
        if type_value == "c":
            x_data = dataframe[dataframe['variable'] == xaxis_var]['value']
        else:
            x_data = dataframe['index_yr'].unique()
        x_tick_format = ',.02%'
    
    if yaxis_var in ["cons", "abs", "rol_cons", "rol_abs", "sqcons", "sqabs", "avail10d", "sub_sur_totabs", "met_sur_totabs", "metsqabs", "metsqcons"]:
        if type_value == "c":
            y_data = dataframe[dataframe['variable'] == yaxis_var]['value'].astype(int)
        else:
            y_data = dataframe[dataframe['variable'] == 'diff_val']['value'].astype(int)
        if "abs" in yaxis_var or yaxis_var == "avail10d":
            if sector_val == "apt":
                y_data = y_data.round(0)
            else:    
                y_data = y_data.round(-3)
        y_tick_format = ','
    else:
        if type_value == "c":
            y_data = dataframe[dataframe['variable'] == yaxis_var]['value']
        else:
            y_data = dataframe[dataframe['variable'] == "diff_val"]['value']
        y_tick_format = ',.02%'
    

    if type_value == "c":
        axis_titles = {
                    "cons": "Construction",
                    "vac": "Vacancy Level",
                    "vac_chg": "Vacancy Change",
                    "abs": "Absorption",
                    "mrent": "Market Rent Level",
                    "G_mrent": "Market Rent Change",
                    "merent": "Effective Rent Level",
                    "G_merent": "Effective Rent Change",
                    "gap": "Gap Level",
                    "gap_chg": "Gap Change",
                    }
    elif type_value == "r":
        axis_titles = {
                    "rol_cons": "Cur Diff to ROL Construction",
                    "rol_vac_chg": "Curr Diff to ROL Vacancy Change",
                    "rol_abs": "Curr Diff to ROL Absorption",
                    "rol_G_mrent": "Curr Diff to ROL Market Rent Change",
                    "rol_G_merent": "Curr Diff to ROL Effective Rent Change",
                    "rol_gap_chg": "Curr Diff to ROL Gap Change",
                    }
    elif type_value == "q":
        axis_titles = {
                    "sqcons": "Curr Diff to Square Construction",
                    "metsqcons": "Curr Diff to Square Construction",
                    "sqvac_chg": "Curr Diff to Square Vacancy Change",
                    "metsqvacchg": "Curr Diff to Square Vacancy Change",
                    "sqabs": "Curr Diff to Square Absorption",
                    "metsqabs": "Curr Diff to Square Absorption",
                    "sq_Gmrent": "Curr Diff to Square Market Rent Change",
                    "metsq_Gmrent": "Curr Diff to Square Market Rent Change",
                    }
    elif type_value == "s":
        axis_titles = {
            "avail10d": "Curr Diff to Surveyed Absoprtion in MSQ",
            "dqren10d": "Curr Diff to Surveyed Rent Change in MSQ",
            "sub_sur_totabs": "Curr Diff to Total Surveyed Absorption",
            "met_sur_totabs": "Curr Diff to Total Surveyed Absorption",
            "sub_g_renx_mo_wgt": "Curr Diff to Total Surveyed Weighted Average Rent Change",
            "met_g_renx_mo_wgt": "Curr Diff to Total Surveyed Weighted Average Rent Change"
                    }

    if type_value == "c":
        x_axis_title = axis_titles[xaxis_var]
        vis_status = True
    else:
        if aggreg_met == False:
            x_axis_title = "Submarkets with Difference to Current"
        elif aggreg_met == True:
            x_axis_title = "Metros with Difference to Current"
        vis_status = False
    y_axis_title = axis_titles[yaxis_var]

    if type_value == "c":
        text_val = dataframe[dataframe['variable'] == yaxis_var]['index']
    else:
        text_val = False

    if len(dataframe) > 0 and aggreg_met == False:
        flagged_status = dataframe.copy()
        flagged_status = flagged_status[flagged_status['variable'] == 'flagged_status'][['index', 'value']]
        flagged_status = list(flagged_status['value'])
        flagged_status = [int(i) for i in flagged_status]
        if all(x == flagged_status[0] for x in flagged_status) == True:
            if flagged_status[0] == 1:
                color = 'red'
            else:
                color = 'blue'
            colorscale = []
        else:
            color = [0 if x == 0 else 1 for x in flagged_status]
            colorscale = [[0, 'blue'], [1, 'red']]
    else:
        color = 'blue'
        colorscale = []
    
    graph_dict = {
                    'data': [dict(
                        x = x_data,
                        y = y_data,
                        text = text_val,
                        customdata = dataframe[dataframe['variable'] == yaxis_var]['index'],
                        mode = 'markers',
                        marker={
                            'size': 15,
                            'opacity': 0.5,
                            'line': {'width': 0.5, 'color': 'white'},
                            'color': color,
                            'colorscale': colorscale
                                },
                            )],
                    'layout': dict(
                        xaxis={
                            'title': x_axis_title,
                            'tickformat': x_tick_format,
                            'title_standoff': 25,
                            'showticklabels': vis_status,
                            },
                        yaxis={
                            'title': y_axis_title,
                            'tickformat': y_tick_format,
                            'title_standoff': 25,
                            },
                        margin={'l': 80, 'b': 70, 't': 10, 'r': 0},
                        height=750,
                        hovermode='closest'
                                )
                    }

    return graph_dict

# This function sets the scale and tick distance for bar graphs based on the variable's max percentage of inventory
def set_bar_scale(data_in, sector_val, numer_list, denomer_list, type_value, curryr, currmon):
    
    data = data_in.copy()

    if "rol_cons" in numer_list:
        data['rol_inv'] = data['inv'] - (data['cons'] - data['rol_cons'])

    if type_value == "s":
        data = data[(data['yr'] == curryr) & (data['currmon'] == currmon)]
    
    if "dqren10d" not in numer_list and "sub_g_renx_mo_wgt" and "met_g_renx_mo_wgt" not in numer_list:
        val_list = []
        for x, y in zip(numer_list, denomer_list):
            data[x + "_per_inv"] = data[x] / data[y]
            val_list.append(list(data[x + "_per_inv"]))
            
        # Removing nans
        combined = []
        for x in val_list:
            x = [y for y in x if y == y]
            if len(x) > 0:
                combined += x
        combined = [float(i) for i in combined]

        max_val = max(combined)
        min_val = min(combined)
        if abs(min_val) > max_val:
            max_per_inv = min_val
        else:
            max_per_inv = max_val
        
        max_inv = max(list(data['inv']))
        
        if all(v == 0 for v in combined):
            if sector_val != "apt":
                range_list = [0, 400000]
                dtick = 100000
            elif sector_val == "apt":
                range_list = [0, 4000]
                dtick = 1000
        else:
            len_max = len(str(int((max_per_inv * max_inv))))
            max_round = round((max_per_inv * max_inv), (len_max - 1) * -1)

            if abs(max_per_inv) < 0.02:
                bound = max_round * 2.5
            elif abs(max_per_inv) >= 0.02 and max_per_inv < 0.05:
                bound = max_round * 2
            elif abs(max_per_inv) >= 0.05 and max_per_inv < 0.1:
                bound = max_round * 1.5
            else:
                bound = max_round * 1
  
            below_zero = 0
            above_zero = 0

            for i in combined:
                if float(i) < below_zero:
                    below_zero = float(i)
                if float(i) > above_zero:
                    above_zero = float(i)
            
            if below_zero == 0:
                range_list = [0, bound]
            if above_zero == 0:
                range_list = [bound, 0]
            elif abs(below_zero) > above_zero and above_zero != 0:
                len_rounder = len(str(int((above_zero * max_inv))))
                range_list = [bound, round(above_zero * max_inv, (len_rounder - 2) * -1)]
            elif above_zero >= abs(below_zero) and below_zero != 0:
                len_rounder = len(str(int((below_zero * max_inv))))
                range_list = [round(below_zero * max_inv, (len_rounder - 2) * -1), bound]

            if sector_val == "ind":
                limit_val = 20000
            elif sector_val == "off" or sector_val == "ret":
                limit_val = 10000
            elif sector_val == "apt":
                limit_val = 20
            dtick = max(abs((range_list[0] - range_list[1]) / 5), limit_val)
            len_dtick = len(str(int(dtick)))
            if dtick != limit_val:
                dtick = round(dtick, (len_dtick - 1) * -1)
        
        tick0 = range_list[0]

    else:
        data = data.reset_index()

        var_1 = data[numer_list[0]].loc[0]
        var_2 = data[numer_list[1]].loc[0]

        percentile_var_1 = data[numer_list[0] + "_perc"].loc[0]
        percentile_var_2 = data[numer_list[1] + "_perc"].loc[0]

        if percentile_var_1 >= percentile_var_2:
            use_percentile = percentile_var_1
            use_var = var_1
            other_var = var_2
        else:
            use_percentile = percentile_var_2
            use_var = var_2
            other_var = var_1

        if use_percentile >= 0.75:
            bound = use_var * (1 + 0.1)
        elif use_percentile >= 0.50 and use_percentile < 0.75:
            bound = use_var * (1 + 0.33)
        elif use_percentile >= 0.25 and use_percentile < 0.50:
            bound = use_var * (1 + 0.66)
        else:
            bound = use_var * (1 + 1)
        
        if var_1 * var_2 >= 0:
            if var_1 < 0:
                range_list = [bound, 0]
            else:
                range_list = [0, bound]
        else:
            if bound < 0 and abs(bound) > abs(other_var):
                range_list = [bound, other_var]
            elif bound > 0 and abs(bound) < abs(other_var):
                range_list = [other_var, bound]
        
        dtick = abs((range_list[0] - range_list[1])) / 5
        
        tick0 = range_list[0]

    return range_list, dtick, tick0

def set_y2_scale(data_in, type_filt, input_var, sector_val):
    data = data_in.copy()
    if type_filt == "sub":
        data = pd.melt(data, id_vars=['subsector', 'metcode', 'subid', 'yr', 'currmon'])
    elif type_filt == "met":
        data = pd.melt(data, id_vars=['subsector', 'metcode', 'yr', 'currmon'])
    elif type_filt == "nat":
        data = pd.melt(data, id_vars=['subsector', 'yr', 'currmon'])

    var = list(data[data['variable'] == input_var]['value'])

    if type_filt != "ts":
        if input_var == "vac":
            rol_var = list(data[(data['variable'] == 'rolsvac') & (data['value'] != '')]['value'])
            oob_var = list(data[(data['variable'] == 'vac_oob') & (data['value'] != '')]['value'])
        elif input_var == "mrent":
            rol_var = list(data[(data['variable'] == 'rolmrent') & (data['value'] != '')]['value'])
            oob_var = list(data[(data['variable'] == 'mrent_oob') & (data['value'] != '')]['value'])
        rol_var = [float(i) for i in rol_var]
        oob_var = [float(i) for i in oob_var]
        combined = var + rol_var + oob_var
    elif type_filt == "ts":
        combined = var

    # Need to set this in conditional that checks to see if combined has data, since there will be times when there are no subs with diff to pub for the selected variable
    if len(combined) > 0:
        max_var = max(combined)
        min_var = min(combined)

        if "vac" in input_var or "gap" in input_var:
            round_val = 2
            tick_0 = max(min_var - 0.01, 0)
            tick_1 = min(max_var + 0.01, 100)
        else:
            round_val = 2
            tick_0 = min_var - round((max_var - min_var) / 5, round_val)
            tick_1 = max_var + round((max_var - min_var) / 5, round_val)
        range_list = [round(tick_0, round_val), round(tick_1, round_val)]
        dtick = round((tick_1 - tick_0) / 5, round_val)

    else:
        if "vac" in input_var or "gap" in input_var:
            range_list = [0, 0.04]
            dtick = 0.01
            tick_0 = 0
        else:
            range_list = [4, 7]
            dtick = 1
            tick_0 = 4


    return range_list, dtick, tick_0

def sub_met_set_data(type_graph, fig, graph, var, color, l_name, tag, yaxis, display, group):

    if type_graph == "bar":
        fig.add_trace(
            go.Bar(
                x=list(graph['x_axis']),
                y=list(graph[graph['variable'] == var]['value']),
                name = l_name,
                marker_color = color,
                hovertemplate='%{x}, ' + '%{text:,}<extra></extra>',
                text = ['{}'.format(i) for i in list(graph[graph['variable'] == tag]['value'])],
                yaxis=yaxis,
                visible= display,
                legendgroup = group,
                    )
                )
    elif type_graph == "scatter":
        fig.add_trace(
            go.Scatter(
                x=list(graph['x_axis']),
                y=list(graph[graph['variable'] == var]['value']),
                line={'dash': 'dash', 'color': color},
                name = l_name,
                marker_color = color,
                hovertemplate='%{x}, ' + '%{text:.2%}<extra></extra>',
                text = ['{}'.format(i) for i in list(graph[graph['variable'] == tag]['value'])],
                yaxis=yaxis,
                visible= display,
                legendgroup = group,
                    )
                )
    return fig

def sub_met_set_layout(fig, title, cons_range_list, y2_title, vac_range_list, dtick, tick0, tick_format, bar_dtick):
    fig.update_layout(
        title={
            'text': title,
            'y':0.85,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        legend=dict(
            itemclick="toggleothers",
            itemdoubleclick="toggle",
            yanchor="bottom",
            y=-.5,
            xanchor="center",
            x=0.5
                ),
        legend_orientation="h",
        xaxis=dict(
            dtick=1,
            tickangle=-90
                ),
        yaxis=dict(
            title= "Construction",
            autorange=False,
            range=cons_range_list,
            dtick = bar_dtick,
            tickformat=',',
            side='left',
            showgrid=False,
                ),
        yaxis2=dict(
            title = y2_title,
            side='right',
            overlaying='y',
            tickformat= tick_format,
            range=vac_range_list,
            fixedrange=True,
            autorange=False,
            dtick=dtick,
            tick0=tick0
                )
            )

    return fig

def sub_met_graph_set_display(graph_in, var, type_filt):

    graph = graph_in.copy()
    
    # Check to see if the rol and oob values equal the current published value. If they do, nan them out so that the trace wont be displayed on the graph
    # Doesnt need to be equal, just no more than 10 basis points different
    check_list = ['rol_cons', 'rol_vac', 'rol_mrent', 'cons_oob', 'vac_oob', 'mrent_oob']
    var_list = ['cons', 'vac', 'mrent']
        
    for x, y in zip(check_list, var_list + var_list):
        graph[x] = np.where((abs(graph[x] - graph[y]) < 0.001), np.nan, graph[x])
    
    if graph[var].isnull().all() == True:
        if "oob" in var:
            display = False
        elif "rol" in var:
            display = 'legendonly'
    else:
        if "oob" in var:
            display = 'legendonly'
        elif "rol" in var:
            display = True

    return display

def sub_met_graphs(data, type_filt, curryr, currmon, sector_val):
    
    graph = data.copy()
    graph = graph[(graph['yr'] >= curryr - 1) | ((graph['yr'] == curryr - 2) & (graph['currmon'] >  0 + currmon))]

    vac_range_list, vac_dtick, vac_tick_0 = set_y2_scale(graph, type_filt, "vac", sector_val)
    rent_range_list, rent_dtick, rent_tick_0 = set_y2_scale(graph, type_filt, "mrent", sector_val)
    
    graph_copy = graph.copy()
    
    if type_filt == "sub":
        graph = pd.melt(graph, id_vars=['subsector', 'metcode', 'subid', 'yr', 'currmon'])
    elif type_filt == "met":
        graph = pd.melt(graph, id_vars=['subsector', 'metcode', 'yr', 'currmon'])
    elif type_filt == "nat":
        graph = pd.melt(graph, id_vars=['identity_us', 'yr', 'currmon'])

    for x in list(graph.columns):
        if "rol" in x:
            graph[x] = np.where((graph['yr'] == curryr) & (graph['currmon'] == currmon), np.nan, graph[x])
    for x in list(graph_copy.columns):
        if "rol" in x:
            graph_copy[x] = np.where((graph_copy['yr'] == curryr) & (graph_copy['currmon'] == currmon), np.nan, graph_copy[x])

    fig_vac = go.Figure()
    fig_rent = go.Figure()
    
    if type_filt == "sub":
        cons_range_list, bar_dtick, bar_tick0 = set_bar_scale(graph_copy, sector_val, ['cons', 'rol_cons', 'cons_oob'], ['inv', 'rol_inv', 'inv_oob'], False, curryr, currmon)
    else:
        cons_range_list, bar_dtick, bar_tick0 = set_bar_scale(graph_copy, sector_val, ['cons', 'rol_cons', 'cons_oob'], ['inv', 'rol_inv', 'inv'], False, curryr, currmon)

    cons_oob_display = sub_met_graph_set_display(graph_copy, "cons_oob", type_filt)
    cons_rol_display = sub_met_graph_set_display(graph_copy, "rol_cons", type_filt)
    vac_oob_display = sub_met_graph_set_display(graph_copy, "vac_oob", type_filt)
    vac_rol_display = sub_met_graph_set_display(graph_copy, "rol_vac", type_filt)

    rent_oob_display = sub_met_graph_set_display(graph_copy, "mrent_oob", type_filt)
    rent_rol_display = sub_met_graph_set_display(graph_copy, "rol_mrent", type_filt)

    if sector_val == "apt":
        rent_tick_format = '.0f'
    else:
        rent_tick_format = '.2f'

    graph['x_axis'] = graph['yr'].astype(str) + "m" + graph['currmon'].astype(str)

    # Note call sub_met_set_data for the pub trace last, so it will be the one to appear if overlapping with another trace
    fig_vac = sub_met_set_data("bar", fig_vac, graph, "cons", 'mediumseagreen', 'cons', 'cons', 'y1', True, 'act')
    fig_vac = sub_met_set_data("bar", fig_vac, graph, "rol_cons", 'palevioletred', 'rol_cons', 'rol_cons', 'y1', cons_rol_display, 'rol')
    fig_vac = sub_met_set_data("scatter", fig_vac, graph, "rol_vac", 'red', 'rol_vac', 'rol_vac_chg', 'y2', vac_rol_display, 'rol')
    fig_vac = sub_met_set_data("bar", fig_vac, graph, "cons_oob", 'lightskyblue', 'cons_oob', 'cons_oob', 'y1', cons_oob_display, 'oob')
    fig_vac = sub_met_set_data("scatter", fig_vac, graph, "vac_oob", 'blue', 'vac_oob', 'vac_chg_oob', 'y2', vac_oob_display, 'oob')
    fig_vac = sub_met_set_data("scatter", fig_vac, graph, "vac", 'darkgreen', 'vac', 'vac_chg', 'y2', True, 'act')

    fig_rent = sub_met_set_data("bar", fig_rent, graph, "cons", 'mediumseagreen', 'cons', 'cons', 'y1', True, 'act')
    fig_rent = sub_met_set_data("bar", fig_rent, graph, "rol_cons", 'palevioletred', 'rol_cons', 'rol_cons', 'y1', cons_rol_display, 'rol')
    fig_rent = sub_met_set_data("scatter", fig_rent, graph, "rol_mrent", 'red', 'rol_mrent', 'rol_G_mrent', 'y2', rent_rol_display, 'rol')
    fig_rent = sub_met_set_data("bar", fig_rent, graph, "cons_oob", 'lightskyblue', 'cons_oob', 'cons_oob', 'y1', cons_oob_display, 'oob')
    fig_rent = sub_met_set_data("scatter", fig_rent, graph, "mrent_oob", 'blue', 'mrent_oob', 'G_mrent_oob', 'y2', rent_oob_display, 'oob')
    fig_rent = sub_met_set_data("scatter", fig_rent, graph, "mrent", 'darkgreen', 'mrent', 'G_mrent', 'y2', True, 'act')
       
            
    fig_vac = sub_met_set_layout(fig_vac, "Vacancy", cons_range_list, "Vacancy Level", vac_range_list, vac_dtick, vac_tick_0, ',.01%', bar_dtick)
    fig_rent = sub_met_set_layout(fig_rent, "Market Rent", cons_range_list, "Rent Level", rent_range_list, rent_dtick, rent_tick_0, rent_tick_format, bar_dtick)

    # if type_filt == "nat":
    #     file_path_temp = "{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/OutputFiles/".format(get_home(), sector_val, curryr, currmon)
    #     fig_vac.write_html(file_path_temp + 'national_vac_series.html')
    #     fig_rent.write_html(file_path_temp + 'national_rent_series.html')

    return fig_vac, fig_rent

def set_ts_scatter(fig, data, level_var, chg_var, bar_color, line_color, curryr, currmon):

    fig.add_trace(
        go.Bar(
            x=list(data['x_axis'].unique()),
            y=list(data[data['variable'] == 'cons']['value']),
            marker_color=bar_color,
            hovertemplate='%{x}: ' + '%{text:,}<extra></extra>',
            text = ['{}'.format(i) for i in list(data[data['variable'] == 'cons']['value'])],
            yaxis='y1',
                )
            )
    fig.add_trace(
        go.Scatter(
            x=list(data['x_axis'].unique()),
            y=list(data[data['variable'] == level_var]['value']),
            line={'dash': 'dash', 'color': line_color},
            hovertemplate = '%{x}: ' + '%{text:.2%}<extra></extra>',
            text = ['{}'.format(i) for i in list(data[data['variable'] == chg_var]['value'])],
            cliponaxis= False,
            yaxis='y2'
                    )
                )

    return fig

def set_ts_bar(fig, data_in, var_1, var_2, curryr, currmon, type_value, bar_color_1, bar_color_2, sector_val):
    
    data = data_in.copy()
    if type_value == "s":
        data = data[(data['yr'] == curryr) & (data['currmon'] == currmon)]

    if var_2 == "dqren10d" or var_2 == "sub_g_renx_mo_wgt":
        template = '%{x}: ' + '%{text:.2%}<extra></extra>'
    else:
        template = '%{x}: ' + '%{text:,}<extra></extra>'

    if type_value == "s":
        y_data_s = list(data[data['variable'] == var_2]['value'])
        if "abs" in var_2 or "avail10d" in var_2:
            if sector_val == "apt":
                y_data_s =  [round(x,0) for x in y_data_s]
            else:
                y_data_s =  [round(x,-3) for x in y_data_s]

    if type_value == "s":
        width_bar = 0.1
    else:
        width_bar = 0.75

    fig.add_trace(
        go.Bar(
            x=list(data['x_axis'].unique()),
            y=list(data[data['variable'] == var_1]['value']),
            name = "Current",
            marker_color = bar_color_1,
            width = width_bar,
            hovertemplate = template,
            text = ['{}'.format(i) for i in list(data[data['variable'] == var_1]['value'])],
            cliponaxis= False,
                    ),
                )
    
    if type_value == "s":
        fig.add_trace(
            go.Bar(
                x=list(data['x_axis'].unique()),
                y=y_data_s,
                name = "Survey",
                marker_color = bar_color_2,
                width = width_bar,
                hovertemplate = template,
                text = ['{}'.format(i) for i in y_data_s],
                cliponaxis= False,
                        ),
                    )
   
    return fig

def set_ts_layout(fig, axis_var, identity, y_tick_range, dtick, tick_0, curryr, currmon, chart_type, range_list, sector_val, type_value, bar_dtick, bar_tick0):
    
    graph_titles = {
                    "cons": "Construction",
                    "vac_chg": "Vacancy Change",
                    "abs": "Absorption",
                    "G_mrent": "Market Rent Change",
                    "G_merent": "Effective Rent Change",
                    "gap_chg": "Gap Change",
                    "rol_cons": "ROL Construction",
                    "rol_vac_chg": "ROL Vacancy Change",
                    "rol_abs": "ROL Absorption",
                    "rol_G_mrent": "ROL Market Rent Change",
                    "rol_G_merent": "ROL Effective Rent Change",
                    "rol_gap_chg": "ROL Gap Change",
                    "sqcons": "Square Construction",
                    "metsqcons": "Square Construction",
                    "sqvac_chg": "Square Vacancy Change",
                    "metsqvacchg": "Square Vacancy Change",
                    "sqabs": "Square Absorption",
                    "metsqabs": "Square Absorption",
                    "sq_Gmrent": "Square Market Rent Change",
                    "metsq_Gmrent": "square Market Rent Change",
                    "avail10d": "MSQ Surveyed Abs VS Curr Pub Abs",
                    "dqren10d": "MSQ Surveyed Rent Chg VS Curr Pub Rent Chg",
                    "sub_sur_totabs": "Surveyed Abs VS Curr Pub Abs",
                    "met_sur_totabs": "Surveyed Abs VS Curr Pub Abs",
                    "sub_g_renx_mo_wgt": "Surveyed WTD Avg Rent Chg VS Current Published Rent Chg",
                    "met_g_renx_mo_wgt": "Surveyed WTD Avg Rent Chg VS Current Published Rent Chg"
                    }

    axis_titles = {
                    "cons": "Construction",
                    "vac_chg": "Vacancy Level",
                    "abs": "Absorption",
                    "G_mrent": "Market Rent Level",
                    "G_merent": "Effective Rent Level",
                    "gap_chg": "Gap",
                    "rol_cons": "ROL Construction",
                    "rol_vac_chg": "ROL Vacancy Level",
                    "rol_abs": "ROL Absorption",
                    "rol_G_mrent": "ROL Market Rent Level",
                    "rol_G_merent": "ROL Effective Rent Level",
                    "rol_gap_chg": "ROL Gap",
                    "sqcons": "Square Construction",
                    "metsqcons": "Square Construction",
                    "sqvac_chg": "Square Vacancy Level",
                    "metsqvacchg": "Square Vacancy Level",
                    "sqabs": "Square Absorption",
                    "metsqabs": "Square Absorption",
                    "sq_Gmrent": "Square Market Rent Level",
                    "metsq_Gmrent": "Square Market Rent Level",
                    "avail10d": "Absorption",
                    "dqren10d": "Market Rent Change",
                    "sub_sur_totabs": "Absorption",
                    "met_sur_totabs": "Absorption",
                    "sub_g_renx_mo_wgt": "Market Rent Change",
                    "met_g_renx_mo_wgt": "Market Rent Change"
                    }

    title_name = '<b>{}</b><br>{}'.format(identity, graph_titles[axis_var])

    if "cons" in axis_var or "abs" in axis_var or "avail10d" in axis_var or "sub_sur_totabs" in axis_var:
        tick_format = ','
    elif "mrent" in axis_var or "merent" in axis_var:
        if sector_val == "apt":
            tick_format = '.0f'
        else:
            tick_format = '.2f'
    else:
        tick_format = ',.01%'

    yaxis_title = axis_titles[axis_var]

    if type_value != "s":
        tick_angle = -90
    else:
        tick_angle = 0
    
    fig.update_layout(
        title={
            'text': title_name,
            'y':0.95,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        height=350,
        margin={'l': 70, 'b': 30, 'r': 10, 't': 70, 'pad': 20},
        yaxis=dict(
            showgrid=False
                    ),
        xaxis=dict(
            title = "Period",
            title_font = dict(size=12),
            dtick = 1,
            tickangle = tick_angle,
                    )
        )
    
    if type_value == "s":
        fig.update_layout(showlegend=True, legend_orientation = "h", legend=dict(yanchor="bottom", xanchor="center", y=-0.30, x = 0.20))
    else:
        fig.update_layout(showlegend=False)
    
    if chart_type == "Bar":
        if range_list != False:
            fig.update_layout(yaxis=dict(range=range_list, autorange=False, title=yaxis_title, tickformat=tick_format, side='left', dtick=bar_dtick, tick0 = bar_tick0))
    else:
        fig.update_layout(
                yaxis2=dict(
                title = yaxis_title,
                title_font = dict(size=12),
                tickformat = tick_format,
                range = y_tick_range,
                dtick = dtick,
                tick0=tick_0,
                fixedrange = True,
                autorange=False,
                side='right', 
                overlaying='y',
                        )
                    )
        fig.update_layout(yaxis=dict(title='Construction', tickformat=',', side='left'))
        if range_list != False:
            fig.update_layout(yaxis=dict(range=range_list, autorange=False, dtick=bar_dtick))

    return fig          

# Function that returns the most recent input value updated by the user
def get_input_id():
    ctx = dash.callback_context

    if not ctx.triggered:
        button_id = 'No update yet'
    else:
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]

   
    return button_id
  
# This function reads and writes dataframes to pickle files
def use_pickle(direction, file_name, dataframe, curryr, currmon, sector_val):
    
    if "decision_log" in file_name:
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/OutputFiles/{}.pickle".format(get_home(), sector_val, str(curryr), str(currmon), file_name))
        
        if direction == "in":
            data = pd.read_pickle(file_path)
            return data
        elif direction == "out":
            dataframe.to_pickle(file_path)
    elif "original_flags" in file_name:
        path_in = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/OutputFiles/{}.pickle".format(get_home(), sector_val, str(curryr), str(currmon), file_name))
        path_out = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/OutputFiles/{}_all_data.csv".format(get_home(), sector_val, str(curryr), str(currmon), sector_val))
        orig_flags = pd.read_pickle(path_in)
        orig_flags.reset_index().set_index('identity').to_csv(path_out, na_rep='')

        r = re.compile("^._flag*")
        flag_cols = list(filter(r.match, orig_flags.columns))
        cols_to_keep = ['identity', 'subsector', 'metcode', 'subid', 'yr', 'currmon'] + flag_cols
        flags_only = orig_flags[cols_to_keep]
        path_out = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/OutputFiles/{}_original_flags.csv".format(get_home(), sector_val, str(curryr), str(currmon), sector_val))
        flags_only.reset_index().set_index('identity').to_csv(path_out, na_rep='')

    else:
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}.pickle".format(get_home(), file_name))
        if direction == "in":
            data = pd.read_pickle(file_path)
            return data
        elif direction == "out":
            dataframe.to_pickle(file_path)

#@Timer("Update Decision Log")
def update_decision_log(decision_data, data, drop_val, sector_val, curryr, currmon, user, button, flag_name):
    if button == "submit":
        # Identify where the trend series has changed for key variables
        decision_data_test = decision_data.copy()
        decision_data_test = decision_data_test[decision_data_test['identity'] == drop_val]
        
        drop_list = []
        for x in list(decision_data_test.columns):
            if "new" in x:
                drop_list.append(x)
        decision_data_test = decision_data_test.drop(drop_list, axis=1)
        for x in list(decision_data_test.columns):
            if "oob" in x:
                decision_data_test = decision_data_test.rename(columns={x: x[:-4]})
        update_data = data.copy()
        update_data = update_data[update_data['identity'] == drop_val]
        if sector_val != "ind":
            update_data = update_data[['identity', 'subsector', 'metcode', 'subid', 'yr', 'currmon', 'cons', 'conv', 'demo', 'vac', 'abs', 'G_mrent', 'G_merent', 'gap', 'inv', 'avail', 'mrent', 'merent', 'vac_chg']]
        else:
            update_data = update_data[['identity', 'subsector', 'metcode', 'subid', 'yr', 'currmon', 'cons', 'vac', 'abs', 'G_mrent', 'G_merent', 'gap', 'inv', 'avail', 'mrent', 'merent', 'vac_chg']]
        decision_data_test = decision_data_test.drop(['i_user', 'c_user', 'v_user', 'g_user', 'e_user'], axis=1)
        update_data['vac'] = round(update_data['vac'], 3)
        update_data['vac_chg'] = round(update_data['vac_chg'], 3)
        update_data['G_mrent'] = round(update_data['G_mrent'], 3)
        update_data['G_merent'] = round(update_data['G_merent'], 3)
        update_data['gap'] = round(update_data['gap'], 3)
        update_data['mrent'] = round(update_data['mrent'], 2)
        update_data['merent'] = round(update_data['merent'], 2)
        decision_data_test['vac'] = round(decision_data_test['vac'], 3)
        decision_data_test['vac_chg'] = round(decision_data_test['vac_chg'], 3)
        decision_data_test['G_mrent'] = round(decision_data_test['G_mrent'], 3)
        decision_data_test['G_merent'] = round(decision_data_test['G_merent'], 3)
        decision_data_test['gap'] = round(decision_data_test['gap'], 3)
        decision_data_test['mrent'] = round(decision_data_test['mrent'], 2)
        decision_data_test['merent'] = round(decision_data_test['merent'], 2)
        test = update_data.ne(decision_data_test)
        update_data = update_data[test]

        # Update user log with username that made the edit
        update_data['i_user'] = np.nan
        update_data['c_user'] = np.nan
        update_data['v_user'] = np.nan
        update_data['g_user'] = np.nan
        update_data['e_user'] = np.nan
        for index, row in update_data.iterrows():
            if math.isnan(row['inv']) == False:
                update_data.loc[index, 'i_user'] = user
            if math.isnan(row['cons']) == False:
                update_data.loc[index, 'c_user'] = user
            if sector_val != "ind":
                if math.isnan(row['conv']) == False:
                    update_data.loc[index, 'i_user'] = user
                if math.isnan(row['demo']) == False:
                    update_data.loc[index, 'i_user'] = user
            if math.isnan(row['vac']) == False or math.isnan(row['vac_chg']) == False or math.isnan(row['avail']) == False or math.isnan(row['abs']) == False:
                update_data.loc[index, 'v_user'] = user
            if math.isnan(row['mrent']) == False or math.isnan(row['G_mrent']) == False:
                update_data.loc[index, 'g_user'] = user
            if  math.isnan(row['merent']) == False or math.isnan(row['G_merent']) == False or math.isnan(row['gap']) == False:
                update_data.loc[index, 'e_user'] = user
        
        # Fill in the new values in a trunc dataframe
        decision_data_fill = decision_data.copy()
        decision_data_fill = decision_data_fill[decision_data_fill['identity'] == drop_val]
        no_change_list = ['identity', 'subsector', 'metcode', 'subid', 'yr', 'currmon', 'i_user', 'c_user', 'v_user', 'g_user', 'e_user']
        for x in list(decision_data_fill.columns):
            if "new" not in x:
                if x not in no_change_list:
                    decision_data_fill = decision_data_fill.drop([x], axis=1)
            elif "new" in x:
                decision_data_fill = decision_data_fill.rename(columns={x: x[:-4]})
        # Since nan values wont replace non nan values when using combine first, replace them all with a crazy number that wont match a real value, and then replace back to nan after combined
        all_cols = list(update_data.columns)
        fill_list = [x for x in all_cols if x not in no_change_list]
        update_data[fill_list] = update_data[fill_list].fillna(9999999999999999)
        update_data[['i_user', 'c_user', 'v_user', 'g_user', 'e_user']] = update_data[['i_user', 'c_user', 'v_user', 'g_user', 'e_user']].fillna("temp")
        update_data = update_data.combine_first(decision_data_fill)
        for x in fill_list:
            update_data[x] = np.where(update_data[x] == 9999999999999999, np.nan, update_data[x])
        for x in ['i_user', 'c_user', 'v_user', 'g_user', 'e_user']:
            update_data[x] = np.where(update_data[x] == "temp", np.nan, update_data[x])
        for x in list(update_data.columns):
            if x not in no_change_list and "oob" not in x:
                update_data = update_data.rename(columns={x: x + "_new"})

        # Because there are slight rounding differences, check if there is an actual change to the level var, and null out diff if no change
        for index, row in update_data.iterrows():
            if math.isnan(row['avail_new']) == True:
                update_data.loc[index, 'vac_new'] = np.nan 
                update_data.loc[index, 'vac_chg_new'] = np.nan 
                update_data.loc[index, 'abs'] = np.nan 
                update_data.loc[index, 'v_user'] = np.nan 
            if math.isnan(row['mrent_new']) == True:
                update_data.loc[index, 'G_mrent_new'] = np.nan 
                update_data.loc[index, 'g_user'] = np.nan 
            if math.isnan(row['merent_new']) == True:
                update_data.loc[index, 'G_merent_new'] = np.nan 
                update_data.loc[index, 'gap_new'] = np.nan 
                update_data.loc[index, 'e_user'] = np.nan 
        
        # Replace the new values in the "new" columns in a trunc dataframe that also has the oob values
        decision_data_replace = decision_data.copy()
        decision_data_replace = decision_data_replace[decision_data_replace['identity'] == drop_val]
        decision_data_replace = decision_data_replace.reset_index()
        update_data = update_data.reset_index()
        for x in drop_list + ['i_user', 'c_user', 'v_user', 'g_user', 'e_user']:
            decision_data_replace[x] = update_data.loc[update_data['identity_row'] == decision_data_replace['identity_row'], x]
        decision_data_replace = decision_data_replace.set_index('identity_row')

        # Append the updated decision for the "new" columns from the trunc dataframe we just created to the rest of the identities in the log, and output the updated log
        decision_data_update = decision_data.copy()
        decision_data_update = decision_data_update[decision_data_update['identity'] != drop_val]
        decision_data_update = decision_data_update.append(decision_data_replace)
        decision_data_update.sort_values(by=['subsector', 'metcode', 'subid', 'yr', 'currmon'], inplace = True)
    
    elif button == "skip":
        decision_data_update = decision_data.copy()
        if decision_data_update['skipped'].loc[drop_val + str(curryr) + str(currmon)] == '':
            decision_data_update.loc[drop_val + str(curryr) + str(currmon), 'skipped'] = flag_name
            decision_data_update.loc[drop_val + str(curryr) + str(currmon), 'skip_user'] = user
        else:
            decision_data_update.loc[drop_val + str(curryr) + str(currmon), 'skipped'] = decision_data_update['skipped'].loc[drop_val + str(curryr) + str(currmon)] + ", " + flag_name
            decision_data_update.loc[drop_val + str(curryr) + str(currmon), 'skip_user'] = decision_data_update['skip_user'].loc[drop_val + str(curryr) + str(currmon)] + ", " + user

    return decision_data_update

# This function filters out submarkets flagged for a specific flag chosen by the user on the Home tab, and creates the necessary table and styles for display
def filter_flags(dataframe_in, drop_flag):

    flag_filt = dataframe_in.copy()
    if "rol" in drop_flag:
        flag_filt[drop_flag + "_test"] = flag_filt.groupby('identity')[drop_flag].transform('sum')
        flag_filt['skip_test'] = flag_filt['flag_skip'].str.contains(drop_flag)
        flag_filt['skip_test'] = np.where(flag_filt['skip_test'] == True, 1, 0)
        flag_filt['skip_test'] = flag_filt.groupby('identity')['skip_test'].transform('sum')
        flag_filt[drop_flag] = np.where((flag_filt[drop_flag + "_test"] > 0) & (flag_filt['skip_test'] > 0), 0, flag_filt[drop_flag])

    flag_filt = flag_filt[[drop_flag, 'identity', 'flag_skip']]
    flag_filt = flag_filt[(flag_filt[drop_flag] > 0)]

    if drop_flag == "c_flag_sqdiff":
        flag_filt[drop_flag] = flag_filt[drop_flag].rank(ascending=False, method='min')
    
    if len(flag_filt) > 0:
        has_skip = flag_filt['flag_skip'].str.contains(drop_flag, regex=False)
        flag_filt['has_skip'] = has_skip
        flag_filt = flag_filt[flag_filt['has_skip'] == False]
        flag_filt = flag_filt.drop(['flag_skip', 'has_skip'], axis=1)
        flag_filt['Total Flags'] = flag_filt[drop_flag].count()
        temp = flag_filt.copy()
        temp = temp.reset_index()
        temp = temp.head(1)
        temp = temp[['Total Flags']]
        flag_filt_title = "Total Flags: " + str(temp['Total Flags'].loc[0])
        flag_filt.sort_values(by=['identity', drop_flag], ascending=[True, True], inplace=True)
        flag_filt = flag_filt.drop_duplicates('identity')
        flag_filt = flag_filt[['identity', drop_flag]]
        flag_filt = flag_filt.rename(columns={'identity': 'Submarkets With Flag', drop_flag: 'Flag Ranking'})
        flag_filt.sort_values(by=['Flag Ranking'], inplace=True)
    elif len(flag_filt) == 0:
        flag_filt_title =  'Total Flags: 0'
        data_fill = {'Submarkets With Flag': ['No Submarkets Flagged'],
                'Flag Ranking': [0]}
        flag_filt = pd.DataFrame(data_fill, columns=['Submarkets With Flag', 'Flag Ranking'])

    flag_filt_display = {'display': 'block', 'padding-top': '20px'}

    if len(flag_filt) >= 10:
        flag_filt_style_table = {'height': '350px', 'overflowY': 'auto'}
    else:
        flag_filt_style_table = {'height': '350px', 'overflowY': 'visible'}

    return flag_filt, flag_filt_style_table, flag_filt_display, flag_filt_title


#This function produces the items that need to be returned by the update_data callback if the user has just loaded the program
def first_update(data_init, file_used, sector_val, orig_cols, curryr, currmon):

    data_init = calc_stats(data_init, curryr, currmon, 0, sector_val)
    
    threshs = data_init.copy()
    threshs['v_diff'] = abs(threshs['met_v_scov_percentile'] - 0.30)
    threshs['r_diff'] = abs(threshs['met_r_scov_percentile'] - 0.30)
    v_threshold = min(max(threshs.sort_values(by=['v_diff'], ascending=[True]).reset_index().loc[0]['met_sur_v_cov_perc'], 0.04), 0.1)
    r_threshold = min(max(threshs.sort_values(by=['r_diff'], ascending=[True]).reset_index().loc[0]['met_sur_r_cov_perc'], 0.04), 0.1)
    
    data = data_init.copy()
    if file_used == "oob":
        data['deep_hist_first_period'] = np.where((data['yr'] == 2008) & (data['yr'].shift(-1) == 2009), 1, 0)
        # Can drop the deep deep history prior to 2009, because that doesnt get edited, but do need to save it so it can be appended back for the final econ file
        deep_hist = data.copy()
        deep_hist = deep_hist[(deep_hist['yr'] < 2008) | ((deep_hist['deep_hist_first_period'] == 0) & (deep_hist['yr'] == 2008))]
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/OutputFiles/{}_deep_hist.pickle".format(get_home(), sector_val, str(curryr), str(currmon), sector_val))
        deep_hist.to_pickle(file_path)
    
        data = data[(data['yr'] > 2008) | ((data['yr'] == 2008) & (data['deep_hist_first_period'] == 1))]
        data = data.drop(['deep_hist_first_period'], axis=1)
    
    data = calc_flags(data, curryr, currmon, sector_val, v_threshold, r_threshold)
    
    r = re.compile("^._flag*")
    flag_cols = list(filter(r.match, data.columns))


    rank_data_met = data.copy()
    rank_data_met = summarize_flags_ranking(rank_data_met, sector_val, 'met', flag_cols)
    rank_data_met = rank_data_met.rename(columns={'subsector': 'Subsector', 'metcode': 'Metcode'})
    rank_data_sub = data.copy()
    rank_data_sub = summarize_flags_ranking(rank_data_sub, sector_val, 'sub', flag_cols)
    rank_data_sub = rank_data_sub.rename(columns={'subsector': 'Subsector', 'metcode': 'Metcode', 'subid': 'Subid'})

    sum_data = data.copy()
    filt_cols = flag_cols + ['identity', 'identity_us', 'identity_met', 'subid', 'yr', 'currmon', 'subsector', 'metcode', 'curr_tag']
    sum_data = sum_data[filt_cols]

    # Create the national rollups of currmon survey data for rent and vac by subsector
    nat_data_rent = data.copy()
    nat_data_rent = nat_data_rent[(nat_data_rent['yr'] == curryr) & (nat_data_rent['currmon'] == currmon)]
    nat_data_rent['sum_rentdrops'] = nat_data_rent.groupby('subsector')['rentdrops'].transform('sum')
    nat_data_rent['sum_rentflats'] = nat_data_rent.groupby('subsector')['rentflats'].transform('sum')
    nat_data_rent['sum_rentincrs'] = nat_data_rent.groupby('subsector')['rentincrs'].transform('sum')
    nat_data_rent = nat_data_rent.drop(['rentdrops', 'rentflats', 'rentincrs'], axis=1)
    nat_data_rent.rename(columns={'sum_rentdrops': 'rentdrops', 'sum_rentflats': 'rentflats', 'sum_rentincrs': 'rentincrs'}, inplace=True)
    nat_data_rent = nat_data_rent.drop_duplicates('subsector')
    nat_data_rent = nat_data_rent[['subsector', 'us_g_renx_mo_wgt', 'us_avg_mos_to_last_rensur', 'us_sur_r_cov_perc', 'rentdrops', 'rentflats', 'rentincrs']]
    if sector_val == "apt" or sector_val == "off":
        nat_data_rent = nat_data_rent.drop(['subsector'], axis=1)
    nat_data_rent.rename(columns={'us_avg_mos_to_last_rensur' :'us_mos_lrsurv'}, inplace=True)

    nat_data_vac = data.copy()
    nat_data_vac = nat_data_vac[(nat_data_vac['yr'] == curryr) & (nat_data_vac['currmon'] == currmon)]
    nat_data_vac['sum_vacdrops'] = nat_data_vac.groupby('subsector')['vacdrops'].transform('sum')
    nat_data_vac['sum_vacflats'] = nat_data_vac.groupby('subsector')['vacflats'].transform('sum')
    nat_data_vac['sum_vacincrs'] = nat_data_vac.groupby('subsector')['vacincrs'].transform('sum')
    nat_data_vac['us_sur_totabs'] = nat_data_vac.groupby('subsector')['sub_sur_totabs'].transform('sum')
    nat_data_vac = nat_data_vac.drop(['vacdrops', 'vacflats', 'vacincrs'], axis=1)
    nat_data_vac.rename(columns={'sum_vacdrops': 'vacdrops', 'sum_vacflats': 'vacflats', 'sum_vacincrs': 'vacincrs'}, inplace=True)
    nat_data_vac = nat_data_vac.drop_duplicates('subsector')
    nat_data_vac = nat_data_vac[['subsector', 'us_sur_totabs', 'us_avg_mos_to_last_vacsur', 'us_sur_v_cov_perc', 'vacdrops', 'vacflats', 'vacincrs']]
    if sector_val == "apt" or sector_val == "off":
        nat_data_vac = nat_data_vac.drop(['subsector'], axis=1)
    nat_data_vac.rename(columns={'us_avg_mos_to_last_vacsur' :'us_mos_lvsurv'}, inplace=True)

    if file_used == "oob":
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/OutputFiles/{}_original_flags.pickle".format(get_home(), sector_val, str(curryr), str(currmon), sector_val))
        data.to_pickle(file_path)
        print("Orig Flags Saved")

    return data, rank_data_met, rank_data_sub, sum_data, nat_data_rent, nat_data_vac, v_threshold, r_threshold, flag_cols

#@Timer("Submit Update")
#This function produces the outputs needed for the update_data callback if the submit button is clicked
def submit_update(data, shim_data, sector_val, orig_cols, user, drop_val, expand, flag_list, skip_list, curryr, currmon, subsequent_chg):

    if sector_val != "ind":
        shim_cols = ['inv', 'cons', 'conv', 'demo', 'avail', 'mrent', 'merent']
    else:
        shim_cols = ['inv', 'cons', 'avail', 'mrent', 'merent']
    
    for col in shim_cols:
        shim_data[col] = np.where(shim_data[col] == '', np.nan, shim_data[col])

    if shim_data[shim_cols].isnull().values.all() == True:
        no_shim = True
    else:
        no_shim = False

    if no_shim == True and len(skip_list) == 0:
        message = "You did not enter any changes."
        message_display = True
    else:
        message = ''
        message_display = False

    if message_display == False:

        data_orig = data.copy()
        data_orig = data_orig[(data_orig['identity'] == drop_val)]
        if "trunc" in expand and "full" not in expand:
            data_orig = data_orig[(data_orig['yr'] >= curryr - 3) | ((data_orig['yr'] == curryr - 4) & (data_orig['currmon'] == 12))]
        data_orig = data_orig[['currmon', 'yr'] + shim_cols]
        shim_data = shim_data[['currmon', 'yr'] + shim_cols]
        
        if no_shim == False:
            data, has_diff = get_diffs(shim_data, data_orig, data, drop_val, curryr, currmon, sector_val, "submit", subsequent_chg)
        else:
            has_diff = 0

        # Update decision log with new values entered via shim
        if has_diff == 1 or len(skip_list) > 0:
            decision_data = use_pickle("in", "decision_log_" + sector_val, False, curryr, currmon, sector_val)
        if has_diff == 1:      
            update_decision_log(decision_data, data, drop_val, sector_val, curryr, currmon, user, "submit", False)

        if flag_list[0] != "v_flag" and len(skip_list) > 0:
            test = data.loc[drop_val + str(curryr) + str(currmon)]['flag_skip']
            test = test.split(",")
            test = [x.strip(' ') for x in test]
            for flag in skip_list:
                if flag not in test:
                    if data.loc[drop_val + str(curryr) + str(currmon), 'flag_skip'] == '':
                        data.loc[drop_val + str(curryr) + str(currmon), 'flag_skip'] = flag
                    else:
                        data.loc[drop_val + str(curryr) + str(currmon), 'flag_skip'] += ", " + flag
                    
                    decision_data = update_decision_log(decision_data, data, drop_val, sector_val, curryr, currmon, user, "skip", flag)

        if has_diff == 1 or len(skip_list) > 0:
            use_pickle("out", "decision_log_" + sector_val, decision_data, curryr, currmon, sector_val)

            data_save = data.copy()
            file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/OutputFiles/{}_mostrecentsave.pickle".format(get_home(), sector_val, str(curryr), str(currmon), sector_val))
            keep_cols = orig_cols
            data_save = data_save[keep_cols]
            data_save.to_pickle(file_path)
        

    preview_data = pd.DataFrame()

    shim_data[shim_cols] = np.nan

    return data, preview_data, shim_data, message, message_display

def test_resolve_flags(preview_data, drop_val, curryr, currmon, sector_val, orig_flag_list, skip_list, v_threshold, r_threshold, flag_cols):
        
    resolve_test = preview_data.copy()
    resolve_test = drop_cols(resolve_test)
    resolve_test = calc_stats(resolve_test, curryr, currmon, 1, sector_val)
    resolve_test = resolve_test[resolve_test['identity'] == drop_val]
    
    test_flag_list = [x for x in orig_flag_list if x not in skip_list]
    
    resolve_test = calc_flags(resolve_test, curryr, currmon, sector_val, v_threshold, r_threshold)

    resolve_test = resolve_test[flag_cols]
    resolve_test['sum_flags'] = resolve_test[flag_cols].sum(axis=1)
    resolve_test = resolve_test[resolve_test['sum_flags'] > 0]

    if len(resolve_test) > 0:
        resolve_test = resolve_test[resolve_test.columns[(resolve_test != 0).any()]]
        resolve_test = resolve_test.drop(['sum_flags'], axis=1)
        flags_remaining = list(resolve_test.columns)

        flags_resolved = [x for x in test_flag_list if x not in flags_remaining and x not in skip_list]
        flags_unresolved = [x for x in test_flag_list if x in flags_remaining and x not in skip_list]
        new_flags = [x for x in flags_remaining if x not in orig_flag_list and x not in skip_list]
    else:
        flags_resolved = test_flag_list
        flags_unresolved = []
        new_flags = []

    return flags_resolved, flags_unresolved, new_flags

# # This function produces the outputs needed for the update_data callback if the preview button is clicked
#@Timer("Preview Update")
def preview_update(data, shim_data, sector_val, preview_data, drop_val, expand, curryr, currmon, subsequent_chg, orig_flag_list, skip_list, v_threshold, r_threshold, flag_cols):  
    

    # At this point, will just always allow the button to be clicked, even if there are no edits entered, as want to allow the user to undo a previewed shim. Can think about a way to test if this is an undo vs a first time entry, but small potatoes as will only marginally increase speed
    message = ''
    message_display = False

    if message_display == False:

        if sector_val != "ind":
            shim_cols = ['inv', 'cons', 'conv', 'demo', 'avail', 'mrent', 'merent']
        else:
            shim_cols = ['inv', 'cons', 'avail', 'mrent', 'merent']

        for col in shim_cols:
            shim_data[col] = np.where(shim_data[col] == '', np.nan, shim_data[col])


        data_orig = data.copy()
        data_orig = data_orig[(data_orig['identity'] == drop_val)]
        if "trunc" in expand and "full" not in expand:
            data_orig = data_orig[(data_orig['yr'] >= curryr - 3) | ((data_orig['yr'] == curryr - 4) & (data_orig['currmon'] == 12))]
        data_orig = data_orig[['currmon', 'yr'] + shim_cols]
        shim_data = shim_data[['currmon', 'yr'] + shim_cols]
        
        preview_data, has_diff = get_diffs(shim_data, data_orig, data, drop_val, curryr, currmon, sector_val, "preview", subsequent_chg)
        
        if has_diff == 1:
            
            # Test if the flag will be resolved by the edit by re-running calc stats flag and the relevant flag function 
            # Dont run if the col_issue is simply v_flag, which is an indication that there are no flags at the sub even though an edit is being made
            if orig_flag_list[0] != "v_flag":
                flags_resolved, flags_unresolved, new_flags = test_resolve_flags(preview_data, drop_val, curryr, currmon, sector_val, orig_flag_list, skip_list, v_threshold, r_threshold, flag_cols)                   
            else:
                flags_resolved = []
                flags_unresolved = []
                new_flags = []

            preview_data = preview_data[(preview_data['identity'] == drop_val)]
            if "trunc" in expand and "full" not in expand:
                preview_data = preview_data[(preview_data['yr'] >= curryr - 3) | ((preview_data['yr'] == curryr - 4) & (preview_data['currmon'] == 12))]
            preview_data['sub_prev'] = np.where(preview_data['identity'] == drop_val, 1, 0)
        else:
            preview_data = pd.DataFrame()
            flags_resolved = []
            flags_unresolved = []
            new_flags = []
    
    return data, preview_data, shim_data, message, message_display, flags_resolved, flags_unresolved, new_flags

# Layout for login page
def login_layout():
    return get_login_layout()


# Main page layout
@validate_login_session
def app_layout():
    return get_app_layout()
        

# Full multipage app layout
trend.layout = html.Div([
                    dcc.Location(id='url',refresh=False),
                        html.Div(
                            login_layout(),
                            id='page-content',                      
                                ),
                            ])

# Check to see what url the user entered into the web browser, and return the relevant page based on their choice
@trend.callback(Output('page-content','children'),
                  [Input('url','pathname')])
#@Timer("URL Check")
def router(pathname):
    if pathname[0:5] == '/home':
        return app_layout()
    elif pathname == '/login':
        return login_layout()
    else:
        return login_layout()

# Authenticate by checking credentials, if correct, authenticate the session, if not, authenticate the session and send user to login
@trend.callback([Output('url','pathname'),
                    Output('login-alert','children'),
                    Output('url', 'search')],
                    [Input('login-button','n_clicks')],
                    [State('login-username','value'),
                    State('login-password','value'),
                    State('sector_input', 'value'),
                    State('login-curryr','value'),
                    State('login-currmon','value'),
                    State('msq_load','value')])
#@Timer("Login Auth")
def login_auth(n_clicks, username, pw, sector_input, curryr, currmon, msq_load):

    if n_clicks is None or n_clicks==0:
        return '/login', no_update, '', 
    else:
        credentials = {'user': username, "password": pw, "sector": sector_input}
        if authenticate_user(credentials) == True:
            session['authed'] = True
            pathname = '/home' + "?"
            if len(msq_load) == 0:
                msq_load = "N"
            else:
                msq_load = msq_load[0]
            return pathname, '', username + "/" + sector_input.title() + "/" + str(curryr) + "m" + str(currmon) + "/" + msq_load
        else:
            session['authed'] = False
            if sector_input == None:
                message = 'Select a Sector.'
            else:
                message = 'Incorrect credentials.'
            return no_update, dbc.Alert(message, color='danger', dismissable=True), no_update


    
@trend.callback([Output('store_user', 'data'),
                 Output('sector', 'data'),
                 Output('curryr', 'data'),
                 Output('currmon', 'data'),
                 Output('store_msq_load', 'data')],
                 [Input('url', 'search')])
#@Timer("Store Input Vals")
def store_input_vals(url_input):
    if url_input is None:
        raise PreventUpdate
    else:
        user, sector_val, global_vals, msq_load = url_input.split("/")
        curryr, currmon = global_vals.split("m")
        curryr = int(curryr)
        currmon = int(currmon)
        return user, sector_val.lower(), curryr, currmon, msq_load

@trend.callback([Output('dropman', 'options'),
                 Output('droproll', 'options'),
                 Output('dropsum', 'options'),
                 Output('dropsum', 'value'),
                 Output('input_file', 'data'),
                 Output('store_orig_cols', 'data'),
                 Output('dropflag', 'options'),
                 Output('dropflag', 'value'),
                 Output('init_trigger', 'data'),
                 Output('file_load_alert', 'is_open'),
                 Output('scatter-type-radios', 'value'),
                 Output('v_threshold', 'data'),
                 Output('r_threshold', 'data'),
                 Output('store_flag_cols', 'data'),
                 Output('hide_cd_container', 'style')],
                 [Input('sector', 'data'),
                 Input('curryr', 'data'),
                 Input('currmon', 'data'),
                 Input('store_msq_load', 'data')],
                 [State('store_flag_cols', 'data')])
#@Timer("Initial Data Load")
def initial_data_load(sector_val, curryr, currmon, msq_load, flag_cols):

    if sector_val is None:
        raise PreventUpdate
    else:
        oob_data, orig_cols, file_used = initial_load(sector_val, curryr, currmon, msq_load)
        
        # Contniue with the callback if the input file loaded successfully
        if file_used != "error":
            # Export the pickled oob values to begin setting up the decision log if this is the first time the user is running the program
            if file_used == "oob":
                test_cols = list(oob_data.columns)
                oob_cols = []
                for x in test_cols:
                    if "oob" in x:
                        oob_cols.append(x)
                decision_data = oob_data.copy()
                decision_data = decision_data.reset_index()
                decision_data = decision_data[['identity_row', 'identity', 'subsector', 'metcode', 'subid', 'yr', 'currmon'] + oob_cols]
                update_cols = ['cons_new', 'vac_new', 'abs_new', 'G_mrent_new', 'G_merent_new', 'gap_new', 'inv_new', 'avail_new', 'mrent_new', 'merent_new', 'vac_chg_new'] 
                if sector_val != "ind":
                    update_cols += ['conv_new', 'demo_new']
                for x in update_cols:
                    decision_data[x] = np.nan
                decision_data = decision_data.set_index('identity_row')
                decision_data['i_user'] = np.nan
                decision_data['c_user'] = np.nan
                decision_data['v_user'] = np.nan
                decision_data['g_user'] = np.nan
                decision_data['e_user'] = np.nan
                decision_data['skipped'] = ''
                decision_data['skip_user'] = ''
                use_pickle("out", "decision_log_" + sector_val, decision_data, curryr, currmon, sector_val)

            temp = oob_data.copy()
            temp = temp.set_index('identity')
            sub_combos = list(temp.index.unique())

            met_combos_temp = list(oob_data['identity_met'].unique())
            met_combos_temp.sort()
            met_combos = sorted(list(oob_data['identity_us'].unique())) + met_combos_temp

            if sector_val == "apt" or sector_val == "off" or sector_val == "ret":
                default_drop = met_combos[0]
            elif sector_val == "ind":
                default_drop = "US" + list(oob_data['subsector'].unique())[0] + list(oob_data['expansion'].unique())[0]

            oob_data.replace([np.inf, -np.inf], np.nan, inplace=True)
            use_pickle("out", "main_data_" + sector_val, oob_data, curryr, currmon, sector_val)

            flag_list = get_issue(False, False, False, False, False, False, False, False, False, False, "list", sector_val)
            flag_list_all = list(flag_list.keys())

            
            oob_data, rank_data_met, rank_data_sub, sum_data, nat_data_rent, nat_data_vac, v_threshold, r_threshold, flag_cols = first_update(oob_data, file_used, sector_val, orig_cols, curryr, currmon)              
            
            use_pickle("out", "main_data_" + sector_val, oob_data, curryr, currmon, sector_val)
            use_pickle("out", "rank_data_met_" + sector_val, rank_data_met, curryr, currmon, sector_val)
            use_pickle("out", "rank_data_sub_" + sector_val, rank_data_sub, curryr, currmon, sector_val)
            use_pickle("out", "sum_data_" + sector_val, sum_data, curryr, currmon, sector_val)
            use_pickle("out", "nat_data_vac_" + sector_val, nat_data_vac, curryr, currmon, sector_val)
            use_pickle("out", "nat_data_rent_" + sector_val, nat_data_rent, curryr, currmon, sector_val)

            init_trigger = True

            if sector_val != "ind":
                hide_cd_display = {'padding-left': '5px', 'width': '7%', 'display': 'inline-block', 'vertical-align': 'top'}
            else:
                 hide_cd_display = {'display': 'none'}

            return [{'label': i, 'value': i} for i in sub_combos], [{'label': i, 'value': i} for i in met_combos], [{'label': i, 'value': i} for i in met_combos], default_drop, file_used, orig_cols, [{'label': i, 'value': i} for i in flag_list_all], flag_list_all[0], init_trigger, no_update, "c", v_threshold, r_threshold, flag_cols, hide_cd_display

        # If the input file did not load successfully, alert the user
        elif file_used == "error":
            init_trigger = False
            return no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update, init_trigger, True, no_update, no_update, no_update, no_update, no_update

@trend.callback(Output('out_flag_trigger', 'data'),
                  [Input('sector', 'data'),
                  Input('flag-button', 'n_clicks'),
                  Input('store_init_flags', 'data')],
                  [State('curryr', 'data'),
                  State('currmon', 'data'),
                  State('init_trigger', 'data'),
                  State('store_flag_cols', 'data')])
#@Timer("Output Flags XLS")
def output_flags(sector_val, init_flags_triggered, all_buttons, curryr, currmon, success_init, flag_cols):
    
    if sector_val is None or success_init == False:
        raise PreventUpdate
    else:
        input_id = get_input_id()
        if input_id == "flag-button":
            use_pickle(False, sector_val + "_original_flags", False, curryr, currmon, sector_val)
            
            current_flags = use_pickle("in", "main_data_" + sector_val, False, curryr, currmon, sector_val)
            
            cols_to_keep = ['identity', 'subsector', 'metcode', 'subid', 'yr', 'currmon'] + flag_cols
            current_flags = current_flags[cols_to_keep]
            path_out = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/OutputFiles/{}_current_flags.csv".format(get_home(), sector_val, str(curryr), str(currmon), sector_val))
            current_flags.reset_index().set_index('identity').to_csv(path_out, na_rep='')
    
        return True

@trend.callback(Output('confirm_finalizer', 'displayed'),
                [Input('sector', 'data'),
                Input('store_submit_button', 'data'),
                Input('finalize-button', 'n_clicks')],
                [State('curryr', 'data'),
                State('currmon', 'data'),
                State('init_trigger', 'data')])
#@Timer("Confirm Finalizer")
def confirm_finalizer(sector_val, submit_button, finalize_button, curryr, currmon, success_init):
    input_id = get_input_id()

    if sector_val is None or success_init == False:
        raise PreventUpdate
    # Need this callback to tie to update_data callback so the callback is not executed before the data is actually updated, but only want to actually save the data when the finalize button is clicked, so only do that when the input id is for the finalize button
    elif input_id != "finalize-button":
        raise PreventUpdate
    else:
        print("End Confirm Finalizer")
        return True

@trend.callback([Output('finalize_trigger', 'data'),
                Output('finalizer_logic_alert', 'is_open'),
                Output('logic_alert_text', 'children')],
                [Input('confirm_finalizer', 'submit_n_clicks')],
                [State('sector', 'data'),
                State('curryr', 'data'),
                State('currmon', 'data'),
                State('init_trigger', 'data')])
#@Timer("Finalize Econ")
def finalize_econ(confirm_click, sector_val, curryr, currmon, success_init):

    if sector_val is None or success_init == False:
        raise PreventUpdate
    else:
        data = use_pickle("in", "main_data_" + sector_val, False, curryr, currmon, sector_val)
        
        if sector_val == "ind":
            output_cols = ['subsector', 'metcode', 'subid', 'subname', 'yr', 'qtr', 'currmon', 'inv', 'cons', 'vac', 'avail', 'occ', 'abs', 'mrent', 'G_mrent', 'merent', 'G_merent', 'gap']
        else:
            output_cols = ['metcode', 'subid', 'subname', 'yr', 'qtr', 'currmon', 'inv', 'cons', 'conv', 'demo', 'vac', 'avail', 'occ', 'abs', 'mrent', 'G_mrent', 'merent', 'G_merent', 
                           'rol_cons', 'rol_vac', 'rol_occ', 'rol_abs', 'rol_merent', 'sqinv', 'sqcons', 'sqvac', 'sqsren']
        if sector_val == "ret":
            output_cols = ['subsector'] + output_cols

        finalized = data.copy()
        finalized = finalized[output_cols]

        # Append the deep history (prior to 2008 q4) to the edits period
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/OutputFiles/{}_deep_hist.pickle".format(get_home(), sector_val, str(curryr), str(currmon), sector_val))
        deep_hist = pd.read_pickle(file_path)
        deep_hist = deep_hist[output_cols]
        finalized = finalized.append(deep_hist, ignore_index=True)
        finalized.sort_values(by=['metcode', 'subid', 'yr', 'qtr', 'currmon'], ascending=[True, True, True, True, True], inplace=True)
        
        if sector_val == "ind":
            finalized = finalized.rename(columns={'G_mrent': 'g_mrent', 'G_merent': 'g_merent'})
            finalized.sort_values(by=['subsector', 'metcode', 'subid', 'yr', 'qtr', 'currmon'], inplace=True)
        elif sector_val == "apt":
            finalized = finalized.rename(columns={'rol_cons': 'cons_pub', 'rol_vac': 'vac_pub', 'rol_occ': 'occ_pub', 'rol_abs': 'abs_pub', 'rol_merent': 'merent_pub', 'sqinv': 'sqinvoob', 'sqcons': 'sqconsoob', 'sqvac': 'sqvacoob', 'sqsren': 'sqsrenoob'})
            finalized['msa_level'] = np.where(finalized['subid'] == 90, 3, 1)
            finalized.sort_values(by=['metcode', 'subid', 'yr', 'qtr', 'currmon'], inplace=True)
        elif sector_val == "off":
            finalized = finalized.rename(columns={'rol_cons': 'cons_pub', 'rol_vac': 'vac_pub', 'rol_occ': 'occ_pub', 'rol_abs': 'abs_pub', 'rol_merent': 'merent_pub', 'sqinv': 'sqinvoob', 'sqcons': 'sqconsoob', 'sqvac': 'sqvacoob', 'sqsren': 'sqsrenoob'})
            finalized['msa_level'] = np.where((finalized['subid'] == 81) | (finalized['subid'] == 82), 3, 1)
            finalized.sort_values(by=['metcode', 'subid', 'yr', 'qtr', 'currmon'], inplace=True)
        elif sector_val == "ret":
            finalized = finalized.rename(columns={'G_mrent': 'g_mrent', 'G_merent': 'g_merent', 'rol_cons': 'cons_pub', 'rol_vac': 'vac_pub', 'rol_occ': 'occ_pub', 'rol_abs': 'abs_pub', 'rol_merent': 'merent_pub', 'sqinv': 'sqinvoob', 'sqcons': 'sqconsoob', 'sqvac': 'sqvacoob', 'sqsren': 'sqsrenoob'})

        finalized['currmon'] = np.where(finalized['currmon'] == 13, np.nan, finalized['currmon'])

        # Check for illogical values; alert the user if found and do not allow the trend to be finalized
        alert_display = False
        alert_text = ""
        vac_check = finalized.copy()
        if sector_val == "ind" or sector_val == "ret":
            vac_check['identity'] = vac_check['metcode'] + vac_check['subid'].astype(str) + vac_check['subsector']
        else:
            vac_check['identity'] = vac_check['metcode'] + vac_check['subid'].astype(str)
        
        vac_check = vac_check[(vac_check['vac'] < 0) | (vac_check['vac'] > 1)] 
        if len(vac_check) > 0:
            subs_flagged = vac_check['identity'].unique()
            alert_display = True
            alert_text = "The following subs have illogical vacancy level values. Cannot finalize the trend until they have been fixed: " + ', '.join(map(str, subs_flagged)) 
        else:
            gap_check = finalized.copy()
            if sector_val == "ind" or sector_val == "ret":
                gap_check['identity'] = gap_check['metcode'] + gap_check['subid'].astype(str) + gap_check['subsector']
            else:
                gap_check['identity'] = gap_check['metcode'] + gap_check['subid'].astype(str)
            gap_check = gap_check[(gap_check['gap'] < 0) | (gap_check['gap'] > 1)] 
            if len(gap_check) > 0:
                subs_flagged = gap_check['identity'].unique()
                alert_display = True
                alert_text = "The following subs have illogical gap level values. Cannot finalize the trend until they have been fixed: " + ', '.join(map(str, subs_flagged)) 
        
        if alert_display == False:
            if sector_val == "ind":
                for subsector in ["DW", "F"]:
                    finalized_copy = finalized.copy()
                    finalized_copy = finalized_copy[finalized_copy['subsector'] == subsector]
                    finalized_copy = finalized_copy.drop(['subsector'], axis=1)
                    file_path = "{}central/subcast/data/{}/download/current/{}subtrnds_{}_{}m{}_final.csv".format(get_home(), sector_val, sector_val, subsector, str(curryr), str(currmon))
                    finalized_copy.to_csv(file_path, index=False, na_rep='')
            
            elif sector_val == "apt" or sector_val == "off":
                file_path = "{}central/square/data/{}/production/msq/tcheck/{}subtrnds_QUANtified_{}m{}.csv".format(get_home(), sector_val, sector_val, str(curryr), str(currmon))
                finalized.to_csv(file_path, index=False, na_rep='')

            elif sector_val == "ret":
                gen_met = pd.read_stata("{}central/master-data/genmet.dta".format(get_home()), columns= ['metcode', 'tier'])
                gen_met = gen_met.rename(columns={'tier': 'msa_level'})
                gen_met = gen_met.set_index('metcode')
                finalized = finalized.join(gen_met, on='metcode')
                frames = []
                for subsector in ["N", "C", "NC", "N_C_NC"]:
                    finalized_copy = finalized.copy()
                    if subsector == "N" or subsector == "C":
                        finalized_copy = finalized_copy[finalized_copy['subsector'] == subsector]
                        finalized_copy = finalized_copy[finalized_copy['msa_level'] == 1]
                    elif subsector == "NC":
                        finalized_copy = finalized_copy[finalized_copy['msa_level'] == 3]
                    else:
                        n_frame = frames[0]
                        c_frame = frames[1]
                        nc_frame = frames[2]
                        finalized_copy = n_frame
                        finalized_copy = finalized_copy.append(c_frame)
                        finalized_copy = finalized_copy.append(nc_frame)
                        order_cols = list(finalized_copy.columns)
                        order_cols.remove('metcode')
                        order_cols = ['metcode'] + order_cols
                        finalized_copy = finalized_copy[order_cols]
                        finalized_copy = finalized_copy.rename(columns={'g_mrent': 'G_mrent', 'g_merent': 'G_merent'})
                        finalized_copy.sort_values(by=['metcode', 'subsector', 'subid', 'yr', 'qtr', 'currmon'], ascending = [True, True, True, True, True, True], inplace = True)
                    if subsector != "N_C_NC":
                        frames.append(finalized_copy)
                        finalized_copy = finalized_copy.drop(['subsector'], axis=1)
                        finalized_copy.sort_values(by=['metcode', 'subid', 'yr', 'qtr', 'currmon'], ascending = [True, True, True, True, True], inplace = True)
                    file_path = "{}central/square/data/{}/production/msq/tcheck/{}subtrnds_{}_QUANtified_{}m{}.csv".format(get_home(), sector_val, sector_val, subsector, str(curryr), str(currmon))
                    finalized_copy.to_csv(file_path, index=False, na_rep='')

            # Convert decision log to csv file and save in OutputFiles folder
            decision_log_in_path = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/OutputFiles/decision_log_{}.{}".format(get_home(), sector_val, str(curryr), str(currmon), sector_val, 'pickle'))
            decision_log = pd.read_pickle(decision_log_in_path)
            decision_log_out_path = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/OutputFiles/decision_log_{}.{}".format(get_home(), sector_val, str(curryr), str(currmon), sector_val, 'csv'))
            decision_log.to_csv(decision_log_out_path)

        return True, alert_display, alert_text


@trend.callback([Output('manual_message', 'message'),
                Output('manual_message', 'displayed'),
                Output('store_all_buttons', 'data'),
                Output('store_submit_button', 'data'),
                Output('store_preview_button', 'data'),
                Output('store_init_flags', 'data'),
                Output('store_flag_resolve', 'data'),
                Output('store_flag_unresolve', 'data'),
                Output('store_flag_new', 'data'),
                Output('store_flag_skips', 'data'),
                Output('countdown', 'data'),
                Output('countdown', 'columns'),
                Output('countdown_container', 'style'),
                Output('flag_filt', 'data'),
                Output('flag_filt', 'columns'),
                Output('flag_filt', 'style_table'),
                Output('flag_filt_container', 'style')],
                [Input('submit-button', 'n_clicks'),
                Input('preview-button', 'n_clicks'),
                Input('dropflag', 'value'),
                Input('init_trigger', 'data')],
                [State('sector', 'data'),
                State('store_orig_cols', 'data'),
                State('curryr', 'data'),
                State('currmon', 'data'),
                State('store_user', 'data'),
                State('input_file', 'data'),
                State('comment_cons', 'value'),
                State('comment_avail', 'value'),
                State('comment_mrent', 'value'),
                State('comment_erent', 'value'),
                State('dropman', 'value'),
                State('expand_hist', 'value'),
                State('flag_list', 'data'),
                State('init_trigger', 'data'),
                State('flag_description_noprev', 'children'),
                State('flag_description_resolved', 'children'),
                State('flag_description_unresolved', 'children'),
                State('flag_description_new', 'children'),
                State('flag_description_skipped', 'children'),
                State('subsequent_fix', 'value'),
                State('v_threshold', 'data'),
                State('r_threshold', 'data'),
                State('store_flag_cols', 'data')])
#@Timer("Update Data")
def update_data(submit_button, preview_button, drop_flag, init_fired, sector_val, orig_cols, curryr, currmon, user, file_used, cons_c, avail_c, mrent_c, erent_c, drop_val, expand, flag_list, success_init, skip_input_noprev, skip_input_resolved, skip_input_unresolved, skip_input_new, skip_input_skipped, subsequent_chg, v_threshold, r_threshold, flag_cols):
    
    input_id = get_input_id()

    if sector_val is None or success_init == False:
        raise PreventUpdate
    else:
        
        data = use_pickle("in", "main_data_" + sector_val, False, curryr, currmon, sector_val)

        # If there is a flag description, use this crazy dict/list slicer to get the actual values of the children prop so we can see what flags the user wants to skip
        if skip_input_noprev == "No flags for this submarket" or skip_input_noprev == "You have cleared all the flags":
            skip_list = []
        elif skip_input_noprev != None or skip_input_resolved != None or skip_input_unresolved != None or skip_input_new != None or skip_input_skipped != None:
            if len(skip_input_noprev) > 0:
                has_check = list(skip_input_noprev['props']['children'][0]['props']['children'][0]['props']['children'][0]['props'].keys())
                if 'value' in has_check:
                    skip_list_temp = skip_input_noprev['props']['children'][0]['props']['children'][0]['props']['children'][0]['props']['value']
                    skip_list = [e[5:] for e in skip_list_temp]
                else:
                    skip_list = []
            else:
                skip_list = []
                for input_list in [skip_input_resolved, skip_input_unresolved, skip_input_new, skip_input_skipped]:
                    if len(input_list) > 0:
                        has_check = list(input_list['props']['children'][0]['props']['children'][0]['props']['children'][0]['props'].keys())
                        if 'value' in has_check:
                            skip_list_temp = input_list['props']['children'][0]['props']['children'][0]['props']['children'][0]['props']['value']
                            skip_list_temp = [e[5:] for e in skip_list_temp]
                            skip_list += skip_list_temp
        else:
            skip_list = []

        if input_id == 'submit-button':
            data = data.reset_index()
            data = data.set_index('identity')
            data.loc[drop_val, 'inv_cons_comment'] = cons_c
            data.loc[drop_val, 'avail_comment'] = avail_c
            data.loc[drop_val, 'mrent_comment'] = mrent_c
            data.loc[drop_val, 'erent_comment'] = erent_c
            data = data.reset_index()
            data = data.set_index('identity_row')
        
        if input_id == 'submit-button' or input_id == 'preview-button':
            preview_data = use_pickle("in", "preview_data_" + sector_val, False, curryr, currmon, sector_val)
            shim_data = use_pickle("in", "shim_data_" + sector_val, False, curryr, currmon, sector_val)

        if input_id == 'submit-button':
            data, preview_data, shim_data, message, message_display = submit_update(data, shim_data, sector_val, orig_cols, user, drop_val, expand, flag_list, skip_list, curryr, currmon, subsequent_chg)

        elif input_id == 'preview-button':
            data, preview_data, shim_data, message, message_display, flags_resolved, flags_unresolved, flags_new = preview_update(data, shim_data, sector_val, preview_data, drop_val, expand, curryr, currmon, subsequent_chg, flag_list, skip_list, v_threshold, r_threshold, flag_cols)
        
        else:
            message = ''
            message_display = False
            preview_data = pd.DataFrame()
            shim_data = pd.DataFrame()

        if input_id == "submit-button" or input_id == "init_trigger":
            countdown = data.copy()
            countdown = countdown[['identity', 'identity_us', 'flag_skip'] + flag_cols]
            countdown[flag_cols] = np.where((countdown[flag_cols] != 0), 1, countdown[flag_cols])
            countdown = live_flag_count(countdown, sector_val, flag_cols)

            type_dict_countdown, format_dict_countdown = get_types(sector_val)

            countdown_display = {'display': 'block', 'padding-top': '55px', 'padding-left': '10px'}

        if input_id != "preview-button":
            flag_filt, flag_filt_style_table, flag_filt_display, flag_filt_title = filter_flags(data, drop_flag)

        
        if input_id == 'submit-button':
            use_pickle("out", "main_data_" + sector_val, data, curryr, currmon, sector_val)
        
        use_pickle("out", "preview_data_" + sector_val, preview_data, curryr, currmon, sector_val)
        use_pickle("out", "shim_data_" + sector_val, shim_data, curryr, currmon, sector_val)

        # Need to set this variable so that the succeeding callbacks will only fire once update is done
        # This works because it makes the callbacks that use elements produced in this callback have an input that is linked to an output of this callback, ensuring that they will only be fired once this one completes
        # But dont update this if the user didnt enter any shims, as we dont want the succeeding callbacks to update
        # We need five - to differentiate if suceeding callbacks should fire regardless of what button was clicked, or if they should only fire only if a particular button was clicked, or if they should only fire if this is initial load
        if message_display == True:
            all_buttons = no_update
            submit_button = no_update
            preview_button = no_update
            init_flags = no_update
        else:
            if input_id == "submit-button":
                all_buttons = 1
                submit_button = 1
                preview_button = no_update
                init_flags = no_update
            elif input_id == "preview-button":
                all_buttons = 1
                submit_button = no_update
                preview_button = 1
                init_flags = no_update
            elif input_id == "dropflag":
                all_buttons = no_update
                submit_button = no_update
                preview_button = no_update
                init_flags = no_update
            else:
                all_buttons = 1
                submit_button = 1
                preview_button = 1
                init_flags = 1

        if input_id != "preview-button":
            flags_resolved = []
            flags_unresolved = []
            flags_new = []

        # Return statement is conditional on input_id - only want to update the flag countdown related outputs if this is a non-preview callback trigger
        
        if input_id == "submit-button" or input_id == "init_trigger":
            return message, message_display, all_buttons, submit_button, preview_button, init_flags, flags_resolved, flags_unresolved, flags_new, skip_list, countdown.to_dict('records'), [{'name': ['Flags Remaining', countdown.columns[i]], 'id': countdown.columns[i], 'type': type_dict_countdown[countdown.columns[i]], 'format': format_dict_countdown[countdown.columns[i]]}
                    for i in range(0, len(countdown.columns))], countdown_display, flag_filt.to_dict('records'), [{'name': [flag_filt_title, flag_filt.columns[i]], 'id': flag_filt.columns[i]} 
                        for i in range(0, len(flag_filt.columns))], flag_filt_style_table, flag_filt_display
        elif input_id == "dropflag":
            return message, message_display, all_buttons, submit_button, preview_button, init_flags, no_update, no_update, no_update, no_update, no_update, no_update, no_update, flag_filt.to_dict('records'), [{'name': [flag_filt_title, flag_filt.columns[i]], 'id': flag_filt.columns[i]} 
                        for i in range(0, len(flag_filt.columns))], flag_filt_style_table, flag_filt_display
        else:
            return message, message_display, all_buttons, submit_button, preview_button, init_flags, flags_resolved, flags_unresolved, flags_new, skip_list, no_update, no_update, no_update, no_update, no_update, no_update, no_update


@trend.callback(Output('dropman', 'value'),
                [Input('sector', 'data'),
                Input('init_trigger', 'data'),
                Input('store_submit_button', 'data')],
                [State('curryr', 'data'),
                State('currmon', 'data'),
                State('init_trigger', 'data'),
                State('v_threshold', 'data'),
                State('r_threshold', 'data'),
                State('store_flag_cols', 'data')])
#@Timer("Set Shim Drop")
def set_shim_drop(sector_val, init_fired, submit_button, curryr, currmon, success_init, v_threshold, r_threshold, flag_cols):
    
    if sector_val is None or success_init == False:
        raise PreventUpdate
    else:
        input_id = get_input_id()
        data = use_pickle("in", "main_data_" + sector_val, False, curryr, currmon, sector_val)

        # In order to get the next sub that is flagged, we need to recalc stats and flags to update the data to see if the old flag is removed.
        data = drop_cols(data)
        data = calc_stats(data, curryr, currmon, 1, sector_val)
        data = calc_flags(data, curryr, currmon, sector_val, v_threshold, r_threshold)
        flag_list, drop_val, has_flag = flag_examine(data, False, False, curryr, currmon, flag_cols)
    
        use_pickle("out", "main_data_" + sector_val, data, curryr, currmon, sector_val)

        return drop_val   


@trend.callback([Output('has_flag', 'data'),
                Output('flag_list', 'data'),
                Output('key_met_radios', 'value')],
                [Input('dropman', 'value'),
                Input('sector', 'data'),
                Input('init_trigger', 'data'),
                Input('store_preview_button', 'data')],
                [State('curryr', 'data'),
                State('currmon', 'data'),
                State('init_trigger', 'data'),
                State('v_threshold', 'data'),
                State('r_threshold', 'data'),
                State('store_flag_cols', 'data'),
                State('store_flag_unresolve', 'data'),
                State('store_flag_new', 'data')])
#@Timer("Calc Stats and Flags")
def calc_stats_flags(drop_val, sector_val, init_fired, preview_status, curryr, currmon, success_init, v_threshold, r_threshold, flag_cols, flags_unresolved, flags_new):
    if sector_val is None or success_init == False:
        raise PreventUpdate
    else:    

        data = use_pickle("in", "main_data_" + sector_val, False, curryr, currmon, sector_val)

        input_id = get_input_id()
        # Call the recalc stats/flags functions so we can get the first flagged sub if this is the initial load. Otherwise the data was refreshed at the earlier callback
        if input_id != 'dropman' and input_id != "store_preview_button":
            data = drop_cols(data)
            data = calc_stats(data, curryr, currmon, 1, sector_val)
            data = calc_flags(data, curryr, currmon, sector_val, v_threshold, r_threshold)

        flag_list, drop_val, has_flag = flag_examine(data, drop_val, True, curryr, currmon, flag_cols)

        # Reset the radio button to the correct variable based on the new flag
        if has_flag == 1:
            if len(flags_unresolved) > 0:
                key_met_radio_val = flags_unresolved[0][0]
            elif len(flags_new) > 0:
                key_met_radio_val = flags_new[0][0]
            else:
                key_met_radio_val = flag_list[0][0]
        else:
            key_met_radio_val = no_update

        return has_flag, flag_list, key_met_radio_val

@trend.callback(Output('download_trigger', 'data'),
                   [Input('sector', 'data'),
                   Input('store_submit_button', 'data'),
                   Input('download-button', 'n_clicks')],
                   [State('curryr', 'data'),
                   State('currmon', 'data'),
                   State('init_trigger', 'data')])
#@Timer("Output Edits XLS")
def output_edits(sector_val, submit_button, download_button, curryr, currmon, success_init):
    input_id = get_input_id()
    if sector_val is None or success_init == False:
        raise PreventUpdate
    # Need this callback to tie to update_data callback so the csv is not set before the data is actually updated, but dont want to call the set csv function each time submit is clicked, so only do that when the input id is for the download button
    elif input_id == "store_submit_button":
        raise PreventUpdate
    else:
        data = use_pickle("in", "main_data_" + sector_val, False, curryr, currmon, sector_val)

        output_cols = ['identity', 'subsector', 'metcode', 'subid', 'subname', 'yr', 'qtr', 'currmon', 'inv', 'cons', 'vac', 'vac_chg', 'avail', 'occ', 'abs', 'mrent', 'G_mrent', 'merent', 'G_merent', 'gap', 'conv', 'demo', 'gap_chg', 'inv_oob', 'cons_oob', 'avail_oob', 'vac_oob', 'G_mrent_oob', 'G_merent_oob', 'conv_oob', 'demo_oob', 'inv_cons_comment', 'avail_comment', 'mrent_comment', 'erent_comment']

        edits_output = data.copy()
        edits_output = edits_output[output_cols]
        edits_output['inv_cons_comment'] = np.where(edits_output['inv_cons_comment'] == "Enter Inv or Cons Shim Note Here", '', edits_output['inv_cons_comment'])
        edits_output['avail_comment'] = np.where(edits_output['avail_comment'] == "Enter Avail Shim Note Here", '', edits_output['avail_comment'])
        edits_output['mrent_comment'] = np.where(edits_output['mrent_comment'] == "Enter Mrent Shim Note Here", '', edits_output['mrent_comment'])
        edits_output['erent_comment'] = np.where(edits_output['erent_comment'] == "Enter Erent Shim Note Here", '', edits_output['erent_comment'])
        edits_output['currmon'] = np.where(edits_output['currmon'] == 13, np.nan, edits_output['currmon'])

        file_path = "{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/OutputFiles/{}_edits_trend.csv".format(get_home(), sector_val, curryr, currmon, sector_val)
        edits_output.to_csv(file_path, index=False, na_rep='')

        # Create the review packet for econ
        review_packet = data.copy()
        
        if sector_val == "ind":
            review_packet['conv'] = 0
            review_packet['demo'] = 0
        
        # Read in the MSQ data and use it to create review packet variables needed for the metro packet
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_msq_data.pickle".format(get_home(), sector_val))
        msq_input = pd.read_pickle(file_path)

        if currmon == 1:
            msq_input = msq_input[((msq_input['yr'] == curryr) | ((msq_input['yr'] == curryr - 1) & (msq_input['currmon'] == 12)))]
        else:
            msq_input = msq_input[(msq_input['yr'] == curryr)]

        if sector_val == "ret":
            msq_input['identity_met'] = msq_input['metcode'] + "Ret"
        
        # Calculate total number of ids per met in the square set
        # Drop ids in balance subs, to be consistent with DQ
        # Caveat here is it will not match DQs figure, since his method leaves out ids built later in the cycle
        data_msq = msq_input.copy()
        data_msq['balance_test'] = data_msq['submkt'].str.slice(0,2)
        data_msq = data_msq[data_msq['balance_test'] != '99']
        data_msq = data_msq[(data_msq['yr'] == curryr) & (data_msq['currmon'] == currmon)]
        data_msq['sq_ids'] = data_msq.groupby('identity_met')['sizex'].transform('count')
        data_msq = data_msq[['identity_met', 'sq_ids']]
        data_msq = data_msq.drop_duplicates('identity_met')
        data_msq = data_msq.set_index('identity_met')
        review_packet = review_packet.join(data_msq, on='identity_met')

        # Calculate some additional sq construction datapoints that could not be joined in from the base met file created by indtrend1
        data_msq = msq_input.copy()
        data_msq = data_msq[(data_msq['yr'] == curryr) & (data_msq['currmon'] == currmon)]
        data_msq = data_msq[(data_msq['yearx'] == curryr) & (data_msq['month'] == currmon)]
        data_msq['sq_cons'] = data_msq.groupby('identity_met')['sizex'].transform('sum')
        data_msq['sq_avail'] = data_msq.groupby('identity_met')['totavailx'].transform('sum')
        data_msq['sq_ncabs'] = data_msq['sq_cons'] - data_msq['sq_avail']
        data_msq['tot_props'] = data_msq.groupby('identity_met')['sizex'].transform('count')
        data_msq['tot_props_surveyed'] = data_msq[data_msq['availxM'] == 0].groupby('identity_met')['sizex'].transform('count')
        data_msq['sq_nc_vac_coverpct'] = data_msq['tot_props_surveyed'] / data_msq['tot_props']
        data_msq['sq_cons_askrev'] = data_msq['renx'] * data_msq['sizex']
        data_msq['met_sq_cons_askrev'] = data_msq.groupby('identity_met')['sq_cons_askrev'].transform('sum')
        data_msq['sq_cons_askrent'] = data_msq['met_sq_cons_askrev'] / data_msq['sq_cons']
        data_msq = data_msq[['identity_met', 'sq_ncabs', 'sq_nc_vac_coverpct', 'sq_cons_askrent']]
        data_msq = data_msq.drop_duplicates('identity_met')
        data_msq = data_msq.set_index('identity_met')
        data_msq['sq_cons_askrent'] = round(data_msq['sq_cons_askrent'], 2)
        review_packet = review_packet.join(data_msq, on='identity_met')

        # Calculate thhe inventory for sq props with a vac survey, as well as the surveyed abs
        data_msq = msq_input.copy()
        if currmon != 1:
            data_msq['past_mon'] = np.where((data_msq['yr'] == curryr) & (data_msq['currmon'] == currmon - 1), 1, 0)
        else:
            data_msq['past_mon'] = np.where((data_msq['yr'] == curryr - 1) & (data_msq['currmon'] == 12), 1, 0)
        data_msq = data_msq[(((data_msq['yr'] == curryr) & (data_msq['currmon'] == currmon)) | (data_msq['past_mon'] == 1))]
        data_msq.sort_values(by=['id', 'yr', 'currmon'], ascending=[True, True, True], inplace=True)
        data_msq['prop_sq_survey_abs'] = np.where(data_msq['id'] == data_msq['id'].shift(1), data_msq['totavailx'] - data_msq['totavailx'].shift(1), np.nan)
        data_msq['prop_sq_survey_abs'] = np.where((data_msq['yearx'] == curryr) & (data_msq['month'] == currmon), data_msq['totavailx'] - data_msq['sizex'], data_msq['prop_sq_survey_abs'])
        data_msq = data_msq[data_msq['past_mon'] == 0]
        data_msq = data_msq[data_msq['availxM'] == 0]
        data_msq['sq_survey_inv'] = data_msq.groupby('identity_met')['sizex'].transform('sum')
        data_msq['sq_survey_abs'] = data_msq.groupby('identity_met')['prop_sq_survey_abs'].transform('sum')
        data_msq['sq_survey_abs'] = data_msq['sq_survey_abs'] * -1
        data_msq = data_msq.drop_duplicates('identity_met')
        data_msq = data_msq.set_index('identity_met')
        data_msq = data_msq[['sq_survey_inv', 'sq_survey_abs']]
        review_packet = review_packet.join(data_msq, on='identity_met')
        review_packet['sq_survey_inv'] = review_packet['sq_survey_inv'].fillna(0)
        
        # Calculate the NC rent to lagged period square rent premium
        review_packet['sq_cons_rentpremium_tolagsq'] = np.where((np.isnan(review_packet['sq_cons_askrent']) == False) & (review_packet['metsqcons'] > 0), (review_packet['sq_cons_askrent'] - review_packet['metsqsren'].shift(1)) / review_packet['metsqsren'].shift(1), np.nan)
        review_packet['sq_cons_rentpremium_tolagsq'] = round(review_packet['sq_cons_rentpremium_tolagsq'], 3)

        # Calculate the survey vars for vacancy and rent, if this is ind and a qtr rollup month. Otherwise, it has already been calculated
        if sector_val == "ind" and currmon in [2,3,6,9,12]:
            temp = msq_input.copy()
            temp['balance_test'] = temp['submkt'].str.slice(0,2)
            temp = temp[temp['balance_test'] != '99']
            temp = temp[(temp['yr'] == curryr) & (temp['currmon'] == currmon)]
            temp['metsqinv'] = temp.groupby('identity_met')['sizex'].transform('sum')
            temp = temp[temp['availxM'] == 0]
            temp['sur_size'] = temp.groupby('identity_met')['sizex'].transform('sum')
            temp['sur_vacsurvey_coverpct'] = temp['sur_size'] / temp['metsqinv']
            temp = temp[['identity_met', 'sur_vacsurvey_coverpct']]
            temp = temp.drop_duplicates('identity_met')
            temp = temp.set_index('identity_met')
            review_packet = review_packet.join(temp, on='identity_met')

            # Note: DQ only included properties with a lagged survey within the sq window, so will drop any ids that do not have surveys with a non nan currmon var value
            temp = msq_input.copy()
            temp['balance_test'] = temp['submkt'].str.slice(0,2)
            temp = temp[temp['balance_test'] != '99']
            temp = temp[(temp['yr'] == curryr) & (temp['currmon'] == currmon)]
            temp['metsqinv'] = temp.groupby('identity_met')['sizex'].transform('sum')
            temp = temp[temp['has_l_surv'] == 1]
            if sector_val != "ret":
                temp = temp[temp['renxM'] == 0]
            else:
                temp = temp[temp['nrenxM'] == 0]
            temp['sur_rntchginv'] = temp.groupby('identity_met')['sizex'].transform('sum')
            temp['sur_rentsurvey_coverpct'] = temp['sur_rntchginv'] / temp['metsqinv']
            temp = temp[['identity_met', 'sur_rentsurvey_coverpct', 'sur_rntchginv', 'sur_rentchgs_avgdiff_mo_wgt', 'sur_rentchgs_avgmos_tolastsur']]
            temp = temp.drop_duplicates('identity_met')
            temp = temp.set_index('identity_met')
            review_packet = review_packet.join(temp, on='identity_met')
        elif sector_val == "apt" or sector_val == "off":
            review_packet['sur_vacsurvey_coverpct'] = review_packet['met_sur_v_cov_perc']
            review_packet['sur_rentsurvey_coverpct'] = review_packet['met_sur_r_cov_perc']
            review_packet['sur_rentchgs_avgmos_tolastsur'] = review_packet['met_avg_mos_to_last_rensur']
            review_packet['sur_rentchgs_avgdiff_mo_wgt'] = review_packet['met_g_renx_mo_wgt']
            review_packet['sur_rntchginv'] = review_packet['met_rntchginv']
        elif sector_val == "ret":
            file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/ret_nc_insight_formetpacket.pickle".format(get_home()))
            insight_nc = pd.read_pickle(file_path)
            insight_nc = insight_nc.reset_index()
            insight_nc['metcode'] = insight_nc['identity'].str.slice(0,2)
            insight_nc = insight_nc[['metcode', 'nc_met_avg_mos_to_last_rensur', 'nc_met_rntchginv', 'nc_met_g_renx_mo_wgt', 'nc_met_sur_r_cov_perc', 'nc_met_sur_v_cov_perc']]
            insight_nc = insight_nc.rename(columns={'nc_met_avg_mos_to_last_rensur': 'sur_rentchgs_avgmos_tolastsur', 'nc_met_rntchginv': 'sur_rntchginv', 'nc_met_g_renx_mo_wgt': 'sur_rentchgs_avgdiff_mo_wgt', 'nc_met_sur_r_cov_perc': 'sur_rentsurvey_coverpct', 'nc_met_sur_v_cov_perc': 'sur_vacsurvey_coverpct'})
            insight_nc = insight_nc.drop_duplicates('metcode')
            insight_nc = insight_nc.set_index('metcode')
            review_packet = review_packet.join(insight_nc, on='metcode')
            for x in ['sur_vacsurvey_coverpct', 'sur_rentsurvey_coverpct', 'sur_rentchgs_avgmos_tolastsur', 'sur_rentchgs_avgdiff_mo_wgt', 'sur_rntchginv']:
                review_packet[x] = np.where((review_packet['yr'] == curryr) & (review_packet['currmon'] == currmon), review_packet[x], np.nan)

        # Calculate the last 3 months vars for published data
        for var, name in zip(['cons', 'abs'], ['final_cons_last3mos', 'final_abs_last3mos']):
            temp_3 = review_packet.copy()
            if currmon >= 3:
                temp_3 = temp_3[(temp_3['yr'] == curryr) & (temp_3['currmon'] >= currmon - 2) & (temp_3['currmon'] <= currmon)]
            elif currmon == 2:
                temp_3 = temp_3[(temp_3['yr'] == curryr) | ((temp_3['yr'] == curryr - 1) & (temp_3['currmon'] == 12))]
            elif currmon == 1:
                temp_3 = temp_3[(temp_3['yr'] == curryr) | ((temp_3['yr'] == curryr - 1) & (temp_3['currmon'] >= 11))]
            temp_3[name] = temp_3.groupby('identity_met')[var].transform('sum')
            temp_3 = temp_3.drop_duplicates('identity_met')
            temp_3 = temp_3.set_index('identity_met')
            temp_3 = temp_3[name]
            review_packet = review_packet.join(temp_3, on='identity_met')

        temp_3 = review_packet.copy()
        if currmon >= 4:
            temp_3 = temp_3[(temp_3['yr'] == curryr) & (temp_3['currmon'] == currmon) | ((temp_3['currmon'] == currmon - 3))]
        elif currmon == 3:
            temp_3 = temp_3[((temp_3['yr'] == curryr) & (temp_3['currmon'] == 3)) | ((temp_3['yr'] == curryr - 1) & (temp_3['currmon'] == 12))]
        elif currmon == 2:
            temp_3 = temp_3[((temp_3['yr'] == curryr) & (temp_3['currmon'] == 2)) | ((temp_3['yr'] == curryr - 1) & (temp_3['currmon'] == 11))]
        elif currmon == 1:
            temp_3 = temp_3[((temp_3['yr'] == curryr) & (temp_3['currmon'] == 1)) | ((temp_3['yr'] == curryr - 1) & (temp_3['currmon'] == 10))]
        
        temp = temp_3.copy()
        temp['met_avail'] = temp.groupby(['identity_met', 'yr', 'currmon'])['avail'].transform('sum')
        temp['met_inv'] = temp.groupby(['identity_met', 'yr', 'currmon'])['inv'].transform('sum')
        temp['met_vac'] = temp['met_avail'] / temp['met_inv']
        temp['met_vac'] = round(temp['met_vac'], 4)
        temp['final_vacchg_last3mos'] = np.where(temp['identity_met'] == temp['identity_met'].shift(1), temp['met_vac'] - temp['met_vac'].shift(1), np.nan)
        
        temp['mrev'] = temp['mrent'] * temp['inv']
        temp['met_mrev'] = temp.groupby(['identity_met', 'yr', 'currmon'])['mrev'].transform('sum')
        temp['met_mrent'] = temp['met_mrev'] / temp['met_inv']
        temp['met_mrent'] = round(temp['met_mrent'], 2)
        temp['final_Gmrent_last3mos'] = np.where(temp['identity_met'] == temp['identity_met'].shift(1), (temp['met_mrent'] - temp['met_mrent'].shift(1)) / temp['met_mrent'].shift(1), np.nan)
        
        temp['erev'] = temp['merent'] * temp['inv']
        temp['met_erev'] = temp.groupby(['identity_met', 'yr', 'currmon'])['erev'].transform('sum')
        temp['met_erent'] = temp['met_erev'] / temp['met_inv']
        temp['met_erent'] = round(temp['met_erent'], 2)
        temp['final_Gmerent_last3mo'] = np.where(temp['identity_met'] == temp['identity_met'].shift(1), (temp['met_erent'] - temp['met_erent'].shift(1)) / temp['met_erent'].shift(1), np.nan)
        
        temp['met_gap'] = ((temp['met_erent'] - temp['met_mrent']) / temp['met_mrent']) * -1
        temp['final_gapchg_last3mo'] = np.where(temp['identity_met'] == temp['identity_met'].shift(1), temp['met_gap'] - temp['met_gap'].shift(1), np.nan)
        
        temp = temp[(temp['yr'] == curryr) & (temp['currmon'] == currmon)]
        temp = temp.drop_duplicates('identity_met')
        temp = temp[['identity_met', 'final_vacchg_last3mos', 'final_Gmrent_last3mos', 'final_Gmerent_last3mo', 'final_gapchg_last3mo']]
        temp = temp.set_index('identity_met')
        review_packet = review_packet.join(temp, on='identity_met')

        # Calcucate the YTD vars for published data. If this is Jan, no need to do this and vars will be set based on rolled vals in the create_packet function
        if currmon > 1:

            temp = review_packet.copy()
            temp = temp[temp['yr'] == curryr]
            temp['final_cons_ytd'] = temp.groupby('identity_met')['cons'].transform('sum')
            temp['final_abs_ytd'] = temp.groupby(['identity_met'])['abs'].transform('sum')
            temp = temp.drop_duplicates('identity_met')
            temp = temp[['identity_met', 'final_cons_ytd', 'final_abs_ytd']]
            temp = temp.set_index('identity_met')
            review_packet = review_packet.join(temp, on='identity_met')

            temp = review_packet.copy()
            temp = temp[((temp['yr'] == curryr) & (temp['currmon'] == currmon)) | ((temp['currmon'] == 12) & (temp['yr'] == curryr - 1))]

            temp['met_avail'] = temp.groupby(['identity_met', 'yr', 'currmon'])['avail'].transform('sum')
            temp['met_inv'] = temp.groupby(['identity_met', 'yr', 'currmon'])['inv'].transform('sum')
            temp['met_vac'] = temp['met_avail'] / temp['met_inv']
            temp['met_vac'] = round(temp['met_vac'], 4)
            temp['final_vacchg_ytd'] = np.where(temp['identity_met'] == temp['identity_met'].shift(1), temp['met_vac'] - temp['met_vac'].shift(1), np.nan)

            temp['mrev'] = temp['mrent'] * temp['inv']
            temp['met_mrev'] = temp.groupby(['identity_met', 'yr', 'currmon'])['mrev'].transform('sum')
            temp['met_mrent'] = temp['met_mrev'] / temp['met_inv']
            temp['met_mrent'] = round(temp['met_mrent'], 2)
            temp['final_Gmrent_ytd'] = np.where(temp['identity_met'] == temp['identity_met'].shift(1), (temp['met_mrent'] - temp['met_mrent'].shift(1)) / temp['met_mrent'].shift(1), np.nan)

            temp['erev'] = temp['merent'] * temp['inv']
            temp['met_erev'] = temp.groupby(['identity_met', 'yr', 'currmon'])['erev'].transform('sum')
            temp['met_erent'] = temp['met_erev'] / temp['met_inv']
            temp['met_erent'] = round(temp['met_erent'], 2)
            temp['final_Gmerent_ytd'] = np.where(temp['identity_met'] == temp['identity_met'].shift(1), (temp['met_erent'] - temp['met_erent'].shift(1)) / temp['met_erent'].shift(1), np.nan)

            temp = temp[(temp['yr'] == curryr) & (temp['currmon'] == currmon)]
            temp = temp.drop_duplicates('identity_met')
            temp = temp[['identity_met', 'final_vacchg_ytd', 'final_Gmrent_ytd', 'final_Gmerent_ytd']]
            temp = temp.set_index('identity_met')
            review_packet = review_packet.join(temp, on='identity_met')

        gen_met = pd.read_stata("{}central/master-data/genmet.dta".format(get_home()), columns= ['metcode', 'metro', 'state', 'reg_long'])
        gen_met = gen_met.set_index('metcode')
        review_packet = review_packet.join(gen_met, on='metcode')

        review_packet = review_packet.rename(columns={'metsqinv': 'sq_inv', 'metsqcons': 'sq_cons', 'metsqavail': 'sq_avail', 'metsqvac': 'sq_vac', 'metsqoccstk': 'sq_occ', 'metsqabs': 'sq_abs', 'metsqsren': 'sq_askrent', 'metsq_Gmrent': 'sq_askrent_chg', 'metncavail': 'sq_cons_avail', 'metncvac': 'sq_cons_vac', 'metdqinvren10': 'cube_inv_rent10', 'metdqren10d': 'cube_inv_rent10_diff'})        

        if sector_val == "ind":
            frames = []
            for subsector in ["DW", "F"]:
                file_path = "{}central/square/data/{}/trends-experimental/{}sq_cube_rollqtr_metstats_{}.dta".format(get_home(), sector_val, sector_val, subsector)
                cube_roll_stats = pd.read_stata(file_path)
                cube_roll_stats['identity_met'] = cube_roll_stats['metalias'].str.slice(0,2) + subsector
                cube_roll_stats = cube_roll_stats.set_index('identity_met')
                cube_roll_stats = cube_roll_stats.drop(['metalias'], axis=1)
                cube_roll_stats = cube_roll_stats.rename(columns={'rollqtr_Binvren00': 'rolling3mocube_rent00_inv', 'rollqtr_Bren00d': 'rolling3mocube_rent00_chg'})
                frames.append(cube_roll_stats)
            cube_roll_combined = frames[0].append(frames[1])
            review_packet = review_packet.join(cube_roll_combined, on='identity_met')

        elif sector_val == "apt" or sector_val == "off":
            file_path = "{}central/square/data/{}/trends-experimental/{}sq_cube_rollqtr_metstats.dta".format(get_home(), sector_val, sector_val)
            cube_roll_stats = pd.read_stata(file_path)
            cube_roll_stats['identity_met'] = cube_roll_stats['metalias'].str.slice(0,2) + sector_val.title()
            cube_roll_stats = cube_roll_stats.set_index('identity_met')
            cube_roll_stats = cube_roll_stats.drop(['metalias'], axis=1)
            cube_roll_stats = cube_roll_stats.rename(columns={'rollqtr_Binvren00': 'rolling3mocube_rent00_inv', 'rollqtr_Bren00d': 'rolling3mocube_rent00_chg'})
            review_packet = review_packet.join(cube_roll_stats, on='identity_met')

        elif sector_val == "ret":
            file_path = "{}central/square/data/{}/trends-experimental/{}sq_cube_rollqtr_metstats_NC.dta".format(get_home(), sector_val, sector_val)
            cube_roll_stats = pd.read_stata(file_path)
            cube_roll_stats['metcode'] = cube_roll_stats['metalias'].str.slice(0,2)
            cube_roll_stats = cube_roll_stats.set_index('metcode')
            cube_roll_stats = cube_roll_stats.drop(['metalias'], axis=1)
            cube_roll_stats = cube_roll_stats.rename(columns={'rollqtr_Binvren00': 'rolling3mocube_rent00_inv', 'rollqtr_Bren00d': 'rolling3mocube_rent00_chg'})
            review_packet = review_packet.join(cube_roll_stats, on='metcode')

        # Use the NC cut sqinsight stats for ret, since the review packet presents each met without subsectors
        if sector_val == "ret":
            file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/ret_combined_insight_data.pickle".format(get_home()))
            insight = pd.read_pickle(file_path)
            cols_to_keep = []
            for x in list(insight.columns):
                if x[0:2] == "nc" and 'sub' not in x:
                    insight.rename(columns={x: x[3:]}, inplace=True)
                    cols_to_keep.append(x[3:])
            insight = insight[cols_to_keep]
            insight = insight.reset_index()
            insight['metcode'] = insight['identity'].str.slice(0,2)
            insight = insight.drop_duplicates('metcode')
            insight = insight.set_index('metcode')
            insight = insight.drop(['identity', 'rentdrops', 'rentflats', 'rentincrs', 'vacdrops', 'vacflats', 'vacincrs'], axis=1)
            review_packet = review_packet.drop(['met_sur_r_cov_perc', 'met_sur_v_cov_perc', 'met_wtdvacchg', 'met_sur_totabs', 'met_vacchginv', 
                                                'met_avg_mos_to_last_vacsur', 'met_stddev_avg_rentchg', 'met_avg_rentchg_mo', 'met_avg_rentchg',
                                                'met_rentincrs', 'met_rentflats', 'met_rentdrops', 'met_rentchgs', 'met_g_renx_mo_wgt',
                                                'met_rntchginv', 'met_avg_mos_to_last_rensur'], axis=1)
            review_packet = review_packet.join(insight, on='metcode')
            review_packet['met_sur_totabs'] = round(review_packet['met_sur_totabs'], -3)
        
        review_packet_us, review_packet_met = create_review_packet(review_packet, curryr, currmon, sector_val)
        
        file_path = "{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/OutputFiles/{}_review_packet_us.csv".format(get_home(), sector_val, curryr, currmon, sector_val)
        review_packet_us.to_csv(file_path, index=False, na_rep='')
        file_path = "{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/OutputFiles/{}_review_packet_met.csv".format(get_home(), sector_val, curryr, currmon, sector_val)
        review_packet_met.to_csv(file_path, index=False, na_rep='')

        return True

@trend.callback([Output('rank_table_met', 'data'),
                Output('rank_table_met', 'columns'),
                Output('rank_table_met', 'style_data_conditional'),
                Output('rank_table_sub', 'data'),
                Output('rank_table_sub', 'columns'),
                Output('rank_table_sub', 'style_data_conditional'),
                Output('rank_table_container', 'style'),
                Output('sum_table', 'data'),
                Output('sum_table', 'columns'),
                Output('sum_table_container', 'style'),
                Output('sum_table', 'style_data_conditional'),
                Output('nat_metrics_vac', 'data'),
                Output('nat_metrics_vac', 'columns'),
                Output('nat_metrics_vac_container', 'style'),
                Output('nat_metrics_vac', 'style_data_conditional'),
                Output('nat_metrics_rent', 'data'),
                Output('nat_metrics_rent', 'columns'),
                Output('nat_metrics_rent_container', 'style'),
                Output('nat_metrics_rent', 'style_data_conditional')],
                [Input('sector', 'data'),
                Input('dropsum', 'value'),
                Input('store_init_flags', 'data')],
                [State('curryr', 'data'),
                State('currmon', 'data'),
                State('init_trigger', 'data'),
                State('store_flag_cols', 'data')])
#@Timer("Display Summary")
def display_summary(sector_val, drop_val, init_flags, curryr, currmon, success_init, flag_cols):
    if sector_val is None or success_init == False:
        raise PreventUpdate
    else:
        input_id = get_input_id()

        rank_display = {'display': 'block'}
        sum_display = {'display': 'block', 'padding-left': '10px'}
        nat_met_vac_display = {'display': 'block', 'padding-top': '30px'}
        nat_met_rent_display = {'display': 'block', 'padding-top': '30px'}
        
        if input_id == 'store_init_flags':
            rank_data_met = use_pickle("in", "rank_data_met_" + sector_val, False, curryr, currmon, sector_val)
            rank_data_sub = use_pickle("in", "rank_data_sub_" + sector_val, False, curryr, currmon, sector_val)
            sum_data = use_pickle("in", "sum_data_" + sector_val, False, curryr, currmon, sector_val)
            nat_data_vac = use_pickle("in", "nat_data_vac_" + sector_val, False, curryr, currmon, sector_val)
            nat_data_rent = use_pickle("in", "nat_data_rent_" + sector_val, False, curryr, currmon, sector_val)

            nat_data_vac = nat_data_vac.rename(columns={'us_sur_totabs': 'Surveyed Abs', 'us_mos_lvsurv': 'Months To Last Surv', 'us_sur_v_cov_perc': 'Survey Cover Pct', 'subsector': 'Subsector'})
            nat_data_rent = nat_data_rent.rename(columns={'us_g_renx_mo_wgt': 'Surveyed Month Rent Chg', 'us_mos_lrsurv': 'Months To Last Surv', 'us_sur_r_cov_perc': 'Survey Cover Pct', 'subsector': 'Subsector'})
            
            sum_data = summarize_flags(sum_data, drop_val, flag_cols)
        
            type_dict_rank_met, format_dict_rank_met = get_types(sector_val)
            highlighting_rank_met = get_style("partial", rank_data_met, curryr, currmon, [], [])
            type_dict_rank_sub, format_dict_rank_sub = get_types(sector_val)
            highlighting_rank_sub = get_style("partial", rank_data_sub, curryr, currmon, [], [])
            type_dict_sum, format_dict_sum = get_types(sector_val)
            highlighting_sum = get_style("partial", sum_data, curryr, currmon, [], [])
            type_dict_nat_vac, format_dict_nat_vac = get_types(sector_val)
            highlighting_nat_vac = get_style("partial", nat_data_vac, curryr, currmon, [], [])
            type_dict_nat_rent, format_dict_nat_rent = get_types(sector_val)
            highlighting_nat_rent = get_style("partial", nat_data_rent, curryr, currmon, [], [])

            nat_vac_title = str(curryr) + ' m' + str(currmon) + ' US Vacancy Survey Stats'
            nat_rent_title = str(curryr) + ' m' + str(currmon) + ' US Rent Survey Stats'
            
            return rank_data_met.to_dict('records'), [{'name':['Top Ten Flagged Metros', rank_data_met.columns[i]], 'id': rank_data_met.columns[i], 'type': type_dict_rank_met[rank_data_met.columns[i]], 'format': format_dict_rank_met[rank_data_met.columns[i]]} 
                                for i in range(0, len(rank_data_met.columns))], highlighting_rank_met, rank_data_sub.to_dict('records'), [{'name':['Top Ten Flagged Submarkets', rank_data_sub.columns[i]], 'id': rank_data_sub.columns[i], 'type': type_dict_rank_sub[rank_data_sub.columns[i]], 'format': format_dict_rank_sub[rank_data_sub.columns[i]]} 
                                for i in range(0, len(rank_data_sub.columns))], highlighting_rank_sub, rank_display, sum_data.to_dict('records'), [{'name': ['OOB Initial Flag Summary', sum_data.columns[i]], 'id': sum_data.columns[i], 'type': type_dict_sum[sum_data.columns[i]], 'format': format_dict_sum[sum_data.columns[i]]} 
                                for i in range(0, len(sum_data.columns))], sum_display, highlighting_sum, nat_data_vac.to_dict('records'), [{'name': [nat_vac_title, nat_data_vac.columns[i]], 'id': nat_data_vac.columns[i], 'type': type_dict_nat_vac[nat_data_vac.columns[i]], 'format': format_dict_nat_vac[nat_data_vac.columns[i]]} 
                                for i in range(0, len(nat_data_vac.columns))], nat_met_vac_display, highlighting_nat_vac, nat_data_rent.to_dict('records'), [{'name': [nat_rent_title, nat_data_rent.columns[i]], 'id': nat_data_rent.columns[i], 'type': type_dict_nat_rent[nat_data_rent.columns[i]], 'format': format_dict_nat_rent[nat_data_rent.columns[i]]} 
                                for i in range(0, len(nat_data_rent.columns))], nat_met_rent_display, highlighting_nat_rent
        else:
            sum_data = use_pickle("in", "sum_data_" + sector_val, False, curryr, currmon, sector_val)
            sum_data = summarize_flags(sum_data, drop_val, flag_cols)
            type_dict_sum, format_dict_sum = get_types(sector_val)
            highlighting_sum = get_style("partial", sum_data, curryr, currmon, [], [])

            return no_update, no_update, no_update, no_update, no_update, no_update, no_update, sum_data.to_dict('records'), [{'name': ['OOB Initial Flag Summary', sum_data.columns[i]], 'id': sum_data.columns[i], 'type': type_dict_sum[sum_data.columns[i]], 'format': format_dict_sum[sum_data.columns[i]]} 
                                for i in range(0, len(sum_data.columns))], sum_display, highlighting_sum, no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update


@trend.callback([Output('expand_hist', 'value'),
                 Output('subsequent_fix', 'value')],
                 [Input('store_submit_button', 'data'),
                 Input('dropman', 'value'),
                 Input('sector', 'data')],
                 [State('init_trigger', 'data')])
#@Timer("Remove Expand Hist")
def remove_expand_hist(submit_button, drop_val, sector_val, success_init):
    if sector_val is None or success_init == False:
        raise PreventUpdate
    else:
        return ['trunc'], 'r'

@trend.callback([Output('man_view', 'data'),
                Output('man_view', 'columns'),
                Output('man_view_container', 'style'),
                Output('man_view', 'style_data_conditional'),
                Output('key_metrics', 'data'),
                Output('key_metrics', 'columns'),
                Output('key_metrics_container', 'style'),
                Output('key_metrics', 'style_data_conditional'),
                Output('key_metrics_2', 'data'),
                Output('key_metrics_2', 'columns'),
                Output('key_metrics_2_container', 'style'),
                Output('key_metrics_2', 'style_data_conditional'),
                Output('flag_description_noprev', 'children'),
                Output('flag_description_resolved', 'children'),
                Output('flag_description_unresolved', 'children'),
                Output('flag_description_new', 'children'),
                Output('flag_description_skipped', 'children'),
                Output('noprev_container', 'style'),
                Output('resolved_container', 'style'),
                Output('unresolved_container', 'style'),
                Output('new_container', 'style'),
                Output('skipped_container', 'style'),
                Output('vac_series', 'figure'),
                Output('vac_series_container', 'style'),
                Output('rent_series', 'figure'),
                Output('rent_series_container', 'style'),
                Output('comment_cons', 'value'),
                Output('comment_avail', 'value'),
                Output('comment_mrent', 'value'),
                Output('comment_erent', 'value'),
                Output('comment_cons_container', 'style'),
                Output('comment_avail_container', 'style'),
                Output('comment_mrent_container', 'style'),
                Output('comment_erent_container', 'style'),
                Output('key_met_radios_container', 'style'),
                Output('submit_button_container', 'style'),
                Output('preview_button_container', 'style'),
                Output('subsequent_change_container', 'style'),
                Output('display_trigger', 'data')],
                [Input('sector', 'data'),
                Input('dropman', 'value'),
                Input('store_all_buttons', 'data'),
                Input('key_met_radios', 'value'),
                Input('expand_hist', 'value'),
                Input('hide_cd', 'value')],
                [State('has_flag', 'data'),
                State('flag_list', 'data'),
                State('store_orig_cols', 'data'),
                State('curryr', 'data'),
                State('currmon', 'data'),
                State('store_flag_resolve', 'data'),
                State('store_flag_unresolve', 'data'),
                State('store_flag_new', 'data'),
                State('store_flag_skips', 'data'),
                State('init_trigger', 'data'),
                State('store_flag_cols', 'data')])
#@Timer("Output Display")
def output_display(sector_val, drop_val, all_buttons, key_met_val, expand, hide_cd, has_flag, flag_list, orig_cols, curryr, currmon, flags_resolved, flags_unresolved, flags_new, flags_skipped, success_init, flag_cols):  
    
    input_id = get_input_id()
    
    if sector_val is None or success_init == False:
        raise PreventUpdate
    else:

        # Set the display so that the default of no display will update with the correct display dict once the data has been loaded and is ready to be displayed in the Divs
        man_view_display = {'display': 'block', 'width': '99%', 'padding-left': '10px'}
        submit_button_display = {'display': 'inline-block', 'padding-left': '30px', 'padding-top': '30px', 'vertical-align': 'top'}
        preview_button_display = {'display': 'inline-block', 'padding-left': '75px', 'padding-top': '35px', 'vertical-align': 'top'}
        subsequent_radios_display = {'display': 'block', 'padding-left': '15px', 'padding-top': '5px'}
        cons_comment_display = {'padding-left': '150px', 'padding-top': '30px', 'display': 'inline-block'}
        avail_comment_display = {'padding-left': '250px', 'padding-top': '30px', 'display': 'inline-block'}
        mrent_comment_display = {'padding-left': '250px', 'padding-top': '30px', 'display': 'inline-block'}
        erent_comment_display = {'padding-left': '250px', 'padding-top': '30px', 'display': 'inline-block'}
        key_metrics_display = {'display': 'inline-block', 'padding-top': '20px', 'padding-left': '30px', 'width': '94%'}
        vac_series_display = {'width': '49%', 'display': 'inline-block'}
        rent_series_display = {'width': '49%', 'display': 'inline-block', 'padding-left': '50px'}
        key_met_radios_display = {'display': 'inline-block', 'width': '6%', 'padding-top': '20px', 'padding-left': '5px', 'vertical-align': 'bottom'}

        data = use_pickle("in", "main_data_" + sector_val, False, curryr, currmon, sector_val)
        data_full = data.copy() # Need this in case the flag is prior to trunc history, even if expand history not selected
        preview_data = use_pickle("in", "preview_data_" + sector_val, False, curryr, currmon, sector_val)
        shim_data = use_pickle("in", "shim_data_" + sector_val, False, curryr, currmon, sector_val)

        # If the user wants to see the full history for the submarket, use the full history, otherwise truncate the view to curryr - 3 plus last month of curryr - 4
        if "full" in expand:
            False
        else:
            data = data[(data['yr'] >= curryr - 3) | ((data['yr'] == curryr - 4) & (data['currmon'] == 12))]
            if len(preview_data) > 0:
                preview_data = preview_data[(preview_data['yr'] >= curryr - 3) | ((preview_data['yr'] == curryr - 4) & (preview_data['currmon'] == 12))]
            if len(shim_data) > 0:
                shim_data = shim_data[(shim_data['yr'] >= curryr - 3) | ((shim_data['yr'] == curryr - 4) & (shim_data['currmon'] == 12))]
   
        # Drop flag columns to reduce dimensionality
        data = data.drop(flag_cols, axis=1)

        if sector_val != "ind":
            shim_cols = ['inv', 'cons', 'conv', 'demo', 'avail', 'mrent', 'merent']
        else:
            shim_cols = ['inv', 'cons', 'avail', 'mrent', 'merent']

        # Reset the shim view to all nulls, unless there are shims entered
        if len(shim_data) == 0:
            shim_data = data.copy()
            shim_data[shim_cols] = np.nan
            shim_data = shim_data[(shim_data['identity'] == drop_val)]
        shim_data = shim_data[['currmon', 'yr'] + shim_cols]

        # If the user chooses to expand the history displayed in the datatable, ensure that the new shim periods get added, but do not lose the shims already entered if there are some
        if "full" in expand and (shim_data.reset_index().loc[0]['yr'] > curryr - 4 or ((shim_data.reset_index().loc[0]['yr'] == curryr - 4) and (shim_data.reset_index().loc[0]['currmon'] == 12))):
            shim_add = data.copy()
            shim_add = shim_add[['identity', 'currmon', 'yr'] + shim_cols]
            shim_add = shim_add[(shim_add['yr'] < curryr - 4) | ((shim_add['yr'] == curryr - 4) & (shim_add['currmon'] < 12))]
            shim_add = shim_add[(shim_add['identity'] == drop_val)]
            shim_add = shim_add.drop(['identity'], axis=1)
            shim_add[['inv', 'cons', 'avail', 'mrent', 'merent']] = np.nan
            shim_add = shim_add.append(shim_data)
            shim_data = shim_add.copy()
            for col in shim_cols:
                shim_data[col] = np.where(shim_data[col] == '', np.nan, shim_data[col])
            
            use_pickle("out", "shim_data_" + sector_val, shim_data, curryr, currmon, sector_val)

            if len(preview_data) > 0:
                preview_add = data.copy()
                preview_add = preview_add[(preview_add['yr'] < curryr - 4) | ((preview_add['yr'] == curryr - 4) & (preview_add['currmon'] < 12))]
                preview_add = preview_add[(preview_add['identity'] == drop_val)]
                preview_add = preview_add.append(preview_data)
                preview_data = preview_add.copy()
                use_pickle("out", "preview_data_" + sector_val, preview_data, curryr, currmon, sector_val)
        
        # If the user changed the sub they want to edit, reset the shim section and the preview dataset
        if (len(preview_data) > 0 and  drop_val != preview_data[preview_data['sub_prev'] == 1].reset_index().loc[0]['identity']) or (shim_data.reset_index()['identity_row'].str.contains(drop_val).loc[0] == False):
            preview_data = pd.DataFrame()
            shim_data = data.copy()
            shim_data = shim_data[['identity', 'currmon', 'yr'] + shim_cols]
            shim_data = shim_data[(shim_data['identity'] == drop_val)]
            shim_data = shim_data.drop(['identity'], axis=1)
            shim_data[shim_cols] = np.nan
            use_pickle("out", "preview_data_" + sector_val, preview_data, curryr, currmon, sector_val)
            use_pickle("out", "shim_data_" + sector_val, shim_data, curryr, currmon, sector_val)

        # Get the Divs that will display the current flags at the sub, as well as the metrics to highlight based on the flag
        issue_description_noprev, issue_description_resolved, issue_description_unresolved, issue_description_new, issue_description_skipped, display_highlight_list, key_metrics_highlight_list = get_issue(data_full, has_flag, flag_list, flags_resolved, flags_unresolved, flags_new, flags_skipped, curryr, currmon, len(preview_data), "specific", sector_val)
        if len(issue_description_noprev) == 0:
            style_noprev = {'display': 'none'}
        else:
            if has_flag == 0 or has_flag == 2:
                style_noprev = {'padding-left': '10px', 'width': '60%', 'display': 'inline-block', 'font-size': '24px', 'vertical-align': 'top', 'text-align': 'center'}
            else:
                style_noprev = {'padding-left': '10px', 'width': '60%', 'display': 'inline-block', 'font-size': '16px', 'vertical-align': 'top'}
        if len(issue_description_resolved) == 0:
            style_resolved = {'display': 'none'}
        else:
            width = str(len(flags_resolved) * 10) + '%'
            style_resolved = {'padding-left': '10px', 'width': width, 'display': 'inline-block', 'font-size': '16px', 'font-weight': 'bold', 'vertical-align': 'top'}
        if len(issue_description_unresolved) == 0:
            style_unresolved = {'display': 'none'}
        else:
            width = str(len(flags_unresolved) * 10) + '%'
            if len(issue_description_resolved) > 0:
                style_unresolved = {'width': width, 'display': 'inline-block', 'font-size': '16px', 'font-weight': 'bold', 'vertical-align': 'top'}
            else:
                style_unresolved = {'padding-left': '10px', 'width': width, 'display': 'inline-block', 'font-size': '16px', 'font-weight': 'bold', 'vertical-align': 'top'}
        if len(issue_description_new) == 0:
            style_new = {'display': 'none'}
        else:
            width = str(len(flags_new) * 10) + '%'
            if len(issue_description_resolved) > 0 or len(issue_description_unresolved) > 0:
                style_new = {'width': width, 'display': 'inline-block', 'font-size': '16px', 'font-weight': 'bold', 'vertical-align': 'top'}
            else:
                style_new = {'padding-left': '10px', 'width': width, 'display': 'inline-block', 'font-size': '16px', 'font-weight': 'bold', 'vertical-align': 'top'}
        if len(issue_description_skipped) == 0:
            style_skipped = {'display': 'none'}
        else:
            width = str(len(flags_skipped) * 10) + '%'
            if len(issue_description_resolved) > 0 or len(issue_description_unresolved) > 0 or len(issue_description_new) > 0:
                style_skipped = {'width': width, 'display': 'inline-block', 'font-size': '16px', 'vertical-align': 'top'}
            else:
                style_skipped = {'padding-left': '10px', 'width': width, 'display': 'inline-block', 'font-size': '16px', 'vertical-align': 'top'}

        # Call the function to set up the sub time series graphs
        if len(preview_data) > 0:
            data_vac, data_rent = sub_met_graphs(preview_data, "sub", curryr, currmon, sector_val)
        else:
            data_vac, data_rent = sub_met_graphs(data[(data['identity'] == drop_val)], "sub", curryr, currmon, sector_val)

        # Set the data for the main data display, using the correct data set based on whether the user is previewing a shim or not
        if len(preview_data) > 0:
            display_data = preview_data.copy()
        else:
            display_data = data.copy()
            display_data = display_data[(display_data['identity'] == drop_val)]

        shim_data_concat = shim_data.copy()
        if 'trunc' in expand and 'full' not in expand:
            shim_data_concat = shim_data_concat[(shim_data_concat['yr'] >= curryr - 3) | ((shim_data_concat['yr'] == curryr - 4) & (shim_data_concat['currmon'] == 12))]
        shim_data_concat = shim_data_concat.drop(['currmon', 'yr'], axis=1)
        shim_data_concat = shim_data_concat.reset_index(drop=True)
        display_data = display_data.reset_index()
        display_data = display_data.drop(['identity_row'], axis=1)
        for x in list(shim_data_concat.columns):
            shim_data_concat = shim_data_concat.rename(columns={x: x + " shim"})
        display_data = pd.concat([shim_data_concat, display_data], axis=1)

        # If the input is to change the key met values to a different var type, then send that input value through to help set the display cols
        # Otherwise, set the display cols based on the current flag type for the sub
        if key_met_val is None:
            key_met_val = flag_list[0][0]
        display_cols, key_met_cols, key_met_2 = set_display_cols(data, drop_val, key_met_val, sector_val, curryr, currmon)

        display_data = display_frame(display_data, drop_val, display_cols, curryr, sector_val)

        # Set the key metrics display and if this is retail, the other subsector support data, display
        key_metrics = data.copy()
        key_metrics = gen_metrics(key_metrics, drop_val, flag_list[0][0], key_met_cols, curryr, currmon)
        key_metrics = key_metrics.rename(columns={'met_avg_mos_to_last_rensur': 'met_mos_lrsurv', 'sub_avg_mos_to_last_rensur': 'sub_mos_lrsurv', 'met_avg_mos_to_last_vacsur': 'met_mos_lvsurv', 'sub_avg_mos_to_last_vacsur': 'sub_mos_lvsurv'})
        key_metrics = key_metrics.rename(columns={'covvac': 'sub_sur_v_cov', 'covren': 'sub_sur_r_cov', 'sub_sur_v_cov_perc':  'sub_sur_v_cov', 'sub_sur_r_cov_perc': 'sub_sur_r_cov', 'sub_sur_r_cov_perc': 'sub_sur_r_cov', 'met_sur_v_cov_perc': 'met_sur_v_cov', 'met_sur_r_cov_perc': 'met_sur_r_cov' })
        key_metrics = key_metrics.rename(columns={'c_met_sur_r_cov_perc': 'c_met_sur_r_cov', 'c_sub_sur_r_cov_perc': 'c_sub_sur_r_cov', 'n_met_sur_r_cov_perc': 'n_met_sur_r_cov', 'n_sub_sur_r_cov_perc': 'n_sub_sur_r_cov', 'nc_met_sur_r_cov_perc': 'nc_met_sur_r_cov', 'nc_sub_sur_r_cov_perc': 'nc_sub_sur_r_cov'})
        key_metrics = key_metrics.rename(columns={'c_met_sur_r_cov_perc': 'c_met_sur_r_cov', 'c_sub_sur_v_cov_perc': 'c_sub_sur_v_cov', 'n_met_sur_v_cov_perc': 'n_met_sur_v_cov', 'n_sub_sur_v_cov_perc': 'n_sub_sur_v_cov', 'nc_met_sur_v_cov_perc': 'nc_met_sur_v_cov', 'nc_sub_sur_v_cov_perc': 'nc_sub_sur_v_cov'})       
        for x in list(key_metrics.columns):
            key_metrics.rename(columns={x: x.replace("_", " ")}, inplace=True)
        key_metrics = key_metrics.rename(columns={'met g renx mo wgt': 'met grenx mo wgt', 'sub g renx mo wgt': 'sub grenx mo wgt', 'c met g renx mo wgt': 'c met grenx mo wgt',
                                            'c sub g renx mo wgt': 'c sub grenx mo wgt','n met g renx mo wgt': 'n met grenx mo wgt','n sub g renx mo wgt': 'n sub grenx mo wgt',
                                            'nc met g renx mo wgt': 'nc met grenx mo wgt','nc sub g renx mo wgt': 'nc sub grenx mo wgt', 'G mrent 12': 'Gmrent 12'})
        
        highlighting_metrics = get_style("metrics", key_metrics, curryr, currmon, key_metrics_highlight_list, [])
        type_dict_metrics, format_dict_metrics = get_types(sector_val)

        if sector_val == "ret" and len(key_met_2) > 0:
            key_met_2 = key_met_2.rename(columns={'c_met_sur_r_cov_perc': 'c_met_sur_r_cov', 'c_sub_sur_r_cov_perc': 'c_sub_sur_r_cov', 'n_met_sur_r_cov_perc': 'n_met_sur_r_cov', 'n_sub_sur_r_cov_perc': 'n_sub_sur_r_cov', 'nc_met_sur_r_cov_perc': 'nc_met_sur_r_cov', 'nc_sub_sur_r_cov_perc': 'nc_sub_sur_r_cov',
                                                  'c_met_sur_v_cov_perc': 'c_met_sur_v_cov', 'c_sub_sur_v_cov_perc': 'c_sub_sur_v_cov', 'n_met_sur_v_cov_perc': 'n_met_sur_v_cov', 'n_sub_sur_v_cov_perc': 'n_sub_sur_v_cov', 'nc_met_sur_v_cov_perc': 'nc_met_sur_v_cov', 'nc_sub_sur_v_cov_perc': 'nc_sub_sur_v_cov'})
            for x in list(key_met_2.columns):
                key_met_2.rename(columns={x: x.replace("_", " ")}, inplace=True)
            key_met_2 = key_met_2.rename(columns={'c met g renx mo wgt': 'c met grenx mo wgt', 'c sub g renx mo wgt': 'c sub grenx mo wgt',
                                                  'n met g renx mo wgt': 'n met grenx mo wgt', 'n sub g renx mo wgt': 'n sub grenx mo wgt', 
                                                  'nc met g renx mo wgt': 'nc met grenx mo wgt', 'nc sub g renx mo wgt': 'nc sub grenx mo wgt'})

            highlighting_key2 = get_style("partial", key_met_2, curryr, currmon, [], [])
            type_dict_met_2, format_dict_met_2 = get_types(sector_val)
            key_met_2_display = {'display': 'block', 'padding-left': '30px', 'padding-top': '10px', 'width': '95%'}
        else:
            highlighting_key2 = no_update
            type_dict_met_2 = {}
            format_dict_met_2 = {}
            key_met_2_display = {'display': 'none'}

        # Retrieve the 4 shim comments from the dataframe and display them to the user
        comment = data.copy()
        comment = comment[(comment['identity'] == drop_val) & (comment['yr'] == curryr) & (comment['currmon'] == currmon)]
        comment = comment.set_index('identity')
        cons_comment = comment['inv_cons_comment'].loc[drop_val]
        avail_comment = comment['avail_comment'].loc[drop_val]
        mrent_comment = comment['mrent_comment'].loc[drop_val]
        erent_comment = comment['erent_comment'].loc[drop_val]

        if cons_comment == "":
            cons_comment = 'Enter Inv or Cons Shim Note Here'
        if avail_comment == "":
            avail_comment = 'Enter Avail Shim Note Here'
        if mrent_comment == "":
            mrent_comment = 'Enter Mrent Shim Note Here'
        if erent_comment == "":
            erent_comment = 'Enter Erent Shim Note Here'
            
        # Rename display cols to remove spaces so that the whitespace can allow for text wrapping
        for x in list(display_data.columns):
            if "_" in x:
                temp = x.replace("_", " ")
                display_data = display_data.rename(columns={x:temp})
        display_data = display_data.rename(columns={"currmon": "month"})
        display_data = display_data.rename(columns={"G mrent": "Gmrent"})
        display_data = display_data.rename(columns={"G merent": "Gmerent"})
        display_data = display_data.rename(columns={"sqinv": "sq inv"})
        display_data = display_data.rename(columns={"sqcons": "sq cons"})
        display_data = display_data.rename(columns={"sqavail": "sq avail"})
        display_data = display_data.rename(columns={"sqabs": "sq abs"})
        display_data = display_data.rename(columns={"sqvac": "sq vac"})
        display_data = display_data.rename(columns={"sqvac chg": "sq vac chg"})
        display_data = display_data.rename(columns={"sqsren": "sq rent"})

        # Get the row index of the metric to be highlighted in the display table. If this is not an rol flag, the row will always be currmon row, otherwise, it is any row where there is a diff to ROL
        temp = display_data.copy()
        temp['id'] = temp.index
        check_for_rol_row = False
        if len(preview_data) == 0:
            if "rol" in flag_list[0]:
                check_for_rol_row = True
        else:
            if len(flags_new) > 0:
                if "rol" in flags_new[0]:
                    check_for_rol_row = True
            elif len(flags_unresolved) > 0:
                if "rol" in flags_unresolved[0]:
                    check_for_rol_row = True
            else:
                if "rol" in flag_list[0]:
                    check_for_rol_row = True
        if check_for_rol_row == True:
            temp = temp[(temp['yr'] != curryr) | ((temp['month'] != currmon) & (temp['yr'] == curryr))]
            if display_highlight_list[0] == "Gmrent":
                rol_val = "rol G mrent"
            else:
                rol_val = "rol vac"
            temp = temp[(temp[rol_val].isnull() == False) & (temp[display_highlight_list[0]].isnull() == False)]
            temp['diff'] = abs(temp[display_highlight_list[0]].astype(float) - temp[rol_val].astype(float))
            temp = temp[temp['diff'] >= 0.001]
            display_highlight_rows = list(temp['id'])
        else:
            display_highlight_rows = list(temp.tail(1)['id'])

        # Drop the rol vars that were only included to help identify the flag row for highlighting purposes
        display_data = display_data.drop(['rol vac', 'rol G mrent'], axis=1)

        # Do not include conv shim and demo shim columns if the user does not want to see them
        if hide_cd[-1] == "Y":
            display_cols = list(display_data.columns)
            display_cols.remove('conv shim')
            display_cols.remove('demo shim')
            display_data['conv shim'], display_data['demo shim'] = display_data.pop('conv shim'), display_data.pop('demo shim')
            for_highlight = display_data.copy()
            for_highlight = for_highlight.drop(['conv shim', 'demo shim'], axis=1)
        else:
            display_cols = list(display_data.columns)
            for_highlight = display_data.copy()

        type_dict_data, format_dict_data = get_types(sector_val)
        highlighting_display = get_style("full", for_highlight, curryr, currmon, display_highlight_list, display_highlight_rows)

        # Make the shim fields editable
        all_cols = list(display_data.columns)
        shim_cols = list(shim_data_concat.columns)
        edit_dict = {}
        for x in all_cols:
            if x in shim_cols:
                edit_dict[x] = True
            else:
                edit_dict[x] = False

        # Get the submarket name and use it in the data table header
        temp = data.copy()
        sub_name = temp[temp['identity'] == drop_val].reset_index().loc[0]['subname']
        if sub_name != "N/A":
            data_title = sub_name + " Submarket Data"
        else:
            data_title = "Submarket Data"
        
        col_header = []
        for x in list(display_data.columns):
            if "shim" in x:
                col_header.append("Shims")
            else:
                col_header.append(data_title)
    
    return display_data.to_dict('records'), [{'name': [col_header[i], display_data.columns[i]], 'id': display_data.columns[i], 'type': type_dict_data[display_data.columns[i]], 'format': format_dict_data[display_data.columns[i]], 'editable': edit_dict[display_data.columns[i]]} 
                            for i in range(0, len(display_cols))], man_view_display, highlighting_display, key_metrics.to_dict('records'), [{'name': ['Key Metrics', key_metrics.columns[i]], 'id': key_metrics.columns[i], 'type': type_dict_metrics[key_metrics.columns[i]], 'format': format_dict_metrics[key_metrics.columns[i]]} 
                            for i in range(0, len(key_metrics.columns))], key_metrics_display, highlighting_metrics, key_met_2.to_dict('records'), [{'name': ['Other Subsector Data', key_met_2.columns[i]], 'id': key_met_2.columns[i], 'type': type_dict_met_2[key_met_2.columns[i]], 'format': format_dict_met_2[key_met_2.columns[i]]} 
                            for i in range(0, len(key_met_2.columns))], key_met_2_display, highlighting_key2, issue_description_noprev, issue_description_resolved, issue_description_unresolved, issue_description_new, issue_description_skipped, style_noprev, style_resolved, style_unresolved, style_new, style_skipped, go.Figure(data=data_vac), vac_series_display, go.Figure(data=data_rent), rent_series_display, cons_comment, avail_comment, mrent_comment, erent_comment, cons_comment_display, avail_comment_display, mrent_comment_display, erent_comment_display, key_met_radios_display, submit_button_display, preview_button_display, subsequent_radios_display, True
        
@trend.callback([Output('droproll', 'value'),
                Output('roll_trigger', 'data')],
                [Input('store_submit_button', 'data'),
                Input('sector', 'data'),
                Input('dropman', 'value')],
                [State('curryr', 'data'),
                State('currmon', 'data'),
                State('init_trigger', 'data')])
#@Timer("Set Rolldrop")
def set_rolldrop(submit_button, sector_val, drop_val, curryr, currmon, success_init):
    
    if sector_val is None or success_init == False:
        raise PreventUpdate
    else:

        if sector_val == "ind":
            if drop_val[-1:] == "F":
                roll_val= drop_val[:2] + drop_val[-1:]
            else:
                roll_val= drop_val[:2] + drop_val[-2:]
        elif sector_val == "ret":
            roll_val = drop_val[:2] + "Ret"
        else:
            roll_val = drop_val[:2] + drop_val[-3:]

        return roll_val, True

@trend.callback([Output('vac_series_met', 'figure'),
                Output('rent_series_met', 'figure'),
                Output('vac_series_met_container', 'style'),
                Output('rent_series_met_container', 'style'),
                Output('metroll', 'data'),
                Output('metroll', 'columns'),
                Output('metroll_container', 'style'),
                Output('metroll', 'style_data_conditional'),
                Output('metroll', 'page_action'),
                Output('metroll', 'style_table'),
                Output('metroll', 'fixed_rows'),
                Output('met_rank', 'data'),
                Output('met_rank', 'columns'),
                Output('sub_rank', 'data'),
                Output('sub_rank', 'columns'),
                Output('sub_rank_container', 'style'),
                Output('met_rank_container', 'style'),
                Output('rank_toggle_container', 'style'),
                Output('roll_view', 'disabled')],
                [Input('droproll', 'value'),
                Input('dropman', 'value'),
                Input('roll_trigger', 'data'),
                Input('store_submit_button', 'data'),
                Input('store_preview_button', 'data'),
                Input('roll_view', 'value'),
                Input('currmon_filt', 'value'),
                Input('rank_toggle', 'value'),
                Input('display_trigger', 'data')],
                [State('store_orig_cols', 'data'),
                State('curryr', 'data'),
                State('currmon', 'data'),
                State('sector', 'data'),
                State('init_trigger', 'data')])
#@Timer("Output Rollup")
def output_rollup(roll_val, drop_val, roll_trigger, submit_button, preview_button, multi_view, currmon_view, rank_only, display_trigger, orig_cols, curryr, currmon, sector_val, success_init):

    if sector_val is None or success_init == False:
        raise PreventUpdate
    else:
        data = use_pickle("in", "main_data_" + sector_val, False, curryr, currmon, sector_val)
        preview_data = use_pickle("in", "preview_data_" + sector_val, False, curryr, currmon, sector_val)
        
        # If the user is previewing a fix, set the rollup data set used in the rollup function to reflect the previewed edits so the user can see their effect on the met and nat levels
        # Otherwise, the rollup data set can just be a copy of the current edited dataset
        if len(preview_data) > 0:
            data_temp = data.copy()
            filt_cols = orig_cols + ['identity', 'identity_met', 'identity_us', 'metsq_Gmrent', 'metsqcons', 'metsqvacchg', 'metsqabs', 'metsqinv', 'metsqsren', 'curr_tag']
            data_temp = data_temp[filt_cols]
            preview_data_temp = preview_data.copy()
            preview_data_temp = preview_data_temp[filt_cols]
            data_temp = data_temp[(data_temp['identity'] != drop_val)]
            preview_data_temp = preview_data_temp[(preview_data_temp['identity'] == drop_val)]
            data_temp = data_temp.append(preview_data_temp)
            data_temp.sort_values(by=['subsector', 'metcode', 'subid', 'yr', 'currmon'], inplace=True)
            roll = data_temp.copy()
        else:
            roll = data.copy()

        if sector_val == "ret":
            roll['subsector'] = "Ret"
        
        # Call the rollup function to set the rollup data set, as well as the relevant vacancy and rent time series charts for the rollup tab
        if multi_view == False or roll_val[:2] == "US":
            rolled = rollup(roll, roll_val, curryr, currmon, sector_val, "reg")
            if roll_val[:2] == "US":
                rolled = rolled[(rolled['identity_us'] == roll_val)]
            else:
                rolled = rolled[(rolled['metcode'] == roll_val[:2]) & (rolled['subsector'] == roll_val[2:])]

        elif multi_view == True:
            roll_combined = pd.DataFrame()
            all_subs = roll.copy()
            all_subs = all_subs[all_subs['identity_met'] == roll_val]
            all_subs = all_subs.drop_duplicates('identity')
            all_subs = list(all_subs['identity'])
            for ident in all_subs:
                roll_temp = rollup(roll, ident, curryr, currmon, sector_val, "list")
                if len(roll_combined) == 0:
                    roll_combined = roll_temp
                else:
                    roll_combined = roll_combined.append(roll_temp)
            rolled = roll_combined.copy()

            rolled_rank = rollup(roll, roll_val, curryr, currmon, sector_val, "reg")

        if (sector_val == "apt" or sector_val == "off" or sector_val == "ret") and roll_val[:2] != "US":
            testing_sub = data[data['identity_met'] == roll_val].reset_index().loc[0]['subid']
            if (sector_val == "apt" and testing_sub == 90) or (sector_val == "off" and (testing_sub == 81 or testing_sub == 82)) or (sector_val == "ret" and testing_sub == 70):
                rolled = rolled.drop(['merent', 'G_merent', 'rol_G_merent', 'gap', 'gap_chg'], axis=1)        
        
        if roll_val[:2] == "US":
            data_vac_roll, data_rent_roll = sub_met_graphs(rolled, "nat", curryr, currmon, sector_val)
        else:
            if multi_view == False:
                data_vac_roll, data_rent_roll = sub_met_graphs(rolled, "met", curryr, currmon, sector_val)
            elif multi_view == True:
                sub_rank, met_rank = rank_it(rolled_rank, roll, roll_val, curryr, currmon, sector_val, rank_only)
                for col in sub_rank:
                    if "_" in col:
                        sub_rank.rename(columns={col: col.replace('_', ' ')}, inplace=True)
                        met_rank.rename(columns={col: col.replace('_', ' ')}, inplace=True)
                sub_rank = sub_rank.rename(columns={'G mrent': 'Gmrent'})
                met_rank = met_rank.rename(columns={'G mrent': 'Gmrent'})
                rolled = rolled[(rolled['metcode'] == roll_val[:2]) & (rolled['subsector'] == roll_val[2:])]


                if rank_only == False:
                    type_dict_rank = {}
                    format_dict_rank = {}
                    for x in list(sub_rank.columns):
                        type_dict_rank[x] = 'numeric'
                        format_dict_rank[x] = Format(precision=0, scheme=Scheme.fixed)
                elif rank_only == True:
                    type_dict_rank, format_dict_rank = get_types(sector_val)            
        
        if currmon_view == True:
            rolled = rolled[(rolled['yr'] == curryr) & (rolled['currmon'] == currmon)]
            roll_page_action ='none'
            num_rows = len(rolled)
            height = str(max(num_rows * 40, 120)) + 'px'
            roll_style_table={'height': height}
            roll_fixed_rows={}
        else:
            roll_page_action ='none'
            roll_style_table={'height': '460px', 'overflowY': 'auto'}
            roll_fixed_rows={'headers': True}
        
        rolled = rolled.drop(['cons_oob', 'vac_oob', 'vac_chg_oob',  'mrent_oob', 'G_mrent_oob'], axis=1)

        # Rename display cols to remove spaces so that the whiteSpace can allow for text wrapping
        for x in list(rolled.columns):
            if "_" in x:
                temp = x.replace("_", " ")
                rolled = rolled.rename(columns={x:temp})

        rolled = rolled.rename(columns={'currmon': 'month'})

        vac_series_met_display = {'width': '49%', 'display': 'inline-block'}
        rent_series_met_display = {'width': '49%', 'display': 'inline-block', 'padding-left': '50px'}
        sub_rank_display = {'display': 'inline-block', 'padding-left': '10px', 'width': '45%', 'padding-top': '10px'}
        met_rank_display = {'display': 'inline-block', 'padding-left': '150px', 'width': '45%', 'padding-top': '10px'}
        rank_toggle_display = {'display': 'block', 'padding-right': '35px', 'padding-top': '75px'}

        roll_display = {'width': '95%', 'padding-left': '10px', 'display': 'block'}
        
        if sector_val == "ind":
            if roll_val[0:2] == "US":
                if roll_val[2] == "F":
                    data_title = roll_val[:2] + " "  + roll_val[2] + " " + roll_val[3:] + " National Data"
                else:
                    data_title = roll_val[:2] + " "  + roll_val[2:4] + " " + roll_val[4:] + " National Data"
            else:
                data_title = roll_val[:2] + " "  + roll_val[2:] + " Metro Data"
        else:
            if roll_val[0:2] == "US":
                data_title = roll_val[:2] + " Tier " + roll_val[2:] + " National Data"
            else:
                data_title = roll_val[:2] + " Metro Data"
                    
        # Needed to keep rol vars in initially so they could be used in graphs, drop them here so that they dont appear in the datatable output
        for x in list(rolled.columns):
            if "rol" in x:
                rolled = rolled.drop([x], axis=1)
        
        if 'met sur totabs' in list(rolled.columns):
            rolled = rolled.drop(['met sur totabs'], axis=1)
        if 'met g renx mo wgt' in list(rolled.columns):
            rolled = rolled.drop(['met g renx mo wgt'], axis=1)

        rolled = rolled.rename(columns={'metsqinv': 'sq inv', 'metsqcons': 'sq cons', 'metsqavail': 'sq avail', 'metsqvacchg': 'sq vac chg', 'metsqabs': 'sq abs', 'metsqsren': 'sq rent', 'metsq Gmrent': 'sq Gmrent'})
        rolled = rolled.rename(columns={'sqinv': 'sq inv', 'sqcons': 'sq cons', 'sqvac': 'sq vac', 'sqvac chg': 'sq vac chg', 'sqabs': 'sq abs', 'sqsren': 'sq rent'})
        rolled = rolled.rename(columns={'G mrent': 'Gmrent', 'G merent': 'Gmerent', 'metcode': 'met'})

        if multi_view == False:
            rolled.sort_values(by=['yr', 'month'], ascending=[True, True], inplace=True)
        
        if sector_val == "ret":
            rolled = rolled.drop(['subsector'], axis=1)

        if 'identity us' in list(rolled.columns):
            rolled = rolled.drop(['identity us'], axis=1)

        type_dict_roll, format_dict_roll = get_types(roll_val[2:].title())
        highlighting_roll = get_style("full", rolled, curryr, currmon, [], [])

        if roll_val[:2] == "US":
            disable_roll_view = True
        else:
            disable_roll_view = False
    
        if multi_view == False or roll_val[:2] == "US":
            return go.Figure(data=data_vac_roll), go.Figure(data=data_rent_roll), vac_series_met_display, rent_series_met_display, rolled.to_dict('records'), [{'name': [data_title, rolled.columns[i]], 'id': rolled.columns[i], 'type': type_dict_roll[rolled.columns[i]], 'format': format_dict_roll[rolled.columns[i]]} 
            for i in range(0, len(rolled.columns))], roll_display, highlighting_roll, roll_page_action, roll_style_table, roll_fixed_rows, no_update, no_update, no_update, no_update, {'display': 'none'}, {'display': 'none'}, {'display': 'none'}, disable_roll_view
        elif multi_view == True:
            return no_update, no_update, {'display': 'none'}, {'display': 'none'}, rolled.to_dict('records'), [{'name': [data_title, rolled.columns[i]], 'id': rolled.columns[i], 'type': type_dict_roll[rolled.columns[i]], 'format': format_dict_roll[rolled.columns[i]]} 
            for i in range(0, len(rolled.columns))], roll_display, highlighting_roll, roll_page_action, roll_style_table, roll_fixed_rows, met_rank.to_dict('records'), [{'name': ['Met Rank', met_rank.columns[i]], 'id': met_rank.columns[i], 'type': type_dict_rank[met_rank.columns[i]], 'format': format_dict_rank[met_rank.columns[i]]} 
                            for i in range(0, len(met_rank.columns))], sub_rank.to_dict('records'), [{'name': ['Sub Rank', sub_rank.columns[i]], 'id': sub_rank.columns[i], 'type': type_dict_rank[sub_rank.columns[i]], 'format': format_dict_rank[sub_rank.columns[i]]}
                            for i in range(0, len(sub_rank.columns))], sub_rank_display, met_rank_display, rank_toggle_display, disable_roll_view

@trend.callback(Output('store_shim_finals', 'data'),
                  [Input('man_view', 'data'),
                  Input('sector', 'data')],
                  [State('curryr', 'data'),
                  State('currmon', 'data'),
                  State('init_trigger', 'data'),
                  State('dropman', 'value')])
#@Timer("Finalize Shims")
def finalize_shims(shim_data, sector_val, curryr, currmon, success_init, drop_val):
  
    if sector_val is None or success_init == False:
        raise PreventUpdate
    else:
        shims_final = pd.DataFrame()
        for x in shim_data:
            if sector_val != "ind":
                temp = {'identity_row': drop_val + str(x['yr']) + str(x['month']), 'currmon': x['month'], 'yr': x['yr'], 'inv': x['inv shim'], 'cons': x['cons shim'], 'conv': x['conv shim'], 'demo': x['demo shim'], 'avail': x['avail shim'], 'mrent': x['mrent shim'], 'merent': x['merent shim']}
            else:
                temp = {'identity_row': drop_val + str(x['yr']) + str(x['month']), 'currmon': x['month'], 'yr': x['yr'], 'inv': x['inv shim'], 'cons': x['cons shim'], 'avail': x['avail shim'], 'mrent': x['mrent shim'], 'merent': x['merent shim']}
            shims_final = shims_final.append(temp, ignore_index=True)
        shims_final = shims_final.set_index('identity_row')
        use_pickle("out", "shim_data_" + sector_val, shims_final, curryr, currmon, sector_val)
        return False
        
@trend.callback([Output('scatter-xaxis-var', 'options'),
                Output('scatter-yaxis-var', 'options'),
                Output('scatter-yaxis-var', 'value'),
                Output('scatter-xaxis-var', 'disabled'),
                Output('scatter-xaxis-var', 'placeholder')],
                [Input('scatter-type-radios', 'value'),
                Input('aggreg_level', 'value'),
                Input('sector', 'data')],
                [State('curryr', 'data'),
                State('currmon', 'data'),
                State('scatter-xaxis-var', 'value'),
                State('init_trigger', 'data')])
#@Timer("Get Scatter Drops")
def get_scatter_drops(type_value, aggreg_met, sector_val, curryr, currmon, x_var, success_init):
    
    if sector_val is None or success_init == False:
        raise PreventUpdate

    else:
        if type_value == "c":
            options_list_1 = ['cons', 'vac_chg', 'abs', 'G_mrent', 'G_merent', 'gap_chg']
            options_list_2 = options_list_1
            if x_var == "G_mrent":
                y_var = "vac_chg"
            else:
                y_var = "G_mrent"
            lock = False
            placeholder = "Select:"
        elif type_value == "r":
            options_list_1 = []
            options_list_2 = ['rol_cons', 'rol_vac_chg', 'rol_abs', 'rol_G_mrent', 'rol_G_merent', 'rol_gap_chg']
            y_var = "rol_" + x_var
            lock = True
            placeholder="Determined by ROL Variable"
        elif type_value == "q":
            options_list_1 = []
            if aggreg_met == False:
                options_list_2 = ['sqcons', 'sqvac_chg', 'sqabs', 'sq_Gmrent']
                if x_var == "G_mrent":
                    y_var = "sq_Gmrent"
                else:
                    y_var = "sq" + x_var
            elif aggreg_met == True:
                options_list_2 = ['metsqcons', 'metsqvacchg', 'metsqabs', 'metsq_Gmrent']
                if x_var == "vac_chg":
                    y_var = "metsqvacchg"
                elif x_var == "G_mrent":
                    y_var = "metsq_Gmrent"
                else:
                    y_var = "metsq" + x_var
            lock = True
            placeholder="Determined by SQ Variable"
        elif type_value == "s":
            options_list_1 = []
            if aggreg_met == False:
                options_list_2 = ['avail10d', 'dqren10d', 'sub_g_renx_mo_wgt', 'sub_sur_totabs']
            elif aggreg_met == True:
                options_list_2 = ['met_g_renx_mo_wgt', 'met_sur_totabs']
            if x_var == "abs":
                if aggreg_met == False:
                    y_var = "sub_sur_totabs"
                elif aggreg_met == True:
                    y_var = "met_sur_totabs"
            elif x_var == "G_mrent":
                if aggreg_met == False:
                    y_var = "sub_g_renx_mo_wgt"
                elif aggreg_met == True:
                    y_var = "met_g_renx_mo_wgt"
            else:
                if aggreg_met == False:
                    y_var = "sub_sur_totabs"
                elif aggreg_met == True:
                    y_var = "met_sur_totabs"
            lock = True
            placeholder="Determined by Survey Variable"
        
        options_list_1 = sorted(options_list_1, key=lambda v: v.upper())
        options_list_2 = sorted(options_list_2, key=lambda v: v.upper())

    return [{'label': i, 'value': i} for i in options_list_1], [{'label': i, 'value': i} for i in options_list_2], y_var, lock, placeholder

@trend.callback([Output('scatter_graph', 'figure'),
                Output('store_scatter_check', 'data'),
                Output('scatter_container', 'style')],
                [Input('scatter-xaxis-var', 'value'),
                Input('scatter-yaxis-var', 'value'),
                Input('scatter-type-radios', 'value'),
                Input('flags_only', 'value'),
                Input('aggreg_level', 'value'),
                Input('sector', 'data'),
                Input('store_submit_button', 'data')],
                [State('curryr', 'data'),
                State('currmon', 'data'),
                State('init_trigger', 'data'),
                State('store_flag_cols', 'data')])
#@Timer("Produce Scatter")
def produce_scatter_graph(xaxis_var, yaxis_var, type_value, flags_only, aggreg_met, sector_val, submit_button, curryr, currmon, success_init, flag_cols):

    if sector_val is None or success_init == False:
        raise PreventUpdate
    else:
        graph_data = use_pickle("in", "main_data_" + sector_val, False, curryr, currmon, sector_val)

        if type_value == "r":
            xaxis_var = yaxis_var[4:]
        elif type_value == "q":
            if yaxis_var == "sq_Gmrent":
                xaxis_var = "G_mrent"
            else:
                if aggreg_met == False:
                    xaxis_var = yaxis_var[2:]
                elif aggreg_met == True:
                    if yaxis_var == "metsqvacchg":
                        xaxis_var = "vac_chg"
                    else:
                        xaxis_var = yaxis_var[5:]

        elif type_value == "s":
            if yaxis_var in ["avail10d", "sub_sur_totabs", "met_sur_totabs"]:
                xaxis_var = "abs"
            elif yaxis_var in ["dqren10d", "sub_g_renx_mo_wgt", "met_g_renx_mo_wgt"]:
                xaxis_var = "G_mrent"

        # Tag subs as flagged or not flagged based on the xaxis var for color purposes on scatter plot
        if aggreg_met == False:
            
            graph_data[flag_cols] = np.where((graph_data[flag_cols] != 0), 1, graph_data[flag_cols])

            def sum_flags(dataframe_in, flag_list):
                dataframe = dataframe_in.copy()
                dataframe['tot_flags'] = 0
                for flag_name in flag_list:
                    dataframe['tot_flags'] += dataframe.groupby('identity')[flag_name].transform('sum')

                return dataframe


            if type_value == "c":
                if xaxis_var in ['cons']:
                    graph_data['c_flag_tot'] = graph_data.filter(regex="^c_flag*").sum(axis=1)
                    graph_data['flagged_status'] = np.where(graph_data['c_flag_tot'] > 0, 1, 0)
                    graph_data = graph_data.drop(['c_flag_tot'], axis=1) 
                elif xaxis_var in ['vac_chg', 'abs']:
                    graph_data['v_flag_tot'] = graph_data.filter(regex="^v_flag*").sum(axis=1)
                    graph_data['flagged_status'] = np.where(graph_data['v_flag_tot'] > 0, 1, 0)
                    graph_data = graph_data.drop(['v_flag_tot'], axis=1)
                elif xaxis_var in ['G_mrent']:
                    graph_data['g_flag_tot'] = graph_data.filter(regex="^g_flag*").sum(axis=1)
                    graph_data['flagged_status'] = np.where(graph_data['g_flag_tot'] > 0, 1, 0)
                    graph_data = graph_data.drop(['g_flag_tot'], axis=1)
                elif xaxis_var in ['gap_chg', 'G_merent']:
                    graph_data['e_flag_tot'] = graph_data.filter(regex="^e_flag*").sum(axis=1)
                    graph_data['flagged_status'] = np.where(graph_data['e_flag_tot'] > 0, 1, 0)
                    graph_data = graph_data.drop(['e_flag_tot'], axis=1)
            
            elif type_value == "r":
                if xaxis_var in ['cons']:
                    graph_data = sum_flags(graph_data, ['c_flag_rolv', 'c_flag_rolg'])
                    graph_data['flagged_status'] = np.where(graph_data['tot_flags'] > 0, 1, 0)
                    graph_data = graph_data.drop(['tot_flags'], axis=1)
                elif xaxis_var in ['vac_chg', 'abs']:
                    graph_data = sum_flags(graph_data, ['v_flag_rol'])
                    graph_data['flagged_status'] = np.where(graph_data['tot_flags'] > 0, 1, 0)
                    graph_data = graph_data.drop(['tot_flags'], axis=1)
                elif xaxis_var in ['G_mrent']:
                    graph_data = sum_flags(graph_data, ['g_flag_rol'])
                    graph_data['flagged_status'] = np.where(graph_data['tot_flags'] > 0, 1, 0)
                    graph_data = graph_data.drop(['tot_flags'], axis=1)
                elif xaxis_var in ['gap_chg', 'G_merent']:
                    graph_data['flagged_status'] = 0

            elif type_value == "q":
                if xaxis_var in ['cons']:
                    if sector_val == "apt":
                        graph_data = sum_flags(graph_data, ['c_flag_sqdiff'])
                        graph_data['flagged_status'] = np.where(graph_data['tot_flags'] > 0, 1, 0)
                        graph_data = graph_data.drop(['tot_flags'], axis=1)
                    else:
                        graph_data['flagged_status'] = 0
                elif xaxis_var in ['vac_chg', 'abs']:
                    if sector_val == "apt":
                        flag_list = ['v_flag_sqlev', 'v_flag_sqabs', 'v_flag_level']
                    else:
                        flag_list = ['v_flag_sqabs', 'v_flag_level']
                    graph_data = sum_flags(graph_data, flag_list)
                    graph_data['flagged_status'] = np.where(graph_data['tot_flags'] > 0, 1, 0)
                    graph_data = graph_data.drop(['tot_flags'], axis=1)
                elif xaxis_var in ['G_mrent']:
                    if sector_val == "apt":
                        flag_list = ['g_flag_sqlev', 'g_flag_sqdir', 'g_flag_sqdiff']
                    else:
                        flag_list = ['g_flag_sqdir', 'g_flag_sqdiff']
                    graph_data = sum_flags(graph_data, flag_list)
                    graph_data['flagged_status'] = np.where(graph_data['tot_flags'] > 0, 1, 0)
                    graph_data = graph_data.drop(['tot_flags'], axis=1)
                elif xaxis_var in ['gap_chg', 'G_merent']:
                    graph_data['flagged_status'] = 0
            elif type_value == "s":
                if xaxis_var in ['cons']:
                    graph_data['flagged_status'] = 0
                    graph_data = graph_data.drop(['tot_flags'], axis=1)
                elif xaxis_var in ['vac_chg', 'abs']:
                    graph_data = sum_flags(graph_data, ['v_flag_surabs', 'v_flag_rsent', 'v_flag_low', 'v_flag_high'])
                    graph_data['flagged_status'] = np.where(graph_data['tot_flags'] > 0, 1, 0)
                    graph_data = graph_data.drop(['tot_flags'], axis=1)
                elif xaxis_var in ['G_mrent']:
                    graph_data = sum_flags(graph_data, ['g_flag_consp', 'g_flag_consn', 'g_flag_large', 'g_flag_surdiff'])
                    graph_data['flagged_status'] = np.where(graph_data['tot_flags'] > 0, 1, 0)
                    graph_data = graph_data.drop(['tot_flags'], axis=1)
                elif xaxis_var in ['gap_chg', 'G_merent']:
                    graph_data = sum_flags(graph_data, ['e_flag_low', 'e_flag_high', 'e_flag_perc', 'e_flag_mdir'])
                    graph_data['flagged_status'] = np.where(graph_data['tot_flags'] > 0, 1, 0)
                    graph_data = graph_data.drop(['tot_flags'], axis=1)

        elif aggreg_met == True:
            graph_data['flagged_status'] = 0

        if aggreg_met == True:
            graph_data = rollup(graph_data, "temp", curryr, currmon, sector_val, "reg")
            
        graph_data = graph_data[graph_data['yr'] >= curryr - 2]

        scatter_graph = filter_graph(graph_data, curryr, currmon, type_value, xaxis_var, yaxis_var, sector_val, flags_only, aggreg_met)
    
        scatter_layout = create_scatter_plot(scatter_graph, xaxis_var, yaxis_var, type_value, curryr, currmon, sector_val, aggreg_met)

        # Need to set this variable so that the succeeding callbacks will only fire once the scatter callback is done. 
        # This works because it makes the callbacks that use elements produced in this callback have an input that is linked to an output of this callback, ensuring that they will only be fired once this one completes
        scatter_check = 1

        return scatter_layout, scatter_check, {'width': '49%', 'display': 'inline-block', 'padding-left': '30px'}

@trend.callback([Output('x_time_series', 'figure'),
                Output('x_ts_container', 'style'),
                Output('y_time_series', 'figure'),
                Output('y_ts_container', 'style')],
                [Input('scatter_graph', 'hoverData'),
                Input('scatter-xaxis-var', 'value'),
                Input('scatter-yaxis-var', 'value'),
                Input('sector', 'data'),
                Input('store_scatter_check', 'data'),
                Input('init_trigger', 'data')],
                [State('curryr', 'data'),
                State('currmon', 'data'),
                State('scatter-type-radios', 'value'),
                State('aggreg_level', 'value'),
                State('init_trigger', 'data')])
#@Timer("Produce Timeseries")
def produce_timeseries(hoverData, xaxis_var, yaxis_var, sector_val, scatter_check, init_trigger, curryr, currmon, type_value, aggreg_met, success_init):
    
    if sector_val is None or success_init == False:
        raise PreventUpdate
    else:
        graph = use_pickle("in", "main_data_" + sector_val, False, curryr, currmon, sector_val)
        
        graph = graph[graph['yr'] >= curryr - 2]

        if aggreg_met == True:
            graph = rollup(graph, "temp", curryr, currmon, sector_val, "reg")
            if sector_val == "ret":
                graph['identity_met'] = graph['metcode'] + "Ret"
            else:
                graph['identity_met'] = graph['metcode'] + graph['subsector']

        # If this is not a c type, check the yvar selection and set the xvar to the appropriate corresponding var
        if type_value == "r":
            xaxis_var = yaxis_var[4:]
        elif type_value == "q":
            if yaxis_var == "sq_Gmrent" or yaxis_var == "metsq_Gmrent":
                xaxis_var = "G_mrent"
            else:
                if aggreg_met == False:
                    xaxis_var = yaxis_var[2:]
                elif aggreg_met == True:
                    if yaxis_var == "metsqvacchg":
                        xaxis_var = "vac_chg"
                    else:
                        xaxis_var = yaxis_var[5:]
        elif type_value == "s":
            if yaxis_var == "avail10d" or yaxis_var == "sub_sur_totabs" or yaxis_var == "met_sur_totabs":
                xaxis_var = "abs"
            elif yaxis_var == "dqren10d" or yaxis_var == "sub_g_renx_mo_wgt" or yaxis_var == "met_g_renx_mo_wgt":
                xaxis_var = "G_mrent"

        if aggreg_met == True and xaxis_var == "G_mrent":
            for x in ['G_mrent', 'met_g_renx_mo_wgt']:
                graph["absolute_" + x] = abs(graph[x])
                graph[x + '_perc'] = graph[(graph['yr'] == curryr) & (graph['currmon'] == currmon)]["absolute_" + x].rank(pct=True)
                graph.drop(["absolute_" + x], axis=1, inplace=True)


        # Determine the init hover if nothing was hovered over yet, and also change the hover from what was previously hovered over if there is a variable change and the identity no longer has a diff to published for types r, q, and s
        no_diff_points = False
        if hoverData == None and type_value != "c":
            temp = graph.copy()
            if type_value == "r":
                temp = temp[(temp['yr'] >= curryr - 1) | ((temp['yr'] == curryr - 2) & (temp['currmon'] >  0 + currmon))]
            elif type_value == "q" or type_value == "s":
                temp = temp[(temp['yr'] == curryr) & (temp['currmon'] == currmon)]
            temp['diff'] = temp[xaxis_var] - temp[yaxis_var]
            temp = temp[(abs(temp['diff']) > 0.001) & (np.isnan(temp[yaxis_var]) == False)]
            if aggreg_met == False:
                diff_list = list(temp['identity'])
            elif aggreg_met == True:
                diff_list = list(temp['identity_met'])
            temp = temp.reset_index()
            
            if len(temp) > 0:
                no_diff_points = False
                if hoverData is None:
                    if aggreg_met == False:
                        sub_choice = temp['identity'].loc[0]
                        hoverData = {'points': [{'customdata': sub_choice}]}
                    elif aggreg_met == True:
                        met_choice = temp['identity_met'].loc[0]
                        hoverData = {'points': [{'customdata': met_choice}]}
                elif hoverData['points'][0]['customdata'] not in diff_list:
                    if aggreg_met == False:
                        sub_choice = temp['identity'].loc[0]
                        hoverData = {'points': [{'customdata': sub_choice}]}
                    elif aggreg_met == True:
                        met_choice = temp['identity_met'].loc[0]
                        hoverData = {'points': [{'customdata': met_choice}]}
            else:
                no_diff_points = True
        elif hoverData == None and type_value == "c":
            no_diff_points = False
            if aggreg_met == False:
                first_sub = graph['identity'].iloc[0]
                hoverData = {'points': [{'customdata': first_sub}]}
            elif aggreg_met == True:
                first_met = graph['identity_met'].iloc[0]
                hoverData = {'points': [{'customdata': first_met}]}

        if no_diff_points == False:
            # Filter out the correct metsub based on the point the user is hovering over
            if aggreg_met == False:
                graph = graph[graph['identity'] == hoverData['points'][0]['customdata']]
            elif aggreg_met == True:
                graph = graph[graph['identity_met'] == hoverData['points'][0]['customdata']]

            graph = graph[(graph['yr'] >= curryr - 1) | ((graph['yr'] == curryr - 2) & (graph['currmon'] >  0 + currmon))]

            graph_copy = graph.copy()

            if aggreg_met == False:
                graph = pd.melt(graph, id_vars=['subsector', 'metcode', 'subid', 'yr', 'currmon'])
            elif aggreg_met == True:
                graph = pd.melt(graph, id_vars=['subsector', 'metcode', 'yr', 'currmon'])

            identity = hoverData['points'][0]['customdata']
            if sector_val == "apt" or sector_val == "off" or (sector_val == "ret" and aggreg_met == True):
                identity = identity[:-3]

            fig_x = go.Figure()
            fig_y = go.Figure()

            if xaxis_var == "cons":
                x_var_1 = "cons"
                x_var_2 = False
            elif xaxis_var == "vac" or xaxis_var == "vac_chg":
                x_level_var = "vac"
                x_chg_var = "vac_chg"
            elif xaxis_var == "abs":
                x_var_1 = "abs"
                if type_value == "s":
                    x_var_2 = yaxis_var
                else:
                    x_var_2 = False
            elif xaxis_var == "mrent" or xaxis_var == "G_mrent":
                x_level_var = "mrent"
                x_chg_var = "G_mrent"
                if type_value == "s":
                    x_var_1 = "G_mrent"
                    x_var_2 = yaxis_var
                else:
                    x_var_2 = False
            elif xaxis_var == "merent" or xaxis_var == "G_merent":
                x_level_var = "merent"
                x_chg_var = "G_merent"
            elif xaxis_var == "gap" or xaxis_var == "gap_chg":
                x_level_var = "gap"
                x_chg_var = "gap_chg"

            
            if yaxis_var == "cons":
                y_var_1 = "cons"
                y_var_2 = False
            elif yaxis_var == "vac" or yaxis_var == "vac_chg":
                y_level_var = "vac"
                y_chg_var = "vac_chg"
            elif yaxis_var == "abs":
                y_var_1 = "abs"
                y_var_2 = False
            elif yaxis_var == "mrent" or yaxis_var == "G_mrent":
                y_level_var = "mrent"
                y_chg_var = "G_mrent"
            elif yaxis_var == "merent" or yaxis_var == "G_merent":
                y_level_var = "merent"
                y_chg_var = "G_merent"
            elif yaxis_var == "gap" or yaxis_var == "gap_chg":
                y_level_var = "gap"
                y_chg_var = "gap_chg"
            elif yaxis_var == "rol_cons":
                y_var_1 = "rol_cons"
                y_var_2 = False
            elif yaxis_var == "rol_vac_chg":
                y_level_var = "rol_vac"
                y_chg_var = "rol_vac_chg"
            elif yaxis_var == "rol_abs":
                y_var_1 = "rol_abs"
                y_var_2 = False
            elif yaxis_var == "rol_G_mrent":
                y_level_var = "rol_mrent"
                y_chg_var = "rol_G_mrent"
            elif yaxis_var == "rol_G_merent":
                y_level_var = "rol_merent"
                y_chg_var = "rol_G_merent"
            elif yaxis_var == "rol_gap_chg":
                y_level_var = "rol_gap"
                y_chg_var = "rol_gap_chg"
            elif yaxis_var == "sqcons":
                y_var_1 = "sqcons"
                y_var_2 = False
            elif yaxis_var == "sqvac_chg":
                y_level_var = "sqvac"
                y_chg_var = "sqvac_chg"
            elif yaxis_var == "sqabs":
                y_var_1 = "sqabs"
                y_var_2 = False
            elif yaxis_var == "sq_Gmrent":
                y_level_var = "sqsren"
                y_chg_var = "sq_Gmrent"
            elif yaxis_var == "metsqcons":
                y_var_1 = "metsqcons"
                y_var_2 = False
            elif yaxis_var == "metsqvacchg":
                y_level_var = "metsqvac"
                y_chg_var = "metsqvacchg"
            elif yaxis_var == "metsqabs":
                y_var_1 = "metsqabs"
                y_var_2 = False
            elif yaxis_var == "metsq_Gmrent":
                y_level_var = "metsqsren"
                y_chg_var = "metsq_Gmrent"

            if xaxis_var == "abs":
                x_numer_list = ["abs"]
            else:
                x_numer_list = ["cons"]
            x_denomer_list = ['inv']

            if "abs" in yaxis_var and "sur" not in yaxis_var:
                y_numer_list = [yaxis_var]
            elif yaxis_var == "avail10d":
                y_numer_list = ['abs', yaxis_var]
            elif yaxis_var == "sub_sur_totabs" or yaxis_var == "met_sur_totabs":
                y_numer_list = ['abs', yaxis_var]
            elif yaxis_var == "dqren10d":
                y_numer_list = ['G_mrent', yaxis_var]
            elif yaxis_var == "sub_g_renx_mo_wgt" or yaxis_var == "met_g_renx_mo_wgt":
                y_numer_list = ['G_mrent', yaxis_var]
            else:
                y_numer_list = ['cons']

            if type_value == "c":
                y_denomer_list = ["inv"]
            elif type_value == "r":
                y_denomer_list = ["rol_inv"]
            elif type_value  == "q":
                if aggreg_met == False:
                    y_denomer_list = ["sqinv"]
                elif aggreg_met == True:
                    y_denomer_list = ["metsqinv"]
            elif type_value == "s":
                y_denomer_list = ['inv', 'inv']

            graph['x_axis'] = graph['yr'].astype(str) + "m" + graph['currmon'].astype(str)

            
            x_bar_range_list, x_bar_dtick, x_bar_tick0 = set_bar_scale(graph_copy, sector_val, x_numer_list, x_denomer_list, type_value, curryr, currmon)
            y_bar_range_list, y_bar_dtick, y_bar_tick0 = set_bar_scale(graph_copy, sector_val, y_numer_list, y_denomer_list, type_value, curryr, currmon)

            if type_value == "c":
                if xaxis_var == "abs":
                    bar_color_1 = "darkorange"
                bar_color_1 = "mediumseagreen"
                bar_color_2 = False
                line_color = "darkgreen"
            elif type_value == "r":
                bar_color_1 = "palevioletred"
                bar_color_2 = False
                line_color = "red"
            elif type_value == "q":
                bar_color_1 = "lightskyblue"
                bar_color_2 = False
                line_color = "blue"
            elif type_value == "s":
                bar_color_1 = "plum"
                bar_color_2 = "mediumseagreen"
                line_color = "purple"

            if "cons" in xaxis_var or "abs" in xaxis_var or "avail10d" in yaxis_var or "sub_sur_totabs" in yaxis_var or "dqren10d" in yaxis_var or "sub_g_renx_mo_wgt" in yaxis_var or "met_sur_totabs" in yaxis_var or "met_g_renx_mo_wgt" in yaxis_var:
                fig_x = set_ts_bar(fig_x, graph, x_var_1, x_var_2, curryr, currmon, type_value, bar_color_1, bar_color_2, sector_val)
                if type_value == "s":
                    title_var = x_var_2
                    bar_range_list = y_bar_range_list
                    bar_dtick = y_bar_dtick
                    bar_tick0 = y_bar_tick0
                else:
                    title_var = x_var_1
                    bar_range_list = x_bar_range_list
                    bar_dtick = x_bar_dtick
                    bar_tick0 = x_bar_tick0
                fig_x = set_ts_layout(fig_x, title_var, identity, [], False, False, curryr, currmon, "Bar", bar_range_list, sector_val, type_value, bar_dtick, bar_tick0)    
            else:
                fig_x = set_ts_scatter(fig_x, graph, x_level_var, x_chg_var, "mediumseagreen", "darkgreen", curryr, currmon)
                y_tick_range, dtick, tick_0 = set_y2_scale(graph, "ts", x_level_var, sector_val)
                fig_x = set_ts_layout(fig_x, x_chg_var, identity, y_tick_range, dtick, tick_0, curryr, currmon, "Scatter", x_bar_range_list, sector_val, type_value, x_bar_dtick, False)

            if "avail10d" in yaxis_var or "sub_sur_totabs" in yaxis_var or "dqren10d" in yaxis_var or "sub_g_renx_mo_wgt" in yaxis_var or "met_sur_totabs" in yaxis_var or "met_g_renx_mo_wgt" in yaxis_var:
                fig_y = go.Figure()
            elif "cons" in yaxis_var or ("abs" in yaxis_var and "tot" not in yaxis_var):
                fig_y = set_ts_bar(fig_y, graph, y_var_1, y_var_2, curryr, currmon, type_value, bar_color_1, bar_color_2, sector_val)
                fig_y = set_ts_layout(fig_y, y_var_1, identity, [], False, False, curryr, currmon, "Bar", y_bar_range_list, sector_val, type_value, y_bar_dtick, y_bar_tick0)    
            else:
                fig_y = set_ts_scatter(fig_y, graph, y_level_var, y_chg_var, bar_color_1, line_color, curryr, currmon)
                y_tick_range, dtick, tick_0 = set_y2_scale(graph, "ts", y_level_var, sector_val)
                fig_y = set_ts_layout(fig_y, y_chg_var, identity, y_tick_range, dtick, tick_0, curryr, currmon, "Scatter", y_bar_range_list, sector_val, type_value, y_bar_dtick, False)

            if type_value == "s":
                y_ts_display = {'display': 'none'}
            else:
                y_ts_display = {'padding-top': '25px'}

            x_ts_display = {}
        
        elif no_diff_points == True:
            fig_x = go.Figure()
            fig_y = go.Figure()
            x_ts_display = {}
            y_ts_display = {'padding-top': '25px'}
            title_name_x = '<b>{}</b><br>{}'.format("No Submarkets With Difference", xaxis_var)
            title_name_y = '<b>{}</b><br>{}'.format("No Submarkets With Difference", yaxis_var)

            fig_x.update_layout(
                                title={
                                    'text': title_name_x,
                                    'y':0.95,
                                    'x':0.5,
                                    'xanchor': 'center',
                                    'yanchor': 'top'},
                                height=350,
                                margin={'l': 70, 'b': 30, 'r': 10, 't': 70, 'pad': 20},
                                yaxis=dict(
                                    showgrid=False)
                                )
            fig_y.update_layout(
                                title={
                                    'text': title_name_y,
                                    'y':0.95,
                                    'x':0.5,
                                    'xanchor': 'center',
                                    'yanchor': 'top'},
                                height=350,
                                margin={'l': 70, 'b': 30, 'r': 10, 't': 70, 'pad': 20},
                                yaxis=dict(
                                    showgrid=False)
                                )

    return fig_x, x_ts_display, fig_y, y_ts_display

@trend.callback(Output('home-url','pathname'),
                  [Input('logout-button','n_clicks')])
#@Timer("Logout")
def logout(n_clicks):
    '''clear the session and send user to login'''
    if n_clicks is None or n_clicks==0:
        return no_update
    session['authed'] = False
    return '/login'


server_check = os.getcwd()
    
if server_check[0:6] == "\\Odin":
    server = 0
else:
    server = 1


if __name__ == '__main__':
    
    if server == 1:
        test_ports = [8080, 8050, 8020, 8010, 8000]
        for x in test_ports:
            try:
                print("Trying port %d" % (x))
                trend.run_server(port=x, host='0.0.0.0')
                break
            except:
                print("Port being used, trying another")
    elif server == 0:
        trend.run_server(debug=True)