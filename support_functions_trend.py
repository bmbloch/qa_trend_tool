import numpy as np
import pandas as pd
import math
from IPython.core.display import display, HTML
import re
from pathlib import Path
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_core_components as dcc
import itertools
import plotly.graph_objs as go 
import textwrap

from init_load_trend import get_home
from timer_trend import Timer

# Function that filters the dataframe for the columns to display on the data tab to the user, based on what type of flag is currently being analyzed
def set_display_cols(dataframe_in, identity_val, variable_fix, sector_val, curryr, currmon, flag_list, test_auto_rebench, message):
    dataframe = dataframe_in.copy()

    dataframe = dataframe[dataframe['identity'] == identity_val]

    # Note: leave rol_vac and rol_G_mrent in so that it can be used to identify row where diff to rol is for highlighting purposes, will be dropped before final output of datatable
    if sector_val != "ind":
        display_cols = ['identity_row', 'inv shim', 'cons shim', 'conv shim', 'demo shim', 'avail shim', 'mrent shim', 'merent shim', 'yr', 'currmon', 'inv', 'cons', 'sqcons', 'conv', 'demo', 'vac',  'rol_vac', 'vac_chg', 'sqvac', 'sqvac_chg', 'occ', 'avail', 'sqavail', 'abs', 'sqabs', 'mrent', 'G_mrent', 'sqsren', 'sq_Gmrent', 'merent', 'G_merent', 'gap', 'gap_chg', 'rol_G_mrent']
    else:
        display_cols = ['identity_row', 'inv shim', 'cons shim', 'avail shim', 'mrent shim', 'merent shim', 'yr', 'currmon', 'inv', 'cons', 'sqcons', 'vac', 'rol_vac', 'vac_chg', 'sqvac', 'sqvac_chg', 'occ', 'avail', 'sqavail', 'abs', 'sqabs', 'mrent', 'G_mrent', 'rol_G_mrent', 'sqsren', 'sq_Gmrent', 'merent', 'G_merent', 'gap', 'gap_chg']
    
    if "c_flag_rolv" in flag_list:
        if sector_val == "ind":
            display_cols.insert(20, 'rol_abs')
        else:
            display_cols.insert(24, 'rol_abs')

    if test_auto_rebench == True or "governance" in message:
        if sector_val == "ind":
            if "vacancy" in message:
                display_cols.insert(16, 'rol_vac_chg')
                display_cols.insert(19, 'rol_avail')
                display_cols.remove('sqvac')
                display_cols.remove('sqvac_chg')
                display_cols.remove('sqavail')
            elif "market rent" in message:
                display_cols.insert(24, 'rol_mrent')
                display_cols.remove('sqsren')
            elif "effective rent" in message:
                display_cols.insert(30, 'rol_merent')
        else:
            if "vacancy" in message:
                display_cols.insert(20, 'rol_vac_chg')
                display_cols.insert(23, 'rol_avail')
                display_cols.remove('sqvac')
                display_cols.remove('sqvac_chg')
                display_cols.remove('sqavail')
            elif "market rent" in message:
                display_cols.insert(28, 'rol_mrent')
                display_cols.remove('sqsren')
            elif "effective rent" in message:
                display_cols.insert(34, 'rol_merent')
    
    if dataframe['merent'].isnull().all(axis=0) == True:
        display_cols.remove('merent')
        display_cols.remove('G_merent')
        display_cols.remove('gap')
        display_cols.remove('gap_chg')

    if variable_fix == "c":
        key_met_cols = ['newncsf', 'newncava', 'ncrenlev', 'newncrev', 'cons_roldiff', 'vac_roldiff', 'gmrent_roldiff']
    elif variable_fix == "v":
        key_met_cols = ['vac_chg_12', 'sqvac_chg_12', 'ss_vac_chg', 'vac_roldiff', 'newncava', 'nc_surabs', 'vacdrops', 'vacflats', 'vacincrs', 'met_sur_totabs', 'met_sur_v_cov_perc', 'met_avg_mos_to_last_vacsur', 'avail10d', 'sub_sur_totabs', 'sub_sur_v_cov_perc', 'sub_avg_mos_to_last_vacsur']

        if sector_val != "apt":
            key_met_cols.remove('vac_chg_12')
            key_met_cols.remove('sqvac_chg_12')
    elif variable_fix == "g":
        key_met_cols = ['ncrenlev', 'newncrev', 'dqren10d', 'ss_rent_chg', 'sub1to99_Grenx', 'G_mrent_12', 'sq_Gmrent_12', 'rentdrops', 'rentflats', 'rentincrs', 'gmrent_roldiff', 'met_g_renx_mo_wgt', 'met_sur_r_cov_perc', 'met_avg_mos_to_last_rensur', 'sub_g_renx_mo_wgt', 'sub_sur_r_cov_perc', 'sub_avg_mos_to_last_rensur']
        if sector_val != "apt":
            key_met_cols.remove('G_mrent_12')
            key_met_cols.remove('sq_Gmrent_12')
    elif variable_fix == "e":
        key_met_cols = ['gmerent_roldiff', 'gap_perc_5', 'gap_perc_95']

    dataframe[['cons_roldiff', 'vac_roldiff', 'gmrent_roldiff']] = np.where(dataframe[['cons_roldiff', 'vac_roldiff', 'gmrent_roldiff']] == 0, np.nan, dataframe[['cons_roldiff', 'vac_roldiff', 'gmrent_roldiff']])
    dataframe['cons_roldiff'] = dataframe['cons_roldiff'].fillna(method='ffill')
    dataframe['vac_roldiff'] = dataframe['vac_roldiff'].fillna(method='ffill')
    dataframe['gmrent_roldiff'] = dataframe['gmrent_roldiff'].fillna(method='ffill')
    dataframe = dataframe[(dataframe['yr'] == curryr) & (dataframe['currmon'] == currmon)]
    dataframe = dataframe.set_index("identity")

    if sector_val == "ret" and identity_val[-2:] != "NC":
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/ret_combined_insight_data.pickle".format(get_home()))
        insight_stats = pd.read_pickle(file_path)
        insight_stats = insight_stats.reset_index()
        insight_stats = insight_stats[insight_stats['identity'] == identity_val[:-1]]

        if variable_fix == "v":
            if identity_val[-1] == "C":
                key_met_2 = ['n_met_sur_totabs', 'n_met_sur_v_cov_perc', 'n_sub_sur_totabs', 'n_sub_sur_v_cov_perc']
            elif identity_val[-1] == "N":
                key_met_2 = ['c_met_sur_totabs', 'c_met_sur_v_cov_perc', 'c_sub_sur_totabs', 'c_sub_sur_v_cov_perc']
            
            key_met_2 = key_met_2 + ['nc_met_sur_totabs', 'nc_met_sur_v_cov_perc', 'nc_sub_sur_totabs', 'nc_sub_sur_v_cov_perc']
            key_met_2 = [x for x in key_met_2 if math.isnan(insight_stats[x]) == False]
            insight_stats = insight_stats[key_met_2]
        elif variable_fix == "g":
            if identity_val[-1] == "C":
                key_met_2 = ['n_met_g_renx_mo_wgt', 'n_met_sur_r_cov_perc', 'n_sub_g_renx_mo_wgt', 'n_sub_sur_r_cov_perc']
            elif identity_val[-1] == "N":
                key_met_2 = ['c_met_g_renx_mo_wgt', 'c_met_sur_r_cov_perc', 'c_sub_g_renx_mo_wgt', 'c_sub_sur_r_cov_perc']
            
            key_met_2 = key_met_2 + ['nc_met_g_renx_mo_wgt', 'nc_met_sur_r_cov_perc', 'nc_sub_g_renx_mo_wgt', 'nc_sub_sur_r_cov_perc']
            key_met_2 = [x for x in key_met_2 if math.isnan(insight_stats[x]) == False]
            insight_stats = insight_stats[key_met_2]
        else:
            insight_stats = pd.DataFrame()

    else:
        insight_stats = pd.DataFrame()
    
    key_met_cols = [x for x in key_met_cols if math.isnan(dataframe[x]) == False or x == "dqren10" or x == "avail10d"]
   
    return display_cols, key_met_cols, insight_stats

# Function to creat the main display on the data tab
def display_frame(dataframe, identity_val, display_cols, curryr, sector_val):
    dataframe = dataframe.reset_index()
    dataframe = dataframe.rename(columns={"index": "identity_row"})
    dataframe = dataframe.set_index("identity")
    dataframe = dataframe.loc[identity_val]
    dataframe = dataframe[display_cols]
    dataframe = dataframe.reset_index()
    dataframe = dataframe.set_index("identity_row")
    dataframe = dataframe.drop(['identity'], axis =1)
    for z in display_cols[1:]:
        dataframe[z] = dataframe[z].apply(lambda x: '' if pd.isnull(x) else x)
    
    return dataframe

# Function to determine what key metrics to display to user based on the type of flag                    
def gen_metrics(dataframe_in, identity_val, variable_fix, key_met_cols, curryr, currmon):
    
    dataframe = dataframe_in.copy()
    dataframe = dataframe.reset_index()
    dataframe = dataframe[(dataframe['identity'] == identity_val)]
    dataframe[['cons_roldiff', 'vac_roldiff', 'gmrent_roldiff']] = np.where(dataframe[['cons_roldiff', 'vac_roldiff', 'gmrent_roldiff']] == 0, np.nan, dataframe[['cons_roldiff', 'vac_roldiff', 'gmrent_roldiff']])
    dataframe['cons_roldiff'] = dataframe['cons_roldiff'].fillna(method='ffill')
    dataframe['vac_roldiff'] = dataframe['vac_roldiff'].fillna(method='ffill')
    dataframe['gmrent_roldiff'] = dataframe['gmrent_roldiff'].fillna(method='ffill')
    dataframe = dataframe[(dataframe['yr'] == curryr) & (dataframe['currmon'] == currmon)]
    dataframe = dataframe.set_index("identity")
    
    dataframe = dataframe[key_met_cols]
    dataframe = pd.DataFrame(dataframe.loc[identity_val]).transpose()
    for z in key_met_cols:
        dataframe[z] = dataframe[z].apply(lambda x: '' if pd.isnull(x) else x)
    
    return dataframe
    
# This function will roll up the data on a metro or national level for review based on the selection of metro by the user on the Rollups tab
def rollup(dataframe, drop_val, curryr, currmon, sector_val, filt_type):
    roll = dataframe.copy()
    
    if filt_type == "reg":
        if drop_val[:2] == "US":
            identity_filt = 'identity_us'
        else:
            identity_filt = 'identity_met'
    else:
        identity_filt = 'identity'
        roll = roll[roll[identity_filt] == drop_val]

    roll['askrevenue'] = roll['inv'] * roll['mrent']
    roll['effrevenue'] = roll['inv'] * roll['merent']
    roll['oobaskrevenue'] = roll['inv_oob'] * roll['mrent_oob']

    if drop_val[:2] == "US":
        roll['sqaskrev'] = roll['sqsren'] * roll['sqinv']

    roll['rol_avail'] = roll['inv'] * roll['rol_vac']
    if sector_val == "apt":
        roll['rol_avail'] = round(roll['rol_avail'], 0)
    else:
        roll['rol_avail'] = round(roll['rol_avail'], -3)

    roll['rolaskrevenue'] = roll['rol_inv'] * roll['rol_mrent']
    roll['roleffrevenue'] = roll['rol_inv'] * roll['rol_merent']

    roll = roll[(roll['yr'] >= curryr - 4)]

    cols_to_roll = ['inv', 'rol_inv', 'inv_oob', 'cons', 'rol_cons', 'cons_oob', 'avail', 'avail_oob', 'rol_avail', 'occ', 'abs', 'rol_abs', 'askrevenue', 'effrevenue', 'rolaskrevenue', 'roleffrevenue', 'oobaskrevenue']
    
    if drop_val[:2] == "US":
        cols_to_roll += ['sqinv', 'sqcons', 'sqavail', 'sqocc', 'sqabs', 'sqaskrev']

    roll[cols_to_roll] = roll.groupby([identity_filt, 'yr', 'currmon'])[cols_to_roll].transform('sum', min_count=1)

    if filt_type == "reg":
        roll = roll.drop_duplicates([identity_filt, 'yr', 'currmon'])

    if drop_val[:2] == "US":
        roll['sqabs'] = np.where((roll['sqabs'] == 0) & (roll['yr'] <= curryr - 2), np.nan, roll['sqabs'])

    roll['vac'] = round(roll['avail'] / roll['inv'], 4)
    roll['vac_oob'] = round(roll['avail_oob'] / roll['inv_oob'], 4)
    roll['vac_chg'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)), roll['vac'] - roll['vac'].shift(1), np.nan)
    roll['rol_vac'] = round(roll['rol_avail'] / roll['rol_inv'], 4)
    roll['rol_vac_chg'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)), roll['rol_vac'] - roll['rol_vac'].shift(1), np.nan)
    roll['vac_chg_oob'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)), roll['vac_oob'] - roll['vac_oob'].shift(1), np.nan)

    if drop_val[:2] == "US":
        roll['sqvac'] = round(roll['sqavail'] / roll['sqinv'], 4)
        roll['sqvac_chg'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)) & (roll['sqavail'] != 0) & (roll['sqavail'].shift(1) != 0), roll['sqvac'] - roll['sqvac'].shift(1), np.nan)
        roll['sqvac_chg'] = np.where((roll[identity_filt] == roll[identity_filt].shift(3)) & (roll['sqvac'] != 0) & (roll['sqvac'].shift(3) != 0) & (roll['sqvac'].shift(1).isnull() == True), roll['sqvac'] - roll['sqvac'].shift(3), roll['sqvac_chg'])
        
    roll['mrent'] = round(roll['askrevenue'] / roll['inv'],2)
    roll['merent'] = round(roll['effrevenue'] / roll['inv'],2)
    roll['rol_mrent'] = round(roll['rolaskrevenue'] / roll['rol_inv'],2)
    roll['mrent_oob'] = round(roll['oobaskrevenue'] / roll['inv'],2)
    roll['rol_merent'] = round(roll['roleffrevenue'] / roll['rol_inv'],2)

    if drop_val[:2] == "US":
        roll['sqsren'] = round(roll['sqaskrev'] / roll['sqinv'],2)
        roll['sq_Gmrent'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)) & (roll['sqsren'] != 0) & (roll['sqsren'].shift(1) != 0), (roll['sqsren'] - roll['sqsren'].shift(1)) / roll['sqsren'].shift(1), np.nan)
        roll['sq_Gmrent'] = np.where((roll[identity_filt] == roll[identity_filt].shift(3)) & (roll['sqsren'] != 0) & (roll['sqsren'].shift(3) != 0) & (roll['sqsren'].shift(1).isnull() == True), (roll['sqsren'] - roll['sqsren'].shift(3)) / roll['sqsren'].shift(3), roll['sq_Gmrent'])
    
    roll['G_mrent'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)), (roll['mrent'] - roll['mrent'].shift(1)) / roll['mrent'].shift(1), np.nan)
    roll['G_merent'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)), (roll['merent'] - roll['merent'].shift(1)) / roll['merent'].shift(1), np.nan)
    roll['rol_G_mrent'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)), (roll['rol_mrent'] - roll['rol_mrent'].shift(1)) / roll['rol_mrent'].shift(1), np.nan)
    roll['G_mrent_oob'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)), (roll['mrent_oob'] - roll['mrent_oob'].shift(1)) / roll['mrent_oob'].shift(1), np.nan)
    roll['rol_G_merent'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)), (roll['rol_merent'] - roll['rol_merent'].shift(1)) / roll['rol_merent'].shift(1), np.nan)

    roll['gap'] = ((roll['merent'] - roll['mrent']) / roll['mrent']) * -1
    roll['rol_gap'] = ((roll['rol_merent'] - roll['rol_mrent']) / roll['rol_mrent']) * -1
    roll['gap_chg'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)), roll['gap'] - roll['gap'].shift(1), np.nan)
    roll['rol_gap_chg'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)), roll['rol_gap'] - roll['rol_gap'].shift(1), np.nan)

    roll['rol_cons'] = np.where((roll['curr_tag'] == 1), np.nan, roll['rol_cons'])
    roll['rol_abs'] = np.where((roll['curr_tag'] == 1), np.nan, roll['rol_abs'])
    roll['rol_vac'] = np.where((roll['curr_tag'] == 1), np.nan, roll['rol_vac'])
    roll['rol_vac_chg'] = np.where((roll['curr_tag'] == 1), np.nan, roll['rol_vac_chg'])
    roll['rol_abs'] = np.where((roll['curr_tag'] == 1), np.nan, roll['rol_abs'])
    roll['rol_G_mrent'] = np.where((roll['curr_tag'] == 1), np.nan, roll['rol_G_mrent'])
    roll['rol_G_merent'] = np.where((roll['curr_tag'] == 1), np.nan, roll['rol_G_merent'])
         
    if filt_type == "reg" and drop_val[:2] != "US":
        cols_to_display = ['identity_us', 'subsector', 'metcode', 'yr', 'currmon', 'inv', 'metsqinv', 'cons', 'rol_cons', 'metsqcons', 'vac', 'vac_chg', 'rol_vac', 'rol_vac_chg', 'metsqvacchg', 'abs', 'rol_abs', 'metsqabs', 'mrent', 'G_mrent', 'rol_G_mrent', 'metsqsren', 'metsq_Gmrent', 'merent', 'G_merent', 'rol_G_merent', 'gap', 'gap_chg', 'rol_mrent', 'rol_inv', 'rol_gap', 'rol_gap_chg']
        if 'met_sur_totabs' in list(roll.columns):
            cols_to_display += ['met_sur_totabs', 'met_g_renx_mo_wgt']
    elif filt_type == "reg" and drop_val[:2] == "US":
        cols_to_display = ['identity_us', 'subsector', 'metcode', 'yr', 'currmon', 'inv', 'sqinv', 'cons', 'rol_cons', 'sqcons', 'vac', 'vac_chg', 'rol_vac', 'rol_vac_chg', 'sqvac_chg', 'abs', 'rol_abs', 'sqabs', 'mrent', 'G_mrent', 'rol_G_mrent', 'sqsren', 'sq_Gmrent', 'merent', 'G_merent', 'rol_G_merent', 'gap', 'gap_chg', 'rol_mrent', 'rol_inv', 'rol_gap', 'rol_gap_chg']
    else:
        cols_to_display = ['subsector', 'metcode', 'subid', 'yr', 'currmon', 'inv', 'sqinv', 'cons', 'sqcons', 'vac', 'vac_chg', 'sqvac', 'sqvac_chg', 'abs', 'sqabs', 'mrent', 'G_mrent', 'sqsren', 'sq_Gmrent', 'merent', 'G_merent', 'gap', 'gap_chg']
    cols_to_display += ['cons_oob', 'vac_oob', 'vac_chg_oob', 'mrent_oob', 'G_mrent_oob']
    if drop_val[:2] == "US":
        cols_to_display.remove('metcode')
    
    roll = roll[cols_to_display]
    
    if drop_val != "temp":
        roll = roll[(roll['yr'] >= curryr - 3)]

    return roll

def live_flag_count(dataframe, sector_val, flag_cols, curryr, currmon): 
    
    rol_flag_cols = [x for x in flag_cols if "rol" in x or x == "v_flag_low" or x == "v_flag_high" or x == "e_flag_high" or x == "e_flag_low" or x == "c_flag_sqdiff"]
    test_flag_cols = [x + "_test" for x in rol_flag_cols]
    dataframe[test_flag_cols] = dataframe.groupby('identity')[rol_flag_cols].transform('sum')
    for x, y in zip(rol_flag_cols, test_flag_cols):
        dataframe[x] = np.where((dataframe[y] > 0) & (dataframe['curr_tag'] == 1), 1, 0)
        dataframe[x] = np.where(dataframe['curr_tag'] == 0, 0, dataframe[x])
    dataframe.drop(test_flag_cols, axis=1, inplace=True)
    
    dataframe['c_flag_tot_sub'] = dataframe.filter(regex="^c_flag*").sum(axis=1)
    dataframe['v_flag_tot_sub'] = dataframe.filter(regex="^v_flag*").sum(axis=1)
    dataframe['g_flag_tot_temp1'] = dataframe.filter(regex="^g_flag*").sum(axis=1)
    dataframe['g_flag_tot_temp2'] = dataframe.filter(regex="^e_flag*").sum(axis=1)
    dataframe['g_flag_tot_sub'] = dataframe['g_flag_tot_temp1'] + dataframe['g_flag_tot_temp2']
    dataframe = dataframe.drop(['g_flag_tot_temp1', 'g_flag_tot_temp2'], axis=1)

    dataframe["c_skip"] = dataframe["flag_skip"].str.count("c_flag")
    dataframe["v_skip"] = dataframe["flag_skip"].str.count("v_flag")
    dataframe["g_skip"] = dataframe["flag_skip"].str.count("g_flag") + dataframe["flag_skip"].str.count("e_flag")

    dataframe['extra_c_skip'] = dataframe['c_skip'] - dataframe['c_flag_tot_sub']
    dataframe['extra_v_skip'] = dataframe['v_skip'] - dataframe['v_flag_tot_sub']
    dataframe['extra_g_skip'] =  dataframe['g_skip'] - dataframe['g_flag_tot_sub']
    dataframe[['extra_c_skip', 'extra_v_skip', 'extra_g_skip']] = np.where(dataframe[['extra_c_skip', 'extra_v_skip', 'extra_g_skip']] < 1, 0, dataframe[['extra_c_skip', 'extra_v_skip', 'extra_g_skip']])

    c_left = dataframe['c_flag_tot_sub'].sum() -  dataframe['flag_skip'].str.count('c_flag').sum() + dataframe['extra_c_skip'].sum()
    v_left = dataframe['v_flag_tot_sub'].sum() - dataframe['flag_skip'].str.count('v_flag').sum() + dataframe['extra_v_skip'].sum()
    g_left = dataframe['g_flag_tot_sub'].sum() - dataframe['flag_skip'].str.count('g_flag').sum() - dataframe['flag_skip'].str.count('e_flag').sum() + dataframe['extra_g_skip'].sum()


    countdown_dict = {'Totals': [c_left, v_left, g_left]}
    countdown = pd.DataFrame.from_dict(countdown_dict, orient='index', columns=["Cons Flags", "Vac Flags", "Rent Flags"])
    
    return countdown

def summarize_flags_ranking(dataframe_in, sector_val, type_filt, flag_cols):
    
    dataframe = dataframe_in.copy()
    dataframe = dataframe.reset_index()

    if type_filt == "met": 
        filt_val = 'identity_met'
    elif type_filt == 'sub':
        if sector_val == "ret":
            dataframe['identity'] = dataframe['metcode'] + dataframe['subid'].astype(str)
        filt_val = 'identity'

    dataframe[flag_cols] = np.where((dataframe[flag_cols] != 0), 1, dataframe[flag_cols])

    dataframe['c_flag_tot'] = dataframe.filter(regex="^c_flag*").sum(axis=1)
    dataframe['v_flag_tot'] = dataframe.filter(regex="^v_flag*").sum(axis=1)
    dataframe['g_flag_tot_temp1'] = dataframe.filter(regex="^g_flag*").sum(axis=1)
    dataframe['g_flag_tot_temp2'] = dataframe.filter(regex="^e_flag*").sum(axis=1)
    dataframe['g_flag_tot'] = dataframe['g_flag_tot_temp1'] + dataframe['g_flag_tot_temp2']

    dataframe['has_flag'] = np.where((dataframe['c_flag_tot'] != 0) | (dataframe['v_flag_tot'] != 0) | (dataframe['g_flag_tot'] != 0), 1, 0)   
    dataframe['sum_has_flag'] = dataframe.groupby(filt_val)['has_flag'].transform('sum')
    dataframe['total_trend_rows'] = dataframe.groupby(filt_val)['identity_row'].transform('nunique')
    dataframe['% Trend Rows W Flag'] = dataframe['sum_has_flag'] / dataframe['total_trend_rows']
    if type_filt == "met":
        if sector_val == "ret":
            dataframe = dataframe[['metcode', '% Trend Rows W Flag']]
            dataframe = dataframe.drop_duplicates('metcode')
        else:
            dataframe = dataframe[['subsector', 'metcode', '% Trend Rows W Flag']]
            dataframe = dataframe.drop_duplicates(['subsector', 'metcode'])
    elif type_filt == "sub":
        if sector_val == "ret":
            dataframe = dataframe[['metcode', 'subid', '% Trend Rows W Flag']]
            dataframe = dataframe.drop_duplicates(['metcode', 'subid'])
        else:
            dataframe = dataframe[['subsector', 'metcode', 'subid', '% Trend Rows W Flag']]
            dataframe = dataframe.drop_duplicates(['subsector', 'metcode', 'subid'])
    dataframe = dataframe.sort_values(by=['% Trend Rows W Flag'], ascending=False)
    dataframe = dataframe.iloc[:10]
    
    return dataframe
    
def summarize_flags(dataframe_in, sum_val, flag_cols):

    dataframe = dataframe_in.copy()
    dataframe = dataframe.reset_index()
    if sum_val[0:2] == "US":
        identity_filt = 'identity_us'
    else:
        identity_filt = 'identity_met'
    
    dataframe = dataframe[dataframe[identity_filt] == sum_val]

    dataframe[flag_cols] = np.where((dataframe[flag_cols] != 0), 1, dataframe[flag_cols])
    dataframe['c_flag_tot'] = dataframe.filter(regex="^c_flag*").sum(axis=1)
    dataframe['v_flag_tot'] = dataframe.filter(regex="^v_flag*").sum(axis=1)
    dataframe['g_flag_tot_temp1'] = dataframe.filter(regex="^g_flag*").sum(axis=1)
    dataframe['g_flag_tot_temp2'] = dataframe.filter(regex="^e_flag*").sum(axis=1)
    dataframe['g_flag_tot'] = dataframe['g_flag_tot_temp1'] + dataframe['g_flag_tot_temp2']

    dataframe['total_subs'] = dataframe.groupby(identity_filt)['identity'].transform('nunique')

    for x  in ["c", "v", "g"]:
        dataframe[x + '_flag_sum'] = dataframe.groupby(identity_filt)[x + '_flag_tot'].transform('sum')
        
        dataframe['has_flag'] = np.where((dataframe[x + '_flag_tot'] != 0), 1, 0)    
        dataframe['sum_has_curr_flag'] = dataframe[dataframe['curr_tag'] == 1].groupby(identity_filt)['has_flag'].transform('sum')
        dataframe[x + '_% Currmon Trend Rows W Flag'] = dataframe['sum_has_curr_flag'] / dataframe['total_subs']

        dataframe[x + '_sub_has_flag'] = dataframe[dataframe['has_flag'] == 1].groupby(identity_filt)['identity'].transform('nunique')
        dataframe[x + '_% Subs W Flag'] = dataframe[x + '_sub_has_flag'] / dataframe['total_subs']
        dataframe[x + '_% Subs W Flag'] = dataframe[x + '_% Subs W Flag'].fillna(method='ffill').fillna(method='bfill')
        dataframe[x + '_% Subs W Flag'] = dataframe[x + '_% Subs W Flag'].fillna(0)


    dataframe.sort_values(by=['metcode', 'subid', 'subsector', 'yr', 'currmon'], ascending=[True, True, True, False, False], inplace=True)
    dataframe = dataframe.drop_duplicates(identity_filt)
    dataframe = dataframe.reset_index()

    input_dict = {
                    'Flag Type': ['Cons Flags', 'Vac Flags', 'Rent Flags'], 
                    'Total Flags': [dataframe['c_flag_sum'].loc[0], dataframe['v_flag_sum'].loc[0], dataframe['g_flag_sum'].loc[0]],
                    '% Currmon Trend Rows W Flag': [dataframe['c_% Currmon Trend Rows W Flag'].loc[0], dataframe['v_% Currmon Trend Rows W Flag'].loc[0], dataframe['g_% Currmon Trend Rows W Flag'].loc[0]],
                    '% Subs W Flag': [dataframe['c_% Subs W Flag'].loc[0], dataframe['v_% Subs W Flag'].loc[0], dataframe['g_% Subs W Flag'].loc[0]]
                  }
    
    dataframe_out = pd.DataFrame(input_dict)

    return dataframe_out


# Return a more verbose description of the flag to the user
def get_issue(type_return, sector_val, dataframe=False, has_flag=False, flag_list=False, p_skip_list=False, show_skips=False, flags_resolved=False, flags_unresolved=False, flags_new=False, flags_skipped=False, curryr=False, currmon=False, preview_status=False, init_skips=False, test_auto_rebench=False):

    # This dict holds a more verbose explanation of the flags, so that it can be printed to the user for clarity
    issue_descriptions = {
        "c_flag_rolv": "Vacancy level is different than ROL and there is a construction rebench.",   
        "c_flag_rolg": "Market Rent Change is different than ROL and there is a construction rebench",
        "c_flag_sqdiff": "Square cons is different than published cons.",
        "v_flag_low": "The vacancy level is very low and is not supported by the square pool.",
        "v_flag_high": "The vacancy level is above 100 percent.",
        "v_flag_rol": "The vacancy level is different than ROL and there is no construction rebench to cause it.",
        "v_flag_sqlev": "The vacancy level is different than the sq vac level.",
        "v_flag_sqabs": "The published absorption is different than the absorption in the square pool.",
        "v_flag_surabs": "The published absorption is different than the surveyed absorption.",
        "v_flag_rsent": "The direction of non-surveyed absorption is not in line with the surveyed rent change.",
        "v_flag_level": "There is a gap between published vac and sq vac, can use the surveyed abs range to close the gap.",
        "g_flag_rol": "Market rent change is different than ROL and there is no construction rebench to cause it.",
        "g_flag_sqlev": "Market rent level is different than square rent level.",
        "g_flag_consp": "There is construction and market rent change is very large.",
        "g_flag_consn": "There is construction and market rent change is negative.",
        "g_flag_large": "The magnitude of market rent change is very large with limited survey support.",
        "g_flag_sqdir": "Published market rent change is in the opposite direction from sq rent change.",
        "g_flag_sqdiff": "Published market rent change is different than sq rent change.",
        "g_flag_surdiff": "Published market rent change is different than surveyed rent change.",
        "e_flag_low": "Gap level is very low",
        "e_flag_high": "Gap level is very high.",
        "e_flag_perc": "Gap level is an outlier, and gap change is being modeled to push it further into the tails.",
        "e_flag_mdir": "Effective rent change is in the opposite direction of market rent change."
    }
    
    if sector_val != "apt":
        del issue_descriptions['c_flag_sqdiff']
        del issue_descriptions['v_flag_sqlev']
        del issue_descriptions['g_flag_sqlev']

    highlighting = {
        "c_flag_rolv": [['vac', 'abs', 'rol abs'], ['vac roldiff', 'cons roldiff']], 
        "c_flag_rolg": [['Gmrent', 'rol Gmrent'], ['gmrent roldiff']],
        "c_flag_sqdiff": [['cons', 'sq cons'], []],
        "v_flag_low": [['vac', 'sq vac'], []],
        "v_flag_high": [['vac', 'sq vac'], []],
        "v_flag_rol": [['vac'], ['vac roldiff']],
        "v_flag_sqlev": [['vac', 'vac chg', 'sq vac', 'sq vac chg'], ['vac chg 12', 'sqvac chg 12']],
        "v_flag_sqabs": [['abs', 'sq abs'], []],
        "v_flag_surabs": [['abs'], ['sub sur totabs', 'nc surabs']],
        "v_flag_rsent": [['abs'], ['sub sur totabs', 'sub grenx mo wgt']],
        "g_flag_rol": [['Gmrent'], ['gmrent roldiff', 'cons roldiff']],
        "g_flag_sqlev": [['mrent', 'Gmrent', 'sq rent', 'sq Gmrent'], ['G mrent 12', 'sq Gmrent 12']],
        "g_flag_consp": [['Gmrent', 'cons', 'sq rent'], ['ncrenlev']],
        "g_flag_consn": [['Gmrent', 'cons', 'sq rent'], ['ncrenlev']],
        "g_flag_large": [['Gmrent'], ['sub grenx mo wgt', 'sub sur r cov', 'met grenx mo wgt', 'met sur r cov']],
        "g_flag_sqdir": [['Gmrent', 'sq Gmrent'], []],
        "g_flag_sqdiff": [['Gmrent', 'sq Gmrent'], []],
        "g_flag_surdiff": [['Gmrent'], ['sub grenx mo wgt', 'sub sur r cov', 'met grenx mo wgt', 'met sur r cov']],
        "e_flag_low": [['gap'], []],
        "e_flag_high": [['gap'], []],
        "e_flag_perc": [['gap', 'gap chg'], ['gap perc 5', 'gap perc 95']],
        "e_flag_mdir": [['Gmrent', 'Gmerent'], []],
    }

    if sector_val != "ind":
        highlighting['v_flag_level'] = ['vac'], ['sub sur totabs', 'avail10d'], ['sq vac']
    else:
        highlighting['v_flag_level'] = ['avail'], ['sub sur totabs', 'avail10d'], ['sq avail']
    
    display_issue_cols = []
    key_metric_issue_cols = []
    issue_description_resolved = []
    issue_description_unresolved = []
    issue_description_new = []
    issue_description_skipped = []
    if type_return == "specific":
        if test_auto_rebench == True:
            issue_description_noprev = "There was an auto rebench for construction in curryr - 3 that caused a significant difference to ROL. Enter a supporting comment to document what caused the rebench before moving on to flag review."
        elif has_flag == 0:
            issue_description_noprev = "You have cleared all the flags"
        elif has_flag == 2 and (show_skips == False or len(p_skip_list) == 0):
            issue_description_noprev = "No flags for this submarket"
        elif has_flag == 2 and show_skips == True and len(p_skip_list) > 0:
            p_skip_list = p_skip_list[0].replace(' ', '').split(",")

            issue_description_noprev = html.Div([
                                        html.Div([
                                            dbc.Container(
                                            [
                                                dbc.Checklist(
                                                    id="flag_descriptions_noprev",
                                                    options=[
                                                            {"label": f" {i[0]} {i[6:]}", "value": f"skip-{i}", "label_id": f"label-{i}", "disabled": True}
                                                            for i in p_skip_list
                                                            ],
                                                    inline=True,
                                                    value = ["skip-" + x for x in p_skip_list]
                                                            ),  
                                                    
                                            ]
                                            + [
                                                dbc.Tooltip(issue_descriptions[i], target=f"label-{i}")
                                                for i in p_skip_list
                                            ],
                                            fluid=True),
                                                
                                        ]), 
                                    ])
        elif has_flag == 1:
            if preview_status == 0:
                if show_skips == True:
                    flags_use = flag_list + p_skip_list
                    disabled_list = [False] * len(flag_list) + [True] * len(p_skip_list)
                else:
                    flags_use = flag_list
                    disabled_list = [False] * len(flag_list)

                issue_description_noprev = html.Div([
                                        html.Div([
                                            dbc.Container(
                                            [
                                                dbc.Checklist(
                                                    id="flag_descriptions_noprev",
                                                    options=[
                                                            {"label": f" {i[0]} {i[6:]}", "value": f"skip-{i}", "label_id": f"label-{i}", "disabled": j}
                                                            for i, j in zip(flags_use, disabled_list)
                                                            ],
                                                    inline=True,
                                                    value = ["skip-" + x for x in init_skips]
                                                            ),  
                                                    
                                            ]
                                            + [
                                                dbc.Tooltip(issue_descriptions[i], target=f"label-{i}")
                                                for i in flags_use
                                            ],
                                            fluid=True),
                                                
                                        ]), 
                                    ])
            else:                
                issue_description_noprev = []
                if len(flags_resolved) > 0:
                    issue_description_resolved = html.Div([
                                            html.Div([
                                                dbc.Container(
                                                [
                                                    dbc.Checklist(
                                                        id="flag_descriptions_resolved",
                                                        options=[
                                                                {"label": f" {i[0]} {i[6:]}", "value": f"skip-{i}", "label_id": f"label-{i}", 'disabled': True}
                                                                for i in flags_resolved
                                                                ],
                                                        inline=True,
                                                        labelStyle={'display': 'inline-block', 'margin': '0 10px 0 10px', 'color': 'green'},
                                                                ),  
                                                        
                                                ]
                                                + [
                                                    dbc.Tooltip(issue_descriptions[i], target=f"label-{i}")
                                                    for i in flags_resolved
                                                ],
                                                fluid=True),
                                                    
                                            ]), 
                                        ])
                else:
                    issue_description_resolved = []
                
                if len(flags_unresolved) > 0:
                    issue_description_unresolved = html.Div([
                                            html.Div([
                                                dbc.Container(
                                                [
                                                    dbc.Checklist(
                                                        id="flag_descriptions_unresolved",
                                                        options=[
                                                                {"label": f" {i[0]} {i[6:]}", "value": f"skip-{i}", "label_id": f"label-{i}"}
                                                                for i in flags_unresolved
                                                                ],
                                                        inline=True,
                                                        labelStyle={'display': 'inline-block', 'margin': '0 10px 0 10px', 'color': 'red'},
                                                                ),  
                                                        
                                                ]
                                                + [
                                                    dbc.Tooltip(issue_descriptions[i], target=f"label-{i}")
                                                    for i in flags_unresolved
                                                ],
                                                fluid=True),
                                                    
                                            ]), 
                                        ])
                else:
                    issue_description_unresolved = []

                if len(flags_new) > 0:
                    issue_description_new = html.Div([
                                            html.Div([
                                                dbc.Container(
                                                [
                                                    dbc.Checklist(
                                                        id="flag_descriptions_new",
                                                        options=[
                                                                {"label": f" {i[0]} {i[6:]}", "value": f"skip-{i}", "label_id": f"label-{i}"}
                                                                for i in flags_new
                                                                ],
                                                        inline=True,
                                                        labelStyle={'display': 'inline-block', 'margin': '0 10px 0 10px', 'color': 'GoldenRod'},
                                                                ),  
                                                        
                                                ]
                                                + [
                                                    dbc.Tooltip(issue_descriptions[i], target=f"label-{i}")
                                                    for i in flags_new
                                                ],
                                                fluid=True),
                                                    
                                            ]), 
                                        ])
                else:
                    issue_description_new = []

                if len(flags_skipped) > 0 or len(p_skip_list) > 0:
                    issue_description_skipped = html.Div([
                                            html.Div([
                                                dbc.Container(
                                                [
                                                    dbc.Checklist(
                                                        id="flag_descriptions_skipped",
                                                        options=[
                                                                {"label": f" {i[0]} {i[6:]}", "value": f"skip-{i}", "label_id": f"label-{i}", "disabled": j}
                                                                for i, j in zip(flags_skipped + p_skip_list, [False] * len(flags_skipped) + [True] * len(p_skip_list))
                                                                ],
                                                        value=[f"skip-{i}" for i in flags_skipped + p_skip_list],
                                                        inline=True,
                                                        labelStyle={'display': 'inline-block', 'margin': '0 10px 0 10px', 'color': 'black'},
                                                                ),  
                                                        
                                                ]
                                                + [
                                                    dbc.Tooltip(issue_descriptions[i], target=f"label-{i}")
                                                    for i in flags_skipped + p_skip_list
                                                ],
                                                fluid=True),
                                                    
                                            ]), 
                                        ])
                else:
                    issue_description_skipped = []


            if preview_status == 0:
                display_issue_cols = highlighting[flag_list[0]][0]
                key_metric_issue_cols = highlighting[flag_list[0]][1]
            else:
                if len(flags_new) > 0:
                    display_issue_cols = highlighting[flags_new[0]][0]
                    key_metric_issue_cols = highlighting[flags_new[0]][1]
                elif len(flags_unresolved) > 0:
                    display_issue_cols = highlighting[flags_unresolved[0]][0]
                    key_metric_issue_cols = highlighting[flags_unresolved[0]][1]
                else:
                    display_issue_cols = highlighting[flag_list[0]][0]
                    key_metric_issue_cols = highlighting[flag_list[0]][1]

        return issue_description_noprev, issue_description_resolved, issue_description_unresolved, issue_description_new, issue_description_skipped, display_issue_cols, key_metric_issue_cols
    
    elif type_return == "list":
        return issue_descriptions

# Function that checks to see if there was an adjustment made from last published data (ROL) that crosses the data governance threshold, thus requiring a support note documenting why the change was made
def rebench_check(data_temp, curryr, currmon, sector_val, avail_check, mrent_check, merent_check):
    
    identity = False

    dataframe_in = data_temp.copy()
    dataframe_in['avail_comment'] = np.where(dataframe_in['avail_comment'] == '', np.nan, dataframe_in['avail_comment'])
    dataframe_in['mrent_comment'] = np.where(dataframe_in['mrent_comment'] == '', np.nan, dataframe_in['mrent_comment'])
    dataframe_in['erent_comment'] = np.where(dataframe_in['erent_comment'] == '', np.nan, dataframe_in['erent_comment'])
    dataframe_in['avail_comment'] = dataframe_in.groupby('identity')['avail_comment'].bfill()
    dataframe_in['mrent_comment'] = dataframe_in.groupby('identity')['mrent_comment'].bfill()
    dataframe_in['erent_comment'] = dataframe_in.groupby('identity')['erent_comment'].bfill()
    dataframe_in['avail_comment'] = np.where(dataframe_in['avail_comment'].isnull() == True, '', dataframe_in['avail_comment'])
    dataframe_in['mrent_comment'] = np.where(dataframe_in['mrent_comment'].isnull() == True, '', dataframe_in['mrent_comment'])
    dataframe_in['erent_comment'] = np.where(dataframe_in['erent_comment'].isnull() == True, '', dataframe_in['erent_comment'])
    dataframe_in = dataframe_in[(dataframe_in['yr'] != curryr) | ((dataframe_in['yr'] == curryr) & (dataframe_in['currmon'] != currmon))]
    
    dataframe = dataframe_in.copy()
    dataframe = dataframe[['rol_vac', 'vac', 'vac_oob', 'yr', 'currmon', 'avail_comment', 'identity']]
    
    dataframe['vac_diff'] = dataframe['vac'] - dataframe['rol_vac']
    dataframe = dataframe[(abs(dataframe['vac_diff']) >= 0.03) & (round(dataframe['vac'],4) == round(dataframe['vac_oob'],4))]
    if len(dataframe) > 0:
        dataframe = dataframe.drop_duplicates('identity')
        for index, row in dataframe.iterrows():
            avail_c = row['avail_comment']
            if avail_c[-9:] != "Note Here" and len(avail_c.strip()) > 0:
                avail_check = False
            else:
                avail_check = True
                identity = row['identity']
                break
    else:
        avail_check = False

    if avail_check == False:
        dataframe = dataframe_in.copy()
        dataframe = dataframe[(dataframe['yr'] != curryr) | ((dataframe['yr'] == curryr) & (dataframe['currmon'] != currmon))]
        dataframe = dataframe[['rol_mrent', 'mrent', 'mrent_oob', 'yr', 'currmon', 'mrent_comment', 'identity']]

        dataframe['mrent_diff'] = (dataframe['mrent'] - dataframe['rol_mrent']) / dataframe['rol_mrent']
        dataframe = dataframe[(abs(dataframe['mrent_diff']) >= 0.05) & (round(dataframe['mrent'],2) == round(dataframe['mrent_oob'],2))]
        if len(dataframe) > 0:
            dataframe = dataframe.drop_duplicates('identity')
            for index, row in dataframe.iterrows():
                mrent_c = row['mrent_comment']
                if mrent_c[-9:] != "Note Here" and len(mrent_c.strip()) > 0:
                    mrent_check = False
                else:
                    mrent_check = True
                    identity = row['identity']
                    break
        else:
            mrent_check = False

    if avail_check == False and mrent_check == False:
        dataframe = dataframe_in.copy()
        dataframe = dataframe[(dataframe['yr'] != curryr) | ((dataframe['yr'] == curryr) & (dataframe['currmon'] != currmon))]
        dataframe = dataframe[['rol_merent', 'merent', 'merent_oob', 'yr', 'currmon', 'erent_comment', 'identity']]
        
        dataframe['merent_diff'] = (dataframe['merent'] - dataframe['rol_merent']) / dataframe['rol_merent']
        dataframe = dataframe[(abs(dataframe['merent_diff']) >= 0.05) & (round(dataframe['merent'],2) == round(dataframe['merent_oob'],2))]
        if len(dataframe) > 0:
            dataframe = dataframe.drop_duplicates('identity')
            for index, row in dataframe.iterrows():
                erent_c = row['erent_comment']
                if erent_c[-9:] != "Note Here" and len(erent_c.strip()) > 0:
                    merent_check = False
                else:
                    merent_check = True
                    identity = row['identity']
                    break
        else:
            merent_check = False
    
    return avail_check, mrent_check, merent_check, identity


# Function that analyzes where edits are made in the display dataframe if manual edit option is selected
def get_diffs(shim_data, data_orig, data, drop_val, curryr, currmon, sector_val, button, subsequent_chg, avail_c, mrent_c, erent_c):
    data_update = shim_data.copy()
    indexes = data_orig.index.values
    data_update['new_index'] = indexes
    data_update = data_update.set_index('new_index')

    diffs = data_update.copy()
    diffs = diffs == data_orig
    diffs = data_update[~diffs]
    diffs = diffs.dropna(axis=0, how='all')
    diffs = diffs.dropna(axis=1, how='all')

    # Because a user might want to retain the current avail in the case of an inv or cons shim ro conversion/demolition shim, or similarly, merent in the case of an mrent shim, we need an additional check to add them back in to the diffs dataframe since they will be nulled out with the boolean indexing method
    if "avail" not in list(diffs.columns) and ("inv" in list(diffs.columns) or "cons" in list(diffs.columns) or "conv" in list(diffs.columns) or "demo" in list(diffs.columns)):
        diffs['avail'] = np.nan
    if "inv" in list(diffs.columns) or "cons" in list(diffs.columns) or "conv" in list(diffs.columns) or "demo" in list(diffs.columns):
        check_avail = data_update.copy()
        if sector_val != "ind":
            check_avail = check_avail[(check_avail['avail'].isnull() == False) & ((check_avail['inv'].isnull() == False) | (check_avail['cons'].isnull() == False) | (check_avail['conv'].isnull() == False) | (check_avail['demo'].isnull() == False))]
        else:
            check_avail = check_avail[(check_avail['avail'].isnull() == False) & ((check_avail['inv'].isnull() == False) | (check_avail['cons'].isnull() == False))]
        check_avail = check_avail[['avail']]
        check_avail = check_avail.rename(columns={'avail': 'avail_check'})
        diffs = diffs.join(check_avail)
        diffs['avail'] = np.where(diffs['avail_check'].isnull() == False, diffs['avail_check'], diffs['avail'])
        diffs = diffs.drop(['avail_check'], axis=1)

    if "merent" not in list(diffs.columns) and "mrent" in list(diffs.columns):
        diffs['merent'] = np.nan
    if "mrent" in list(diffs.columns):
        check_merent = data_update.copy()
        check_merent = check_merent[(check_merent['merent'].isnull() == False) & (check_merent['mrent'].isnull() == False)]
        check_merent = check_merent[['merent']]
        check_merent = check_merent.rename(columns={'merent': 'merent_check'})
        diffs = diffs.join(check_merent)
        diffs['merent'] = np.where(diffs['merent_check'].isnull() == False, diffs['merent_check'], diffs['merent'])
        diffs = diffs.drop(['merent_check'], axis=1)
 
    if len(diffs) > 0:
        for index, row in diffs.iterrows():
            for col_name in list(diffs.columns):
                row_to_fix_diffs = index
                if math.isnan(row[col_name]) == False:
                    fix_val = row[col_name]
                    if col_name == "inv":
                        col_issue_diffs = "i_flag"
                    elif col_name == "cons":
                        col_issue_diffs = "c_flag"
                    elif col_name == "conv":
                        col_issue_diffs = "a_flag"
                    elif col_name == "demo":
                        col_issue_diffs = "d_flag"
                    elif col_name == "avail":
                        col_issue_diffs = "v_flag"
                    elif col_name == "mrent":
                        col_issue_diffs = "g_flag"
                    elif col_name == "merent":
                        col_issue_diffs = "e_flag"
                    
                    data_temp = insert_fix(data, row_to_fix_diffs, drop_val, fix_val, col_issue_diffs[0], curryr, currmon, sector_val, subsequent_chg)
           
        # Check to see if a vacancy or rent shim created a change in a historical period above the data governance threshold set by key stakeholders. If it did, do not process the shim unless there is an accompanying note explaining why the rebench was made

        avail_check = False
        mrent_check = False
        merent_check = False

        init_avail_c = data_temp.loc[drop_val + str(curryr) + str(currmon)]['avail_comment']
        init_mrent_c = data_temp.loc[drop_val + str(curryr) + str(currmon)]['mrent_comment']
        init_erent_c = data_temp.loc[drop_val + str(curryr) + str(currmon)]['erent_comment']

        if avail_check == True:
            if avail_c[-9:] != "Note Here" and len(avail_c.strip()) > 0 and avail_c != init_avail_c:
                avail_check = False

        if mrent_check == True and avail_check == False:
            if mrent_c[-9:] != "Note Here" and len(mrent_c.strip()) > 0 and mrent_c != init_mrent_c:
                mrent_check = False
    
        if merent_check == True and avail_check == False and mrent_check == False:
            if erent_c[-9:] != "Note Here" and len(erent_c.strip()) > 0 and erent_c != init_erent_c:
                merent_check = False
        
        if button == 'submit':
            for var in ['avail', 'mrent', 'merent']:
                rebench_to_check = shim_data.copy()
                if rebench_to_check[var].isnull().values.all() == False:
                    if rebench_to_check[rebench_to_check[var].isnull() == False].reset_index().loc[0]['yr'] != curryr or (rebench_to_check[rebench_to_check[var].isnull() == False].reset_index().loc[0]['yr'] == curryr and rebench_to_check[rebench_to_check[var].isnull() == False].reset_index().loc[0]['currmon'] != currmon):
                        if var == "avail" and avail_c[-9:] != "Note Here" and len(avail_c.strip()) > 0 and avail_c != init_avail_c:
                            avail_check = False
                        elif var == "mrent" and mrent_c[-9:] != "Note Here" and len(mrent_c.strip()) > 0 and mrent_c != init_mrent_c:
                            mrent_check = False
                        elif var == "merent" and erent_c[-9:] != "Note Here" and len(erent_c.strip()) > 0 and erent_c != init_erent_c:
                            merent_check = False
                        else:
                            if var == 'avail':
                                var = 'vac'
                                new_vac = data_temp.copy()
                                new_vac = new_vac[new_vac['identity'] == drop_val]
                                new_vac = new_vac[[var, 'rol_' + var]]
                                rebench_to_check = rebench_to_check.join(new_vac)
                            else:
                                rol_var = data_temp.copy()
                                rol_var = rol_var[rol_var['identity'] == drop_val]
                                rol_var = rol_var[['rol_' + var]]
                                rebench_to_check = rebench_to_check.join(rol_var)
                            orig_to_check = data_orig.copy()
                            orig_to_check['vac_orig'] = orig_to_check['avail'] / orig_to_check['inv']
                            rebench_to_check = rebench_to_check[[var, 'rol_' + var]]
                            if var == "vac":
                                orig_to_check = orig_to_check[[var + "_orig"]]
                            else:
                                orig_to_check = orig_to_check[[var]]
                                orig_to_check = orig_to_check.rename(columns={var: var + "_orig"})
                            rebench_to_check = rebench_to_check.join(orig_to_check)
                            if var == "vac":
                                thresh = 0.03
                                rebench_to_check['diff_to_orig'] = rebench_to_check[var] - rebench_to_check[var + "_orig"]
                                rebench_to_check['diff_to_rol'] = rebench_to_check[var] - rebench_to_check['rol_' + var]
                                rebench_to_check['orig_diff_to_rol'] = rebench_to_check[var + "_orig"] - rebench_to_check['rol_' + var]
                            else:
                                thresh = 0.05
                                rebench_to_check['diff_to_orig'] = (rebench_to_check[var] - rebench_to_check[var + "_orig"]) / rebench_to_check[var + "_orig"]
                                rebench_to_check['diff_to_rol'] = (rebench_to_check[var] - rebench_to_check['rol_' + var]) / rebench_to_check['rol_' + var]
                                rebench_to_check['orig_diff_to_rol'] = (rebench_to_check[var + "_orig"] - rebench_to_check['rol_' + var]) / rebench_to_check['rol_' + var]
                                
                            rebench_to_check = rebench_to_check[((abs(rebench_to_check['diff_to_orig']) >= thresh) & (abs(rebench_to_check['diff_to_rol']) >= thresh) & (abs(rebench_to_check['diff_to_rol']) >= abs(rebench_to_check['diff_to_orig']))) | 
                                                                ((abs(rebench_to_check['diff_to_rol']) > abs(rebench_to_check['orig_diff_to_rol'])) & (abs(rebench_to_check['diff_to_rol']) >= thresh))]

                            if len(rebench_to_check) > 0 and var == "vac":
                                avail_check = True
                            if len(rebench_to_check) > 0 and var == "mrent":
                                mrent_check = True
                            elif len(rebench_to_check) > 0 and var == "merent":
                                merent_check = True
            
            if avail_check == False and mrent_check == False and merent_check == False:
                has_diff = 1
                data = data_temp.copy()
            else:
                has_diff = 2
        else:
            has_diff = 1
            data = data_temp.copy()           
    else:
        has_diff = 0

    return data, has_diff, avail_check, mrent_check, merent_check

# Function to insert the suggested or user fix to the fixed dataframe for review by user, as originally formatted by BB before HSY suggestion of using model coefficients directly
def insert_fix(dataframe, row_to_fix, identity_val, fix, variable_fix, curryr, currmon, sector_val, subsequent_chg):
    
    if sector_val == "apt":
        round_val = 0
    else: 
        round_val = -3

    if variable_fix == "i":
        orig_inv = dataframe.loc[row_to_fix]['inv']
        inv_diff = fix - orig_inv
    elif variable_fix == "c":
        orig_cons = dataframe.loc[row_to_fix]['cons']
        cons_diff = fix - orig_cons
    elif variable_fix == "a":
        orig_conv = dataframe.loc[row_to_fix]['conv']
        cv_diff = fix - orig_conv
    elif variable_fix == "d":
        orig_demo = dataframe.loc[row_to_fix]['demo']
        cv_diff = fix - orig_demo
    elif variable_fix == "v":
        orig_avail = dataframe.loc[row_to_fix]['avail']
        avail_diff = fix - orig_avail
    # Since there is no way for a user to enter a null shim, and there may be cases where they want to actually have a null value (example: tier 3 subs where a mistaken shim was entered for merent, creating a value where there previously was not one) if the user enteres a zero for mrent or merent we will treat that as null out
    elif (variable_fix == "g" or variable_fix == "e") and fix == 0:
        fix = np.nan

    dataframe = dataframe.reset_index()
    index_row = dataframe.index[dataframe['identity_row'] == row_to_fix].tolist()[0]
    l_index_row = dataframe.index[(dataframe['identity'] == identity_val) & (dataframe['yr'] == curryr) & (dataframe['currmon'] == currmon)].tolist()[0]
    dataframe = dataframe.reset_index()
    dataframe = dataframe.rename(columns={'index': 'index_row'})
     
    if variable_fix == "i":
        dataframe.loc[index_row, 'inv'] = fix
        dataframe['inv'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] > index_row), dataframe['inv'] + inv_diff, dataframe['inv'])
        if inv_diff > 0:
            dataframe['occ'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] >= index_row), dataframe['occ'] + inv_diff, dataframe['occ'])
        else:
            dataframe['occ'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] >= index_row), dataframe['inv'] * (1 - dataframe['vac']), dataframe['occ'])
            dataframe['occ'] = round(dataframe['occ'], round_val)

        dataframe['avail'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] >= index_row), dataframe['inv'] - dataframe['occ'], dataframe['avail'])
        dataframe['vac'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] >= index_row), dataframe['avail'] / dataframe['inv'], dataframe['vac'])
        dataframe['vac'] = round(dataframe['vac'], 4)
        dataframe['vac_chg'] = np.where((dataframe['identity'] == identity_val) & (dataframe['identity'] == dataframe['identity'].shift(1)) & (dataframe['index_row'] >= index_row), dataframe['vac'] - dataframe['vac'].shift(1), dataframe['vac_chg'])
        dataframe['abs'] = np.where((dataframe['identity'] == identity_val) & (dataframe['identity'] == dataframe['identity'].shift(1)) & (dataframe['index_row'] >= index_row), dataframe['occ'] - dataframe['occ'].shift(1), dataframe['abs'])
    elif variable_fix == "c":
        dataframe.loc[index_row, 'cons'] = fix
        dataframe['inv'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] >= index_row), dataframe['inv'] + cons_diff, dataframe['inv'])
        dataframe['occ'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] >= index_row), dataframe['occ'] + cons_diff, dataframe['occ'])
        dataframe['vac'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] >= index_row), dataframe['avail'] / dataframe['inv'], dataframe['vac'])
        dataframe['vac'] = round(dataframe['vac'], 4)
        dataframe['vac_chg'] = np.where((dataframe['identity'] == identity_val) & (dataframe['identity'] == dataframe['identity'].shift(1)) & (dataframe['index_row'] >= index_row), dataframe['vac'] - dataframe['vac'].shift(1), dataframe['vac_chg'])
        dataframe['abs'] = np.where((dataframe['identity'] == identity_val) & (dataframe['identity'] == dataframe['identity'].shift(1)) & (dataframe['index_row'] >= index_row), dataframe['occ'] - dataframe['occ'].shift(1), dataframe['abs'])
    elif variable_fix == "a" or variable_fix == "d":
        if variable_fix == "a":
            dataframe.loc[index_row, 'conv'] = fix
            dataframe['inv'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] >= index_row), dataframe['inv'] + cv_diff, dataframe['inv'])
            if cv_diff > 0:
                dataframe['occ'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] >= index_row), dataframe['occ'] + cv_diff, dataframe['occ'])
            else:
                dataframe['avail'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] >= index_row), dataframe['avail'] + cv_diff, dataframe['avail'])
        elif variable_fix == "d":
            dataframe.loc[index_row, 'demo'] = fix
            dataframe['inv'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] >= index_row), dataframe['inv'] + cv_diff, dataframe['inv'])
            dataframe['avail'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] >= index_row), dataframe['avail'] + cv_diff, dataframe['avail'])
        dataframe['vac'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] >= index_row), dataframe['avail'] / dataframe['inv'], dataframe['vac'])
        dataframe['vac'] = round(dataframe['vac'], 4)
        dataframe['vac_chg'] = np.where((dataframe['identity'] == identity_val) & (dataframe['identity'] == dataframe['identity'].shift(1)) & (dataframe['index_row'] >= index_row), dataframe['vac'] - dataframe['vac'].shift(1), dataframe['vac_chg'])
        dataframe['abs'] = np.where((dataframe['identity'] == identity_val) & (dataframe['identity'] == dataframe['identity'].shift(1)) & (dataframe['index_row'] >= index_row), dataframe['occ'] - dataframe['occ'].shift(1), dataframe['abs'])
    elif variable_fix == "v":
        if subsequent_chg == "r":
            dataframe['avail'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] == index_row), dataframe['avail'] + avail_diff, dataframe['avail'])
            dataframe['avail'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] > index_row) & (dataframe['avail'] + avail_diff >= 0) & (dataframe['avail'] + avail_diff <= dataframe['inv']), dataframe['avail'] + avail_diff, dataframe['avail'])
            dataframe['avail'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] > index_row) & (dataframe['avail'] + avail_diff < 0), 0, dataframe['avail'])
            dataframe['avail'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] > index_row) & (dataframe['avail'] + avail_diff > dataframe['inv']), dataframe['inv'], dataframe['avail'])
        else:
            dataframe['avail'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] == index_row), dataframe['avail'] + avail_diff, dataframe['avail'])
            for index_val in range(index_row + 1, l_index_row + 1):
                dataframe['avail'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] == index_val) & (dataframe['avail'].shift(1) + (dataframe['sqavail'] - dataframe['sqavail'].shift(1)) >= 0) & (dataframe['avail'].shift(1) + (dataframe['sqavail'] - dataframe['sqavail'].shift(1)) <= dataframe['inv']), dataframe['avail'].shift(1) + (dataframe['sqavail'] - dataframe['sqavail'].shift(1)), dataframe['avail'])
                dataframe['avail'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] == index_val) & (dataframe['avail'].shift(1) + (dataframe['sqavail'] - dataframe['sqavail'].shift(1)) < 0), dataframe['avail'].shift(1), dataframe['avail'])
                dataframe['avail'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] == index_val) & (dataframe['avail'].shift(1) + (dataframe['sqavail'] - dataframe['sqavail'].shift(1)) > dataframe['inv']), dataframe['avail'].shift(1), dataframe['avail'])
        dataframe['vac'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] >= index_row), dataframe['avail'] / dataframe['inv'], dataframe['vac'])
        dataframe['vac'] = round(dataframe['vac'], 4)
        dataframe['occ'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] >= index_row), dataframe['inv'] - dataframe['avail'], dataframe['occ'])
        dataframe['vac_chg'] = np.where((dataframe['identity'] == identity_val) & (dataframe['identity'] == dataframe['identity'].shift(1)) & (dataframe['index_row'] >= index_row), dataframe['vac'] - dataframe['vac'].shift(1), dataframe['vac_chg'])
        dataframe['abs'] = np.where((dataframe['identity'] == identity_val) & (dataframe['identity'] == dataframe['identity'].shift(1)) & (dataframe['index_row'] >= index_row), dataframe['occ'] - dataframe['occ'].shift(1), dataframe['abs'])
    elif variable_fix == "g":
        dataframe.loc[index_row, 'mrent'] = fix
        dataframe['G_mrent'] = np.where((dataframe['identity'] == identity_val) & (dataframe['identity'] == dataframe['identity'].shift(1)) & (dataframe['index_row'] == index_row), (dataframe['mrent'] - dataframe['mrent'].shift(1)) / dataframe['mrent'].shift(1), dataframe['G_mrent'])
        if subsequent_chg == "r":
            for index_val in range(index_row + 1, l_index_row + 1):
                dataframe['mrent'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] == index_val), dataframe['mrent'].shift(1) * (1 + dataframe['G_mrent']), dataframe['mrent'])
            dataframe['mrent'] = round(dataframe['mrent'], 2)
        elif subsequent_chg == "s":
            for index_val in range(index_row + 1, l_index_row + 1):
                dataframe['mrent'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] == index_val) & (dataframe['identity'] == dataframe['identity'].shift(-1)), dataframe['mrent'].shift(1) * (1 + dataframe['sq_Gmrent']), dataframe['mrent'])
                dataframe['mrent'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] == index_val) & (dataframe['identity'] != dataframe['identity'].shift(-1)), dataframe['mrent'].shift(1) * (1 + dataframe['G_mrent']), dataframe['mrent'])
                dataframe['mrent'] = round(dataframe['mrent'], 2)
            dataframe['G_mrent'] = np.where((dataframe['identity'] == identity_val) & (dataframe['identity'] == dataframe['identity'].shift(1)) & (dataframe['index_row'] > index_row), (dataframe['mrent'] - dataframe['mrent'].shift(1)) / dataframe['mrent'].shift(1), dataframe['G_mrent'])
        dataframe['merent'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] >= index_row) & (np.isnan(dataframe['merent']) == False), dataframe['mrent'] - (dataframe['gap'] * dataframe['mrent']), dataframe['merent'])
        dataframe['merent'] = round(dataframe['merent'], 2)
        dataframe['G_merent'] = np.where((dataframe['identity'] == identity_val) & (dataframe['identity'] == dataframe['identity'].shift(1)) & (dataframe['index_row'] >= index_row) & (np.isnan(dataframe['G_merent']) == False), (dataframe['merent'] - dataframe['merent'].shift(1)) / dataframe['merent'].shift(1), dataframe['G_merent'])
        dataframe['gap'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] >= index_row), ((dataframe['merent'] - dataframe['mrent']) / dataframe['mrent']) * -1, dataframe['gap'])
        dataframe['gap'] = round(dataframe['gap'], 4)
        dataframe['gap_chg'] = np.where((dataframe['identity'] == identity_val) & (dataframe['identity'] == dataframe['identity'].shift(1)) & (dataframe['index_row'] == index_row), dataframe['gap'] - dataframe['gap'].shift(1), dataframe['gap_chg'])
    elif variable_fix == "e":
        dataframe.loc[index_row, 'merent'] = fix
        dataframe['G_merent'] = np.where((dataframe['identity'] == identity_val) & (dataframe['identity'] == dataframe['identity'].shift(1)) & (dataframe['index_row'] == index_row), (dataframe['merent'] - dataframe['merent'].shift(1)) / dataframe['merent'].shift(1), dataframe['G_merent'])
        for index_val in range(index_row + 1, l_index_row + 1):
            dataframe['merent'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] == index_val), dataframe['merent'].shift(1) * (1 + dataframe['G_merent']), dataframe['merent'])
        dataframe['merent'] = round(dataframe['merent'], 2)
        dataframe['gap'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] >= index_row), ((dataframe['merent'] - dataframe['mrent']) / dataframe['mrent']) * -1, dataframe['gap'])
        dataframe['gap'] = round(dataframe['gap'], 4)
        dataframe['gap_chg'] = np.where((dataframe['identity'] == identity_val) & (dataframe['identity'] == dataframe['identity'].shift(1)) & (dataframe['index_row'] == index_row), dataframe['gap'] - dataframe['gap'].shift(1), dataframe['gap_chg'])

    dataframe = dataframe.set_index('identity_row')
    dataframe = dataframe.drop(['index_row'], axis=1)
    dataframe['inv'] = round(dataframe['inv'], round_val)
    dataframe['avail'] = round(dataframe['avail'], round_val)
    dataframe['occ'] = round(dataframe['occ'], round_val)
    dataframe['abs'] = round(dataframe['abs'], round_val)
    dataframe['vac'] = round(dataframe['vac'], 4)
    dataframe['vac_chg'] = round(dataframe['vac_chg'], 4)
    dataframe['mrent'] = round(dataframe['mrent'], 2)
    dataframe['merent'] = round(dataframe['merent'], 2)
    dataframe['G_mrent'] = round(dataframe['G_mrent'], 4)
    dataframe['G_merent'] = round(dataframe['G_merent'], 4)
    dataframe['gap'] = round(dataframe['gap'], 4)
    dataframe['gap_chg'] = round(dataframe['gap_chg'], 4)
    
    return dataframe

# Function to identify if a submarket has a flag for review
def flag_examine(data, identity_val, filt, curryr, currmon, flag_cols, flag_flow, test_auto_rebench, sector_val):
    
    if test_auto_rebench == True:
        avail_check, mrent_check, merent_check, identity_for_check = rebench_check(data, curryr, currmon, sector_val, True, True, True)
        if avail_check == False and mrent_check == False and merent_check == False:
            test_auto_rebench = False
        else:
            flag_list = ['v_flag']
            skip_list = []
            has_flag = 3
            identity_val = identity_for_check    
    else:
        avail_check = False
        mrent_check = False
        merent_check = False 

    if test_auto_rebench == False:
    
        dataframe = data.copy()
        has_flag = 0
        skip_list = []
        if filt == True:
            dataframe = dataframe[dataframe['identity'] == identity_val]
            skip_list = list(dataframe.loc[identity_val + str(curryr) + str(currmon)][['flag_skip']])
            if skip_list[0] == '':
                skip_list = []
        else:
            first_sub = dataframe.reset_index().loc[0]['identity']
            if flag_flow == "cat":
                dataframe_test = dataframe.copy()

                dataframe_test[flag_cols] = np.where((dataframe_test[flag_cols] != 0), 1, dataframe_test[flag_cols])

                rol_flag_cols = [x for x in flag_cols if "rol" in x or x == "v_flag_low" or x == "v_flag_high" or x == "e_flag_high" or x == "e_flag_low" or x == "c_flag_sqdiff"]
                test_flag_cols = [x + "_test" for x in rol_flag_cols]
                dataframe_test[test_flag_cols] = dataframe_test.groupby('identity')[rol_flag_cols].transform('sum')
                for x, y in zip(rol_flag_cols, test_flag_cols):
                    dataframe_test[x] = np.where((dataframe_test[y] > 0) & (dataframe_test['curr_tag'] == 1), 1, 0)
                    dataframe_test[x] = np.where(dataframe_test['curr_tag'] == 0, 0, dataframe_test[x])
                dataframe_test.drop(test_flag_cols, axis=1, inplace=True)
                
                dataframe_test['has_c'] = dataframe_test.filter(regex="^c_flag*").sum(axis=1)
                dataframe_test['has_v'] = dataframe_test.filter(regex="^v_flag*").sum(axis=1)
                dataframe_test['has_g'] = dataframe_test.filter(regex="^g_flag*").sum(axis=1)
                dataframe_test['has_e'] = dataframe_test.filter(regex="^e_flag*").sum(axis=1)

                dataframe_test['has_c'] = dataframe_test['has_c'] -  dataframe_test['flag_skip'].str.count('c_flag')
                dataframe_test['has_v'] = dataframe_test['has_v'] - dataframe_test['flag_skip'].str.count('v_flag')
                dataframe_test['has_g'] = dataframe_test['has_g'] - dataframe_test['flag_skip'].str.count('g_flag')
                dataframe_test['has_e'] = dataframe_test['has_e'] - dataframe_test['flag_skip'].str.count('e_flag')

                for x in ['has_c', 'has_v', 'has_g', 'has_e']:
                    dataframe_test_1 = dataframe_test.copy()
                    dataframe_test_1 = dataframe_test_1[dataframe_test_1[x] > 0]
                    if len(dataframe_test_1) > 0:
                        dataframe = dataframe_test_1.copy()
                        break
            elif flag_flow == "flag":
                dataframe_test = dataframe.copy()
                rol_flag_cols = [x for x in flag_cols if "rol" in x or x == "v_flag_low" or x == "v_flag_high" or x == "e_flag_high" or x == "e_flag_low" or x == "c_flag_sqdiff"]
                test_flag_cols = [x + "_test" for x in rol_flag_cols]
                dataframe_test[test_flag_cols] = dataframe_test.groupby('identity')[rol_flag_cols].transform('sum')
                for x, y in zip(rol_flag_cols, test_flag_cols):
                    dataframe_test[x] = np.where((dataframe_test[y] > 0) & (dataframe_test['curr_tag'] == 1), 1, 0)
                    dataframe_test[x] = np.where(dataframe_test['curr_tag'] == 0, 0, dataframe_test[x])
                dataframe_test.drop(test_flag_cols, axis=1, inplace=True)
                
                for x in flag_cols:
                    dataframe_test_1 = dataframe_test.copy()
                    dataframe_test_1 = dataframe_test_1[(dataframe_test_1[x] > 0) & (dataframe_test['flag_skip'].str.contains(x) == False)]
                    if len(dataframe_test_1) > 0:
                        dataframe = dataframe_test_1.copy()
                        break

        cols_to_keep = flag_cols + ['identity', 'flag_skip']
        dataframe = dataframe[cols_to_keep]
        dataframe[flag_cols] = np.where((dataframe[flag_cols] > 0), 1, dataframe[flag_cols])
        rol_flag_cols = [x for x in flag_cols if "rol" in x or x == "v_flag_low" or x == "v_flag_high" or x == "e_flag_high" or x == "e_flag_low" or x == "c_flag_sqdiff"]
        for x in rol_flag_cols:
            dataframe[x + "_test"] = dataframe.groupby('identity')[x].transform('sum')
            dataframe[x] = np.where(dataframe[x] == 1, dataframe[x] / dataframe[x + "_test"], dataframe[x])
        dataframe['sum_flags'] = dataframe[flag_cols].sum(axis=1)
        dataframe['sum_commas'] = dataframe['flag_skip'].str.count(',')
        dataframe['sum_skips'] = np.where((dataframe['flag_skip'] == ''), 0, np.nan)
        dataframe['sum_skips'] = np.where((dataframe['flag_skip'] != ''), dataframe['sum_commas'] + 1, dataframe['sum_skips'])
        dataframe['total_flags'] = dataframe.groupby('identity')['sum_flags'].transform('sum')
        dataframe['total_skips'] = dataframe.groupby('identity')['sum_skips'].transform('sum')
        dataframe['flags_left'] = round(dataframe['total_flags'] - dataframe['total_skips'],0)
        dataframe = dataframe[dataframe['flags_left'] > 0]
        
        if len(dataframe) == 0:
            if filt == True:
                has_flag = 2
                flag_list = ['v_flag']
            else:
                if identity_val is None:
                    identity_val = first_sub
                has_flag = 0
                flag_list = ['v_flag']
        else:
            if filt == False:
                identity_val = dataframe.reset_index().loc[0]['identity']
                dataframe = dataframe[dataframe['identity'] == identity_val]
            flags = dataframe.copy()
            flags = flags[flag_cols + ['flag_skip']]
            flags = flags[flags.columns[(flags != 0).any()]]
            flag_list = list(flags.columns)
            flags = flags.reset_index()
            skip_list = str.split(flags.loc[flags.index[-1]]['flag_skip'].replace(",", ""))
            flag_list = [x for x in flag_list if x not in skip_list]
            flag_list.remove('flag_skip')
            
            if len(flag_list) > 0:
                has_flag = 1
            else:
                has_flag = 0
                if filt == True:
                    flag_list = ['v_flag']
                    has_flag = 2
                else:
                    if identity_val is None:
                        identity_val = dataframe.reset_index().loc[0]['identity']  
                    flag_list = ['v_flag']

    return flag_list, skip_list, identity_val, has_flag, test_auto_rebench, avail_check, mrent_check, merent_check

# This function creates the tables to be used for metro sorts
def metro_sorts(rolled, data, roll_val, curryr, currmon, sector_val, sorts_val):
    frames = []
    
    for x in ['identity', 'identity_met']:
        if x == "identity":
            rank = data.copy()
            rank = rank[rank['curr_tag'] == 1]
        elif x == "identity_met":
            rank = rolled.copy()
            if sector_val == "ind":
                if "DW" in roll_val:
                    rank = rank[rank['subsector'] == "DW"]
                else:
                    rank = rank[rank['subsector'] == "F"]
            rank['identity_met'] = rank['metcode'] + rank['subsector']
            rank = calc_3mos(rank, curryr, currmon, "sorts")
            if currmon != 1:
                rank = calc_ytd(rank, curryr, currmon, "sorts")
            else:
                rank['final_cons_ytd'] = rank['cons']
                rank['final_vacchg_ytd'] = rank['vac_chg']
                rank['final_abs_ytd'] = rank['abs']
                rank['final_Gmrent_ytd'] = rank['G_mrent']
                rank['final_Gmerent_ytd'] = rank['G_merent']
            rank = rank[(rank['yr'] == curryr) & (rank['currmon'] == currmon)]

        rank_col_name = sorts_val + "_rank"
        if "vac" in sorts_val or "gap" in sorts_val:
            sort_order = True
        else:
            sort_order = False
        rank[rank_col_name] = rank[sorts_val].rank(ascending=sort_order, method='min')

        if x == "identity_met":
            rank = rank.drop_duplicates(['identity_met'])    

        if x == "identity_met":
            if sorts_val == "cons":
                support_cols = ['final_cons_last3mos', 'final_cons_ytd']
                join_cols = []
            elif sorts_val == "vac_chg":
                support_cols = ['met_wtdvacchg', 'met_sur_v_cov_perc', 'final_vacchg_last3mos', 'final_vacchg_ytd']
                join_cols = ['met_wtdvacchg', 'met_sur_v_cov_perc']
            elif sorts_val == "abs":
                support_cols = ['met_sur_totabs', 'met_sur_v_cov_perc', 'final_abs_last3mos', 'final_abs_ytd']
                join_cols = ['met_sur_v_cov_perc']
            elif sorts_val == "G_mrent":
                support_cols = ['metsq_Gmrent', 'met_sur_r_cov_perc', 'met_g_renx_mo_wgt', 'final_Gmrent_last3mos', 'final_Gmrent_ytd']
                join_cols = ['met_sur_r_cov_perc']
            elif sorts_val == "gap_chg":
                support_cols = ['vac_chg', 'final_gapchg_last3mo', 'final_gapchg_ytd']
                join_cols = []

            support = data.copy()
            support = support[(data['yr'] == curryr) & (support['currmon'] == currmon)]
            support = support[join_cols + ['identity_met']]
            support = support.drop_duplicates('identity_met')
            rank = rank.join(support.set_index('identity_met'), on='identity_met')

        rank = rank.set_index(x)

        if x == "identity_met":
            rank = rank[['metcode'] + [rank_col_name] + [sorts_val] + support_cols]
        else:
            rank = rank[['metcode', 'subid'] + [rank_col_name, sorts_val]]

        rank.sort_values(by=[rank_col_name], ascending=[True], inplace=True)
        
        if x == "identity_met":
            rank = rank.reset_index()
            test = rank.index[rank['identity_met'] == roll_val].tolist()
            rank = pd.concat([rank.iloc[[test[0]],:], rank.drop(test[0], axis=0)], axis=0)
            rank = rank.set_index('identity_met')

        frames.append(rank)

    return frames[0], frames[1]

def calc_3mos(dataframe_in, curryr, currmon, type_filt):
    dataframe = dataframe_in.copy()

    if currmon >= 3:
        dataframe[['final_cons_last3mos', 'final_abs_last3mos']] = dataframe[(dataframe['yr'] == curryr) & (dataframe['currmon'] >= currmon - 2) & (dataframe['currmon'] <= currmon)].groupby('identity_met')[['cons', 'abs']].transform('sum')
    elif currmon == 2:
        dataframe[['final_cons_last3mos', 'final_abs_last3mos']] = dataframe[(dataframe['yr'] == curryr) | ((dataframe['yr'] == curryr - 1) & (dataframe['currmon'] == 12))].groupby('identity_met')[['cons', 'abs']].transform('sum')
    elif currmon == 1:
        dataframe[['final_cons_last3mos', 'final_abs_last3mos']] = dataframe[(dataframe['yr'] == curryr) | ((dataframe['yr'] == curryr - 1) & (dataframe['currmon'] >= 11))].groupby('identity_met')[['cons', 'abs']].transform('sum')

    if type_filt == "packet":
        dataframe['mrev'] = dataframe['mrent'] * dataframe['inv']
        dataframe['erev'] = dataframe['merent'] * dataframe['inv']
        dataframe[['met_avail', 'met_mrev', 'met_erev', 'met_inv']] = dataframe.groupby(['identity_met', 'yr', 'currmon'])[['avail', 'mrev', 'erev', 'inv']].transform('sum')
        dataframe['met_vac'] = dataframe['met_avail'] / dataframe['met_inv']
        dataframe['met_vac'] = round(dataframe['met_vac'], 4)
        dataframe['final_vacchg_last3mos'] = np.where(dataframe['identity_met'] == dataframe['identity_met'].shift(3), dataframe['met_vac'] - dataframe['met_vac'].shift(3), np.nan)
    else:
        dataframe['final_vacchg_last3mos'] = np.where(dataframe['identity_met'] == dataframe['identity_met'].shift(3), dataframe['vac'] - dataframe['vac'].shift(3), np.nan)

    if type_filt == "packet":
        dataframe['met_mrent'] = dataframe['met_mrev'] / dataframe['met_inv']
        dataframe['met_mrent'] = round(dataframe['met_mrent'], 2)
        dataframe['final_Gmrent_last3mos'] = np.where(dataframe['identity_met'] == dataframe['identity_met'].shift(3), (dataframe['met_mrent'] - dataframe['met_mrent'].shift(3)) / dataframe['met_mrent'].shift(3), np.nan)
    else:
        dataframe['final_Gmrent_last3mos'] = np.where(dataframe['identity_met'] == dataframe['identity_met'].shift(3), (dataframe['mrent'] - dataframe['mrent'].shift(3)) / dataframe['mrent'].shift(3), np.nan)
    
    if type_filt == "packet":    
        dataframe['met_erent'] = dataframe['met_erev'] / dataframe['met_inv']
        dataframe['met_erent'] = round(dataframe['met_erent'], 2)
        dataframe['final_Gmerent_last3mo'] = np.where(dataframe['identity_met'] == dataframe['identity_met'].shift(3), (dataframe['met_erent'] - dataframe['met_erent'].shift(3)) / dataframe['met_erent'].shift(3), np.nan)
    
    if type_filt == "packet":
        dataframe['met_gap'] = ((dataframe['met_erent'] - dataframe['met_mrent']) / dataframe['met_mrent']) * -1
        dataframe['final_gapchg_last3mo'] = np.where(dataframe['identity_met'] == dataframe['identity_met'].shift(3), dataframe['met_gap'] - dataframe['met_gap'].shift(3), np.nan)
    else:
        dataframe['final_gapchg_last3mo'] = np.where(dataframe['identity_met'] == dataframe['identity_met'].shift(3), dataframe['gap'] - dataframe['gap'].shift(3), np.nan)
    
    if type_filt == "packet":
        dataframe = dataframe.drop(['met_avail', 'met_inv', 'met_vac', 'mrev', 'met_mrev', 'met_mrent', 'erev', 'met_erev', 'met_erent', 'met_gap'], axis=1)

    return dataframe

def calc_ytd(dataframe_in, curryr, currmon, type_filt):
    
    dataframe = dataframe_in.copy()
    
    dataframe[['final_cons_ytd', 'final_abs_ytd']] = dataframe[dataframe['yr'] == curryr].groupby('identity_met')[['cons', 'abs']].transform('sum')

    if type_filt == "packet":
        dataframe['mrev'] = dataframe['mrent'] * dataframe['inv']
        dataframe['erev'] = dataframe['merent'] * dataframe['inv']

        dataframe[['met_avail', 'met_inv', 'met_mrev', 'met_erev']] = dataframe.groupby(['identity_met', 'yr', 'currmon'])[['avail', 'inv', 'mrev', 'erev']].transform('sum')
    
    if type_filt == "packet":
        dataframe['met_vac'] = dataframe['met_avail'] / dataframe['met_inv']
        dataframe['met_vac'] = round(dataframe['met_vac'], 4)
        dataframe['final_vacchg_ytd'] = np.where(dataframe['identity_met'] == dataframe['identity_met'].shift(currmon), dataframe['met_vac'] - dataframe['met_vac'].shift(currmon), np.nan)
    else:
        dataframe['final_vacchg_ytd'] = np.where(dataframe['identity_met'] == dataframe['identity_met'].shift(currmon), dataframe['vac'] - dataframe['vac'].shift(currmon), np.nan)

    if type_filt == "packet":
        dataframe['met_mrent'] = dataframe['met_mrev'] / dataframe['met_inv']
        dataframe['met_mrent'] = round(dataframe['met_mrent'], 2)
        dataframe['final_Gmrent_ytd'] = np.where(dataframe['identity_met'] == dataframe['identity_met'].shift(currmon), (dataframe['met_mrent'] - dataframe['met_mrent'].shift(currmon)) / dataframe['met_mrent'].shift(currmon), np.nan)
    else:
        dataframe['final_Gmrent_ytd'] = np.where(dataframe['identity_met'] == dataframe['identity_met'].shift(currmon), (dataframe['mrent'] - dataframe['mrent'].shift(currmon)) / dataframe['mrent'].shift(currmon), np.nan)

    if type_filt == "packet":
        dataframe['met_erent'] = dataframe['met_erev'] / dataframe['met_inv']
        dataframe['met_erent'] = round(dataframe['met_erent'], 2)
        dataframe['final_Gmerent_ytd'] = np.where(dataframe['identity_met'] == dataframe['identity_met'].shift(currmon), (dataframe['met_erent'] - dataframe['met_erent'].shift(currmon)) / dataframe['met_erent'].shift(currmon), np.nan)
    else:
        dataframe['final_gapchg_ytd'] = np.where(dataframe['identity_met'] == dataframe['identity_met'].shift(currmon), dataframe['gap'] - dataframe['gap'].shift(currmon), np.nan)

    if type_filt == "packet":
        dataframe = dataframe.drop(['met_avail', 'met_inv', 'met_vac', 'mrev', 'met_mrev', 'met_mrent', 'erev', 'met_erev', 'met_erent'], axis=1)

    return dataframe


# This function rolls up the edited data to the metro and US level for presentation to econ
def create_review_packet(data_in, curryr, currmon, sector_val):

    us_roll = data_in.copy()
        
    # If sector is Ind, drop expansion mets before rolilng the US
    if sector_val == "ind":
        us_roll = us_roll[us_roll['expansion'] == "Leg"]

    if sector_val == "apt" or sector_val == "off" or sector_val == "ret":
        identity_val = 'tier'
    else:
        identity_val = 'subsector'   
    
    us_roll.sort_values(by=[identity_val, 'yr', 'qtr', 'currmon', 'metcode', 'subid'], inplace=True)

    if sector_val != "ind":
        us_roll = us_roll[us_roll['yr'] >= 2008]

    us_roll['askrevenue'] = us_roll['inv'] * us_roll['mrent']
    us_roll['effrevenue'] = us_roll['inv'] * us_roll['merent']

    cols_to_roll = ['inv', 'cons', 'avail', 'occ', 'abs', 'askrevenue', 'effrevenue', 'conv', 'demo']
    for x in cols_to_roll:
        us_roll[x + "_sum"] = us_roll.groupby([identity_val, 'yr', 'qtr', 'currmon'])[x].transform('sum')
        us_roll = us_roll.drop([x], axis=1)
        us_roll = us_roll.rename(columns={x + "_sum": x})
    us_roll = us_roll.drop_duplicates([identity_val, 'yr', 'qtr', 'currmon'])

    us_roll['vac'] = round(us_roll['avail'] / us_roll['inv'], 4)
    us_roll['vac_chg'] = np.where((us_roll[identity_val] == us_roll[identity_val].shift(1)), us_roll['vac'] - us_roll['vac'].shift(1), np.nan)
    
    us_roll['mrent'] = round(us_roll['askrevenue'] / us_roll['inv'],2)
    us_roll['merent'] = round(us_roll['effrevenue'] / us_roll['inv'],2)
    
    us_roll['Gmrent'] = np.where((us_roll[identity_val] == us_roll[identity_val].shift(1)), (us_roll['mrent'] - us_roll['mrent'].shift(1)) / us_roll['mrent'].shift(1), np.nan)
    us_roll['Gmerent'] = np.where((us_roll[identity_val] == us_roll[identity_val].shift(1)), (us_roll['merent'] - us_roll['merent'].shift(1)) / us_roll['merent'].shift(1), np.nan)
    
    us_roll['gap'] = (us_roll['mrent'] - us_roll['merent']) / us_roll['mrent']

    if sector_val == "apt" or sector_val == "off":
        us_roll["merent"] = np.where(us_roll['tier'] > 1, np.nan, us_roll["merent"])
        us_roll["Gmerent"] = np.where(us_roll['tier'] > 1, np.nan, us_roll["Gmerent"])
        us_roll["gap"] = np.where(us_roll['tier'] > 1, np.nan, us_roll["gap"])

    us_roll['sector'] = sector_val.title()

    if sector_val == "ind":
        us_roll['trend_yr1'] = np.where(us_roll['expansion'] == "Exp", 2017, 2009)

    if sector_val == "ret":
        us_roll = us_roll.drop_duplicates(['tier', 'yr', 'currmon'])
        us_roll = us_roll[us_roll['tier'] == 1]
    
    if sector_val != "ind":
        cols_to_display = ['sector', 'subsector', 'tier', 'yr', 'qtr', 'currmon', 'inv', 'cons', 'conv', 'demo', 'vac', 'vac_chg', 'avail', 'occ', 'abs', 'mrent', 'Gmrent', 'merent', 'Gmerent', 'gap']
        if sector_val == "apt" or sector_val == "off":
            cols_to_display.remove('subsector')
    else:
        cols_to_display = ['sector', 'subsector', 'trend_yr1', 'yr', 'qtr', 'currmon', 'inv', 'cons', 'vac', 'vac_chg', 'avail', 'occ', 'abs', 'mrent', 'Gmrent', 'merent', 'Gmerent', 'gap']
    us_roll = us_roll[cols_to_display]

    for x in cols_to_display:
        if x not in ['sector', 'subsector', 'tier', 'trend_yr1', 'yr', 'qtr', 'currmon']:
            us_roll.rename(columns={x: x + "_nat"}, inplace=True)
    
    us_roll = us_roll[(us_roll['yr'] >= 2009)]

    if sector_val == "ret": 
        us_roll['subsector'] = "NC"

    us_roll['vac_chg_nat'] = round(us_roll['vac_chg_nat'], 4)
    us_roll['Gmrent_nat'] = round(us_roll['Gmrent_nat'], 4)
    us_roll['Gmerent_nat'] = round(us_roll['Gmerent_nat'], 4)
    us_roll['gap_nat'] = round(us_roll['gap_nat'], 4)

    if sector_val != "ind":
        us_roll.sort_values(by=['tier', 'yr', 'currmon'], ascending=[True, True, True], inplace=True)

    met_roll = data_in.copy()

    if sector_val == "ret":
        use_roll = "metcode"
    else:
        use_roll = "subsector"

    if currmon == 1:
         met_roll = met_roll[(met_roll['yr'] == curryr) | ((met_roll['yr'] == curryr - 1) & (met_roll['currmon'] == 12))]
    else:
        met_roll = met_roll[(met_roll['yr'] == curryr) & (met_roll['currmon'] >= currmon - 1)]

    met_roll['askrevenue'] = met_roll['inv'] * met_roll['mrent']
    met_roll['effrevenue'] = met_roll['inv'] * met_roll['merent']

    cols_to_roll = ['inv', 'cons', 'avail', 'occ', 'abs', 'askrevenue', 'effrevenue']
    for x in cols_to_roll:
        met_roll[x + "_sum"] = met_roll.groupby([use_roll, 'metcode', 'yr', 'currmon'])[x].transform('sum')
        met_roll = met_roll.drop([x], axis=1)
        met_roll = met_roll.rename(columns={x + "_sum": x})
    met_roll = met_roll.drop_duplicates([use_roll, 'metcode', 'yr', 'currmon'])

    met_roll['vac'] = round(met_roll['avail'] / met_roll['inv'], 4)
    met_roll['vac_chg'] = np.where((met_roll['identity_met'] == met_roll['identity_met'].shift(1)), met_roll['vac'] - met_roll['vac'].shift(1), np.nan)
    

    met_roll['mrent'] = round(met_roll['askrevenue'] / met_roll['inv'],2)
    met_roll['merent'] = round(met_roll['effrevenue'] / met_roll['inv'],2)
    
    met_roll['Gmrent'] = np.where((met_roll['identity_met'] == met_roll['identity_met'].shift(1)), (met_roll['mrent'] - met_roll['mrent'].shift(1)) / met_roll['mrent'].shift(1), np.nan)
    met_roll['Gmerent'] = np.where((met_roll['identity_met'] == met_roll['identity_met'].shift(1)), (met_roll['merent'] - met_roll['merent'].shift(1)) / met_roll['merent'].shift(1), np.nan)

    met_roll['gap'] = (met_roll['mrent'] - met_roll['merent']) / met_roll['mrent']

    if sector_val != "ind":
        met_roll['sector'] = sector_val.title()
    
    if sector_val == "ind":
        met_roll['trend_yr1'] = np.where(met_roll['expansion'] == "Exp", 2017, 2009)

    if sector_val == "ret":
        met_roll = met_roll.drop_duplicates(['metcode', 'yr', 'currmon'])

    if sector_val == "apt" or sector_val == "off" or sector_val == "ret":
        met_roll["merent"] = np.where(met_roll['tier'] > 1, np.nan, met_roll["merent"])
        met_roll["Gmerent"] = np.where(met_roll['tier'] > 1, np.nan, met_roll["Gmerent"])
        met_roll["gap"] = np.where(met_roll['tier'] > 1, np.nan, met_roll["gap"])
        met_roll["final_Gmerent_last3mo"] = np.where(met_roll['tier'] > 1, np.nan, met_roll["final_Gmerent_last3mo"])
        met_roll["final_Gmerent_ytd"] = np.where(met_roll['tier'] > 1, np.nan, met_roll["final_Gmerent_ytd"])
        met_roll["final_gapchg_last3mo"] = np.where(met_roll['tier'] > 1, np.nan, met_roll["final_gapchg_last3mo"])

    if currmon == 1:
        met_roll['final_cons_ytd'] = met_roll['cons']
        met_roll['final_vacchg_ytd'] = met_roll['vac_chg']
        met_roll['final_abs_ytd'] = met_roll['abs']
        met_roll['final_Gmrent_ytd'] = met_roll['G_mrent']
        met_roll['final_Gmerent_ytd'] = met_roll['G_merent']
        

    if sector_val != "ind":
        cols_to_display = ['sector', 'subsector', 'tier', 'metcode', 'metro', 'state', 'region', 'yr', 'qtr', 'currmon', 'final_inv', 'sq_ids', 'sq_inv', 'final_cons', 
        'sq_cons', 'sq_ncabs', 'sq_nc_vac_coverpct', 'final_cons_last3mos',	'final_cons_ytd', 'final_conv',	'final_demo', 'final_vac', 'final_vac_chg',
        'final_avail', 'final_occ', 'final_abs', 'sq_avail', 'sq_vac', 'sq_occ', 'sq_abs', 'sq_survey_inv', 'sq_survey_abs', 'sq_cons_avail',
        'sq_cons_vac', 'sur_vacsurvey_coverpct', 'sur_vacchginv', 'sur_totabs', 'sur_vacchg_wgt', 'sur_vacchgs_avgmos_tolastsur', 'final_vacchg_last3mos', 
        'final_vacchg_ytd', 'final_abs_last3mos', 'final_abs_ytd', 'final_askrent', 'final_Gmrent', 'sq_askrent', 'sq_askrent_chg', 'cube_inv_rent10', 
        'cube_inv_rent10_diff', 'sur_rentsurvey_coverpct', 'sur_rntchginv', 'sur_rentchgs_total', 'sur_rentchgs_drop', 'sur_rentchgs_flat', 'sur_rentchgs_incr',
        'sur_rentchgs_avgdiff', 'sur_rentchgs_avgdiff_mo', 'sur_rentchgs_avgdiff_mo_wgt', 'sur_rentchgs_avgmos_tolastsur', 'sur_rentchgs_stddev', 
        'final_Gmrent_last3mos', 'final_Gmrent_ytd', 'rolling3mocube_rent00_inv', 'rolling3mocube_rent00_chg', 'sq_cons_askrent', 'sq_cons_rentpremium_tolagsq', 
        'final_effrent', 'final_Gmerent', 'final_gap', 'final_Gmerent_last3mo', 'final_Gmerent_ytd', 'final_gapchg_last3mo']
        if sector_val == "apt" or sector_val == "off":
            cols_to_display.remove('subsector')
        if sector_val == "ret":
            met_roll['subsector'] = "NC"

    else:
        cols_to_display = ['subsector', 'trend_yr1', 'metcode', 'metro', 'state', 'region', 'yr', 'qtr', 'currmon', 'final_inv', 'sq_ids', 'sq_inv', 'final_cons', 
        'sq_cons', 'sq_ncabs', 'sq_nc_vac_coverpct', 'final_cons_last3mos',	'final_cons_ytd', 'final_conv',	'final_demo', 'final_vac', 'final_vac_chg',
        'final_avail', 'final_occ', 'final_abs', 'sq_avail', 'sq_vac', 'sq_occ', 'sq_abs', 'sq_survey_inv', 'sq_survey_abs', 'sq_cons_avail',
        'sq_cons_vac', 'sur_vacsurvey_coverpct', 'sur_vacchginv', 'sur_totabs', 'sur_vacchg_wgt', 'sur_vacchgs_avgmos_tolastsur', 'final_vacchg_last3mos', 
        'final_vacchg_ytd', 'final_abs_last3mos', 'final_abs_ytd', 'final_askrent', 'final_Gmrent', 'sq_askrent', 'sq_askrent_chg', 'cube_inv_rent10', 
        'cube_inv_rent10_diff', 'sur_rentsurvey_coverpct', 'sur_rntchginv', 'sur_rentchgs_total', 'sur_rentchgs_drop', 'sur_rentchgs_flat', 'sur_rentchgs_incr',
        'sur_rentchgs_avgdiff', 'sur_rentchgs_avgdiff_mo', 'sur_rentchgs_avgdiff_mo_wgt', 'sur_rentchgs_avgmos_tolastsur', 'sur_rentchgs_stddev', 
        'final_Gmrent_last3mos', 'final_Gmrent_ytd', 'rolling3mocube_rent00_inv', 'rolling3mocube_rent00_chg', 'sq_cons_askrent', 'sq_cons_rentpremium_tolagsq', 
        'final_effrent', 'final_Gmerent', 'final_gap', 'final_Gmerent_last3mo', 'final_Gmerent_ytd', 'final_gapchg_last3mo']
    
    met_roll = met_roll.rename(columns={
                                        'conv': 'final_conv',
                                        'demo': 'final_demo',
                                        'vac_chg': 'final_vac_chg',
                                        'met_sur_totabs': 'sur_totabs',
                                        'vac': 'final_vac',
                                        'met_avg_rentchg_mo': 'sur_rentchgs_avgdiff_mo',
                                        'gap': 'final_gap',
                                        'met_rentdrops': 'sur_rentchgs_drop',
                                        'met_rentincrs': 'sur_rentchgs_incr',
                                        'met_rentflats': 'sur_rentchgs_flat',
                                        'occ': 'final_occ',
                                        'inv': 'final_inv',
                                        'cons': 'final_cons',
                                        'merent': 'final_effrent',
                                        'Gmerent': 'final_Gmerent',
                                        'abs': 'final_abs',
                                        'mrent': 'final_askrent',
                                        'Gmrent': 'final_Gmrent',
                                        'reg_long': 'region',
                                        'avail': 'final_avail',
                                        'met_avg_mos_to_last_vacsur': 'sur_vacchgs_avgmos_tolastsur', 
                                        'met_avg_rentchg': 'sur_rentchgs_avgdiff',
                                        'met_wtdvacchg': 'sur_vacchg_wgt',
                                        'met_avg_rentchg_mo': 'sur_rentchgs_avgdiff_mo',
                                        'met_vacchginv': 'sur_vacchginv',
                                        'met_rentchgs': 'sur_rentchgs_total',
                                        'met_stddev_avg_rentchg': 'sur_rentchgs_stddev',
                                        })

    met_roll = met_roll[cols_to_display]
    
    met_roll = met_roll[(met_roll['currmon'] == currmon)]

    if sector_val == "ind":
        met_roll.sort_values(by=['subsector', 'metcode', 'yr', 'currmon'], inplace=True)
    else:
        met_roll.sort_values(by=['metcode', 'yr', 'currmon'], inplace=True)

    met_roll['final_vac_chg'] = round(met_roll['final_vac_chg'], 4)
    met_roll['final_Gmrent'] = round(met_roll['final_Gmrent'], 4)
    met_roll['final_Gmerent'] = round(met_roll['final_Gmerent'], 4)
    met_roll['final_gap'] = round(met_roll['final_gap'], 4)

    return us_roll, met_roll

def check_skips(dataframe_in, decision_data, curryr, currmon, sector_val, flag_cols, init_drop_val):
    dataframe = dataframe_in.copy()
        
    rol_flag_cols = [x for x in flag_cols if "rol" in x or x == "v_flag_low" or x == "v_flag_high" or x == "e_flag_high" or x == "e_flag_low" or x == "c_flag_sqdiff"]
    test_cols = [x + "_test" for x in rol_flag_cols]
    dataframe[test_cols] = dataframe.groupby('identity')[rol_flag_cols].transform('sum')
    for x, y in zip(rol_flag_cols, test_cols):
        dataframe[x] = np.where((dataframe[y] > 0) & (dataframe[x] == 0) & (dataframe['yr'] == curryr) & (dataframe['currmon'] == currmon), 1, dataframe[x])
    
    dataframe[flag_cols]
    dataframe = dataframe[(dataframe['identity'] == init_drop_val) & (dataframe['curr_tag'] == 1)]
    skips = list(dataframe['flag_skip'])
    skips = skips[0].replace(' ', '').split(",")
    if len(dataframe) > 0:           
        flags_only = dataframe.copy()
        flags_only = flags_only[flag_cols]
        flags = dataframe.apply(lambda row: row[row != 0].index, axis=1).values[0]
        flags = [x for x in flags if x in flag_cols]
        remove_skips = [x for x in skips if x not in flags]
        if len(remove_skips) > 0:
            new_skips = [x for x in skips if x in flags]
            dataframe_in.loc[init_drop_val + str(curryr) + str(currmon), 'flag_skip'] = ''
            for x in skips:
                if x not in remove_skips:
                    if dataframe_in.loc[init_drop_val + str(curryr) + str(currmon), 'flag_skip'] == '':
                            dataframe_in.loc[init_drop_val + str(curryr) + str(currmon), 'flag_skip'] = x
                    else:
                        dataframe_in.loc[init_drop_val + str(curryr) + str(currmon), 'flag_skip'] += ", " + x
            
            decision_data.loc[init_drop_val + str(curryr) + str(currmon), 'skipped'] = ''
            for x in skips:
                if x not in remove_skips:
                    if decision_data.loc[init_drop_val + str(curryr) + str(currmon), 'skipped'] == '':
                            decision_data.loc[init_drop_val + str(curryr) + str(currmon), 'skipped'] = x
                    else:
                        decision_data.loc[init_drop_val + str(curryr) + str(currmon), 'skipped'] += ", " + x
            if len(remove_skips) == len(skips):
                decision_data.loc[init_drop_val + str(curryr) + str(currmon), 'skip_user'] = ''

    return dataframe_in, decision_data

def get_user_skips(skip_input_noprev, skip_input_resolved, skip_input_unresolved, skip_input_new, skip_input_skipped):

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

    return skip_list

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

def ncsur_tooltip(ncsur_props, sub_ncabsprops_keys, tables, underline_cols):

    for key in sub_ncabsprops_keys:
        if key == sub_ncabsprops_keys[0]:
            table_ncsur = textwrap.dedent(
                                            '''
                                            First Surv NC Prop Abs
                                            | id   |  YrBuilt                |  Abs    |
                                            |------|-------------------------|---------|
                                            | {id} | {yearbuilt}m{monthbuilt}| {abs:,} | 
                                            '''.format(
                                                id=ncsur_props[key]['id'],
                                                yearbuilt=ncsur_props[key]['yearx'],
                                                monthbuilt=ncsur_props[key]['month'],
                                                abs=ncsur_props[key]['nc_surabs']
                                            )
                                        )
        else:
            table_ncsur += textwrap.dedent(
                                            '''
                                            {id}     {yearbuilt}m{monthbuilt}     {abs:,}
                                            '''.format(
                                                id=ncsur_props[key]['id'],
                                                yearbuilt=ncsur_props[key]['yearx'],
                                                monthbuilt=ncsur_props[key]['month'],
                                                abs=ncsur_props[key]['nc_surabs']
                                                
                                            )
                                        )
    tables.append(table_ncsur)
    underline_cols += ['nc surabs']

    return tables, underline_cols

def avail10_tooltip(surv_avail_props, sub_availprops_keys, tables, underline_cols):
    for key in sub_availprops_keys:
        if key == sub_availprops_keys[0]:
            table_avail = textwrap.dedent(
                                            '''
                                            Top Props 10 Absorption
                                            | id   |  Absorption | 
                                            |------|-------------|
                                            | {id} | {abs:,}     | 
                                            '''.format(
                                                id=surv_avail_props[key]['id'],
                                                abs=surv_avail_props[key]['abs']
                                            )
                                        )
        else:
            table_avail += textwrap.dedent(
                                            '''
                                            {id}   {abs:,}
                                            '''.format(
                                                id=surv_avail_props[key]['id'],
                                                abs=surv_avail_props[key]['abs']
                                            )
                                        )
    tables.append(table_avail)
    underline_cols += ['avail10d']

    return tables, underline_cols

def all_avail_tooltip(all_avail_props, sub_all_availprops_keys, tables, underline_cols):

    for key in sub_all_availprops_keys:
        if key == sub_all_availprops_keys[0]:
            table_all_avail = textwrap.dedent(
                                            '''
                                            Top Props Absorption
                                            | id   |  Absorption |  AvailxM |
                                            |------|-------------|----------|
                                            | {id} | {abs:,}     | {xm}     |
                                            '''.format(
                                                id=all_avail_props[key]['id'],
                                                abs=all_avail_props[key]['abs'],
                                                xm=all_avail_props[key]['availxM'] 
                                            )
                                        )
        else:
            table_all_avail += textwrap.dedent(
                                            '''
                                            {id}   {abs:,}   {xm}
                                            '''.format(
                                                id=all_avail_props[key]['id'],
                                                abs=all_avail_props[key]['abs'],
                                                xm=all_avail_props[key]['availxM']

                                            )
                                        )
    tables.append(table_all_avail)
    underline_cols += ['ss vac chg']

    return tables, underline_cols

def ren10_tooltip(surv_rg_props, sub_rgprops_keys, tables, underline_cols):

    for key in sub_rgprops_keys:
        if key == sub_rgprops_keys[0]:
            table_surv_rg = textwrap.dedent(
                                            '''
                                            Top Props Rent 10 Chg
                                            | id   |  Rent Chg  |
                                            |------|------------|
                                            | {id} | {rg:.1%}   |
                                            '''.format(
                                                id=surv_rg_props[key]['id'],
                                                rg=surv_rg_props[key]['rg'],
                                            )
                                        )
        else:
            table_surv_rg += textwrap.dedent(
                                            '''
                                            {id}   {rg:.1%}
                                            '''.format(
                                                id=surv_rg_props[key]['id'],
                                                rg=surv_rg_props[key]['rg'],
                                            )
                                        )
    tables.append(table_surv_rg)
    underline_cols += ['dqren10d']

    return tables, underline_cols

def all_rent_tooltip(all_rg_props, sub_all_rgprops_keys, tables, underline_cols):
    for key in sub_all_rgprops_keys:
        if key == sub_all_rgprops_keys[0]:
            table_all_rent = textwrap.dedent(
                                            '''
                                            Top Props Rent Chg
                                            | id   |  Rent Chg |  RenxM   |
                                            |------|-----------|----------|
                                            | {id} | {rg:.1%}  | {xm}     |
                                            '''.format(
                                                id=all_rg_props[key]['id'],
                                                rg=all_rg_props[key]['rg'],
                                                xm=all_rg_props[key]['renxM'] 
                                            )
                                        )
        else:
            table_all_rent += textwrap.dedent(
                                            '''
                                            {id}   {rg:.1%}     {xm}
                                            '''.format(
                                                id=all_rg_props[key]['id'],
                                                rg=all_rg_props[key]['rg'],
                                                xm=all_rg_props[key]['renxM']

                                            )
                                        )

    tables.append(table_all_rent)
    underline_cols += ['ss rent chg']

    return tables, underline_cols

def ncbackfill_tooltip(newnc_props, sub_newncprops_keys, tables, underline_cols):
    for key in sub_newncprops_keys:
            if key == sub_newncprops_keys[0]:
                table_ncbackfill = textwrap.dedent(
                                                '''
                                                Top Props Absorption
                                                | id   |  Yr Built               |  Size   | Totavail        |  Rent      |
                                                |------|-------------------------|---------|-----------------|------------|
                                                | {id} | {yearbuilt}m{monthbuilt}|  {size:,} | {totavail:,}  | {renx:.3}  |    
                                                '''.format(
                                                    id=newnc_props[key]['id'],
                                                    yearbuilt=newnc_props[key]['yearx'],
                                                    monthbuilt=newnc_props[key]['month'],
                                                    size=newnc_props[key]['sizex'], 
                                                    totavail=newnc_props[key]['totavailx'], 
                                                    renx=float(newnc_props[key]['renx']),  
                                                )
                                            )
            else:
                table_ncbackfill += textwrap.dedent(
                                                '''
                                                {id} {yearbuilt}m{monthbuilt}  {size:,} {totavail:,} {renx:.3}  
                                                '''.format(
                                                    id=newnc_props[key]['id'],
                                                    yearbuilt=newnc_props[key]['yearx'],
                                                    monthbuilt=newnc_props[key]['month'],
                                                    size=newnc_props[key]['sizex'], 
                                                    totavail=newnc_props[key]['totavailx'], 
                                                    renx=float(newnc_props[key]['renx']), 

                                                )
                                            )
    tables.append(table_ncbackfill)
    underline_cols += ['newncsf']

    return tables, underline_cols