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

from init_load_trend import get_home
#from timer_trend import Timer

# Function that filters the dataframe for the columns to display on the data tab to the user, based on what type of flag is currently being analyzed
def set_display_cols(dataframe_in, identity_val, variable_fix, sector_val, curryr, currmon):
    dataframe = dataframe_in.copy()

    # Note: leave rol_vac and rol_G_mrent in so that it can be used to identify row where diff to rol is for highlighting purposes, will be dropped before final output of datatable
    if sector_val != "ind":
        display_cols = ['identity_row', 'inv shim', 'cons shim', 'conv shim', 'demo shim', 'avail shim', 'mrent shim', 'merent shim', 'yr', 'currmon', 'inv', 'cons', 'sqcons', 'conv', 'demo', 'vac', 'vac_chg', 'sqvac', 'sqvac_chg', 'occ', 'avail', 'sqavail', 'abs', 'sqabs', 'mrent', 'G_mrent', 'sqsren', 'sq_Gmrent', 'merent', 'G_merent', 'gap', 'gap_chg', 'rol_vac', 'rol_G_mrent']
    else:
        display_cols = ['identity_row', 'inv shim', 'cons shim', 'avail shim', 'mrent shim', 'merent shim', 'yr', 'currmon', 'inv', 'cons', 'sqcons', 'vac', 'vac_chg', 'sqvac', 'sqvac_chg', 'occ', 'avail', 'sqavail', 'abs', 'sqabs', 'mrent', 'G_mrent', 'sqsren', 'sq_Gmrent', 'merent', 'G_merent', 'gap', 'gap_chg', 'rol_vac', 'rol_G_mrent']
    
    if ('90' in identity_val and sector_val == "apt") or (sector_val == "off" and ('81' in identity_val or '82' in identity_val)) or (('70' in identity_val and sector_val == "ret")):
        display_cols.remove('merent')
        display_cols.remove('G_merent')
        display_cols.remove('gap')
        display_cols.remove('gap_chg')

    if variable_fix == "c":
        key_met_cols = ['newncsf', 'newncava', 'ncrenlev', 'newncrev', 'cons_roldiff', 'vac_roldiff', 'gmrent_roldiff']
    elif variable_fix == "v":
        key_met_cols = ['vac_chg_12', 'sqvac_chg_12', 'ss_vac_chg', 'vac_roldiff', 'newncava', 'vacdrops', 'vacflats', 'vacincrs', 'met_sur_totabs', 'met_sur_v_cov_perc', 'met_avg_mos_to_last_vacsur', 'avail10d', 'sub_sur_totabs', 'sub_sur_v_cov_perc', 'sub_avg_mos_to_last_vacsur']

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

    dataframe = dataframe[dataframe['identity'] == identity_val]
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
    if (sector_val == "apt" and identity_val[:-2] == '90') or (sector_val == "off" and (identity_val[:-2] == '81' or identity_val[:-2] == '82')) or (sector_val == "ret" and identity_val[:-2] == '70'):
        display_cols.remove('merent')
        display_cols.remove('G_merent')
        display_cols.remove('rol_G_merent')
        display_cols.remove('gap')
        display_cols.remove('gap_chg')
    dataframe = dataframe[display_cols]
    dataframe = dataframe.loc[identity_val]
    dataframe = dataframe.reset_index()
    dataframe = dataframe.set_index("identity_row")
    dataframe = dataframe.drop(['identity'], axis =1)
    for z in display_cols[1:]:
        dataframe[z] = dataframe[z].apply(lambda x: '' if pd.isnull(x) else x)
    
    return dataframe

# Function to determine what key metrics to display to user based on the type of flag                    
def gen_metrics(dataframe_in, identity_val, variable_fix, key_met_cols, curryr, currmon):
    
    dataframe_met = dataframe_in.copy()
    dataframe_met = dataframe_met.reset_index()
    dataframe_met = dataframe_met[(dataframe_met['identity'] == identity_val)]
    dataframe_met['cons_roldiff'] = dataframe_met['cons_roldiff'].fillna(method='ffill')
    dataframe_met['vac_roldiff'] = dataframe_met['vac_roldiff'].fillna(method='ffill')
    dataframe_met['gmrent_roldiff'] = dataframe_met['gmrent_roldiff'].fillna(method='ffill')
    dataframe_met = dataframe_met[(dataframe_met['yr'] == curryr) & (dataframe_met['currmon'] == currmon)]
    dataframe_met = dataframe_met.set_index("identity")
    
    dataframe_met = dataframe_met[key_met_cols]
    dataframe_met = pd.DataFrame(dataframe_met.loc[identity_val]).transpose()
    for z in key_met_cols:
        dataframe_met[z] = dataframe_met[z].apply(lambda x: '' if pd.isnull(x) else x)
    
    return dataframe_met

# Function that drops columns of the dataframe that will be recalculated each time a sub is examined for flags
def drop_cols(dataframe):
        cols = dataframe.columns
        for x in range(0, len(cols)):
            if cols[x] == "Recalc Insert":
                index = x
        drop_list = cols[index :]
        dataframe = dataframe.drop(columns = drop_list, axis = 1)
        
        return dataframe

# This function creates the rank table for key vars for subs within the met
#@Timer("Rank it")
def rank_it(rolled, data, roll_val, curryr, currmon, sector_val, values):
    frames = []
    
    for x in ['identity', 'identity_met']:
        if x == "identity":
            rank = data.copy()
            #rank = rank[rank['identity_met'] == roll_val]
            rank = rank[rank['curr_tag'] == 1]
            if rank.reset_index().loc[0]['subid'] == 90 or rank.reset_index().loc[0]['subid'] == 82 or rank.reset_index().loc[0]['subid'] == 81 or rank.reset_index().loc[0]['subid'] == 70:
                has_no_gap = True
            else:
                has_no_gap = False
        elif x == "identity_met":
            rank = rolled.copy()
            rank = rank[(rank['yr'] == curryr) & (rank['currmon'] == currmon)]
            if sector_val == "ind":
                if "DW" in roll_val:
                    rank = rank[rank['subsector'] == "DW"]
                else:
                    rank = rank[rank['subsector'] == "F"]
            rank = rank.rename(columns={'ask_chg': 'G_mrent'})
            rank['identity_met'] = rank['metcode'] + rank['subsector']

        if values == False:
        
            calc_cols = ['cons', 'vac_chg', 'abs', 'G_mrent', 'gap_chg']
            if has_no_gap == True:
                calc_cols.remove('gap_chg')
            rank_cols = []
            for y in calc_cols:
                col_name = y + "_rank"
                rank_cols.append(col_name)
                if "vac" in y or "gap" in y:
                    sort_order = True
                else:
                    sort_order = False
                rank[col_name] = rank[y].rank(ascending=sort_order, method='min')
                rank[col_name] = rank[col_name].astype(int)
        elif values == True:
            rank_cols = ['cons', 'vac_chg', 'abs', 'G_mrent', 'gap_chg']

        if x == "identity_met":
            rank = rank.drop_duplicates(['metcode'])    
        
        rank = rank.set_index(x)
        if x == "identity_met":
            rank = rank[['metcode'] + rank_cols]
        else:
            rank = rank[['metcode', 'subid'] + rank_cols]
        
        for z in list(rank.columns): 
            if "_rank" in z:
                rank = rank.rename(columns={z: z[:-5]})
        
        if x == "identity_met":
            rank = rank.reset_index()
            test = rank.index[rank['identity_met'] == roll_val].tolist()
            rank = pd.concat([rank.iloc[[test[0]],:], rank.drop(test[0], axis=0)], axis=0)
            rank = rank.set_index('identity_met')
       
        frames.append(rank)

    return frames[0], frames[1]


    
# This function will roll up the data on a metro or national level for review based on the selection of metro by the user on the Rollups tab
#@Timer("Rollup")    
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

    roll['rol_avail'] = roll['inv'] * roll['rol_vac']
    if sector_val == "apt":
        roll['rol_avail'] = round(roll['rol_avail'], 0)
    else:
        roll['rol_avail'] = round(roll['rol_avail'], -3)

    roll['rolaskrevenue'] = roll['rol_inv'] * roll['rol_mrent']
    roll['roleffrevenue'] = roll['rol_inv'] * roll['rol_merent']

    roll = roll[(roll['yr'] >= curryr - 4)]

    cols_to_roll = ['inv', 'rol_inv', 'inv_oob', 'cons', 'rol_cons', 'cons_oob', 'avail', 'avail_oob', 'rol_avail', 'occ', 'abs', 'rol_abs', 'askrevenue', 'effrevenue', 'rolaskrevenue', 'roleffrevenue', 'oobaskrevenue']
    
    roll[cols_to_roll] = roll.groupby([identity_filt, 'yr', 'currmon'])[cols_to_roll].transform('sum')

    if filt_type == "reg":
        roll = roll.drop_duplicates([identity_filt, 'yr', 'currmon'])

    roll['vac'] = round(roll['avail'] / roll['inv'], 4)
    roll['vac_oob'] = round(roll['avail_oob'] / roll['inv_oob'], 4)
    roll['vac_chg'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)), roll['vac'] - roll['vac'].shift(1), np.nan)
    roll['rol_vac'] = round(roll['rol_avail'] / roll['rol_inv'], 4)
    roll['rol_vac_chg'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)), roll['rol_vac'] - roll['rol_vac'].shift(1), np.nan)
    roll['vac_chg_oob'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)), roll['vac_oob'] - roll['vac_oob'].shift(1), np.nan)
    
    roll['mrent'] = round(roll['askrevenue'] / roll['inv'],2)
    roll['merent'] = round(roll['effrevenue'] / roll['inv'],2)
    roll['rol_mrent'] = round(roll['rolaskrevenue'] / roll['rol_inv'],2)
    roll['mrent_oob'] = round(roll['oobaskrevenue'] / roll['inv'],2)
    roll['rol_merent'] = round(roll['roleffrevenue'] / roll['rol_inv'],2)
    
    roll['G_mrent'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)), (roll['mrent'] - roll['mrent'].shift(1)) / roll['mrent'].shift(1), np.nan)
    roll['G_merent'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)), (roll['merent'] - roll['merent'].shift(1)) / roll['merent'].shift(1), np.nan)
    roll['rol_G_mrent'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)), (roll['rol_mrent'] - roll['rol_mrent'].shift(1)) / roll['rol_mrent'].shift(1), np.nan)
    roll['G_mrent_oob'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)), (roll['mrent_oob'] - roll['mrent_oob'].shift(1)) / roll['mrent_oob'].shift(1), np.nan)
    roll['rol_G_merent'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)), (roll['rol_merent'] - roll['rol_merent'].shift(1)) / roll['rol_merent'].shift(1), np.nan)

    roll['gap'] = ((roll['merent'] - roll['mrent']) / roll['mrent']) * -1
    roll['rol_gap'] = ((roll['rol_merent'] - roll['rol_mrent']) / roll['rol_mrent']) * -1
    roll['gap_chg'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)), roll['gap'] - roll['gap'].shift(1), np.nan)
    roll['rol_gap_chg'] = np.where((roll[identity_filt] == roll[identity_filt].shift(1)), roll['rol_gap'] - roll['rol_gap'].shift(1), np.nan)

    roll['cons'] = roll['cons'].astype(int)
    roll['rol_cons'] = roll['rol_cons'].astype(int)
    roll['cons_oob'] = roll['cons_oob'].astype(int)
    roll['abs'] = roll['abs'].astype(int)
    roll['rol_abs'] = roll['rol_abs'].astype(int)

    roll['rol_cons'] = np.where((roll['curr_tag'] == 1), np.nan, roll['rol_cons'])
    roll['rol_abs'] = np.where((roll['curr_tag'] == 1), np.nan, roll['rol_abs'])
    roll['rol_vac'] = np.where((roll['curr_tag'] == 1), np.nan, roll['rol_vac'])
    roll['rol_vac_chg'] = np.where((roll['curr_tag'] == 1), np.nan, roll['rol_vac_chg'])
    roll['rol_abs'] = np.where((roll['curr_tag'] == 1), np.nan, roll['rol_abs'])
    roll['rol_G_mrent'] = np.where((roll['curr_tag'] == 1), np.nan, roll['rol_G_mrent'])
    roll['rol_G_merent'] = np.where((roll['curr_tag'] == 1), np.nan, roll['rol_G_merent'])
         
    if filt_type == "reg":
        cols_to_display = ['identity_us', 'subsector', 'metcode', 'yr', 'currmon', 'inv', 'metsqinv', 'cons', 'rol_cons', 'metsqcons', 'vac', 'vac_chg', 'rol_vac', 'rol_vac_chg', 'metsqvacchg', 'abs', 'rol_abs', 'metsqabs', 'mrent', 'G_mrent', 'rol_G_mrent', 'metsqsren', 'metsq_Gmrent', 'merent', 'G_merent', 'rol_G_merent', 'gap', 'gap_chg', 'rol_mrent', 'rol_inv', 'rol_gap', 'rol_gap_chg']
        if 'met_sur_totabs' in list(roll.columns):
            cols_to_display += ['met_sur_totabs', 'met_g_renx_mo_wgt']
    else:
        cols_to_display = ['subsector', 'metcode', 'subid', 'yr', 'currmon', 'inv', 'sqinv', 'cons', 'sqcons', 'vac', 'vac_chg', 'sqvac', 'sqvac_chg', 'abs', 'sqabs', 'mrent', 'G_mrent', 'sqsren', 'sq_Gmrent', 'merent', 'G_merent', 'gap', 'gap_chg']
    cols_to_display += ['cons_oob', 'vac_oob', 'vac_chg_oob', 'mrent_oob', 'G_mrent_oob']
    if drop_val[:2] == "US":
        cols_to_display.remove('metcode')
    
    roll = roll[cols_to_display]
    roll = roll[(roll['yr'] >= curryr - 3)]

    return roll

def live_flag_count(dataframe, sector_val, flag_cols): 

    rol_flag_cols = [x for x in flag_cols if "rol" in x]
    test_flag_cols = [x + "_test" for x in rol_flag_cols]
    dataframe[test_flag_cols] = dataframe.groupby('identity')[rol_flag_cols].transform('sum')
    for x, y in zip(rol_flag_cols, test_flag_cols):
        dataframe[x] = np.where(dataframe[x] == 1, dataframe[x] / dataframe[y], dataframe[x])
    dataframe.drop(test_flag_cols, axis=1, inplace=True)
    
    dataframe['c_flag_tot_sub'] = dataframe.filter(regex="^c_flag*").sum(axis=1)
    dataframe['v_flag_tot_sub'] = dataframe.filter(regex="^v_flag*").sum(axis=1)
    dataframe['g_flag_tot_temp1'] = dataframe.filter(regex="^g_flag*").sum(axis=1)
    dataframe['g_flag_tot_temp2'] = dataframe.filter(regex="^e_flag*").sum(axis=1)
    dataframe['g_flag_tot_sub'] = dataframe['g_flag_tot_temp1'] + dataframe['g_flag_tot_temp2']
    dataframe = dataframe.drop(['g_flag_tot_temp1', 'g_flag_tot_temp2'], axis=1)

    c_left = dataframe['c_flag_tot_sub'].sum() -  dataframe['flag_skip'].str.count('c_flag').sum()
    v_left = dataframe['v_flag_tot_sub'].sum() - dataframe['flag_skip'].str.count('v_flag').sum()
    g_left = dataframe['g_flag_tot_sub'].sum() - dataframe['flag_skip'].str.count('g_flag').sum() - dataframe['flag_skip'].str.count('e_flag').sum()


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
def get_issue(dataframe, has_flag, flag_list, flags_resolved, flags_unresolved, flags_new, flags_skipped, curryr, currmon, preview_status, type_return, sector_val):

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
        "c_flag_rolv": [['vac'], ['vac roldiff', 'cons roldiff']], 
        "c_flag_rolg": [['Gmrent'], ['gmrent roldiff']],
        "c_flag_sqdiff": [['cons', 'sq cons'], []],
        "v_flag_low": [['vac', 'sq vac'], []],
        "v_flag_high": [['vac', 'sq vac'], []],
        "v_flag_rol": [['vac'], ['vac roldiff']],
        "v_flag_sqlev": [['vac', 'vac chg', 'sq vac', 'sq vac chg'], ['vac chg 12', 'sqvac chg 12']],
        "v_flag_sqabs": [['abs', 'sq abs'], []],
        "v_flag_surabs": [['abs'], ['sub sur totabs']],
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

    if type_return == "specific":
        if has_flag == 0:
            issue_description_noprev = "You have cleared all the flags"
            display_issue_cols = []
            key_metric_issue_cols = []
            issue_description_resolved = []
            issue_description_unresolved = []
            issue_description_new = []
            issue_description_skipped = []
        elif has_flag == 2:
            issue_description_noprev = "No flags for this submarket"
            display_issue_cols = []
            key_metric_issue_cols = []
            issue_description_resolved = []
            issue_description_unresolved = []
            issue_description_new = []
            issue_description_skipped = []
        elif has_flag == 1:
            if preview_status == 0:
                issue_description_resolved = []
                issue_description_unresolved = []
                issue_description_new = []
                issue_description_skipped = []
                issue_description_noprev = html.Div([
                                        html.Div([
                                            dbc.Container(
                                            [
                                                dbc.Checklist(
                                                    id="flag_descriptions_noprev",
                                                    options=[
                                                            {"label": f" {i[0]} {i[6:]}", "value": f"skip-{i}", "label_id": f"label-{i}"}
                                                            for i in flag_list
                                                            ],
                                                    inline=True
                                                            ),  
                                                    
                                            ]
                                            + [
                                                dbc.Tooltip(issue_descriptions[i], target=f"label-{i}")
                                                for i in flag_list
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

                if len(flags_skipped) > 0:
                    issue_description_skipped = html.Div([
                                            html.Div([
                                                dbc.Container(
                                                [
                                                    dbc.Checklist(
                                                        id="flag_descriptions_skipped",
                                                        options=[
                                                                {"label": f" {i[0]} {i[6:]}", "value": f"skip-{i}", "label_id": f"label-{i}"}
                                                                for i in flags_skipped
                                                                ],
                                                        value=[f"skip-{i}" for i in flags_skipped],
                                                        inline=True,
                                                        labelStyle={'display': 'inline-block', 'margin': '0 10px 0 10px', 'color': 'black'},
                                                                ),  
                                                        
                                                ]
                                                + [
                                                    dbc.Tooltip(issue_descriptions[i], target=f"label-{i}")
                                                    for i in flags_skipped
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

# Function that analyzes where edits are made in the display dataframe if manual edit option is selected
def get_diffs(shim_data, data_orig, data, drop_val, curryr, currmon, sector_val, button, subsequent_chg):
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
        check_avail = check_avail[(check_avail['avail'].isnull() == False) & ((check_avail['inv'].isnull() == False) | (check_avail['cons'].isnull() == False) | (check_avail['conv'].isnull() == False) | (check_avail['demo'].isnull() == False))]
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
                if math.isnan(row[col_name]) == True:
                    break
                else:
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
                
                data = insert_fix(data, row_to_fix_diffs, drop_val, fix_val, col_issue_diffs[0], curryr, currmon, sector_val, subsequent_chg)

        has_diff = 1
    else:
        has_diff = 0

    return data, has_diff

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
        elif variable_fix == "d":
            dataframe.loc[index_row, 'demo'] = fix
        dataframe['inv'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] >= index_row), dataframe['inv'] + cv_diff, dataframe['inv'])
        dataframe['occ'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] >= index_row), dataframe['occ'] + cv_diff, dataframe['occ'])
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
            dataframe['mrent'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] > index_row), dataframe['mrent'].shift(1) * (1 + dataframe['G_mrent']), dataframe['mrent'])
        elif subsequent_chg == "s":
            dataframe['mrent'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] > index_row) & (dataframe['identity'] == dataframe['identity'].shift(-1)), dataframe['mrent'].shift(1) * (1 + dataframe['sq_Gmrent']), dataframe['mrent'])
            dataframe['mrent'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] > index_row) & (dataframe['identity'] != dataframe['identity'].shift(-1)), dataframe['mrent'].shift(1) * (1 + dataframe['G_mrent']), dataframe['mrent'])
        dataframe['mrent'] = round(dataframe['mrent'], 2)
        dataframe['merent'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] >= index_row) & (np.isnan(dataframe['merent']) == False), dataframe['mrent'] - (dataframe['gap'] * dataframe['mrent']), dataframe['merent'])
        dataframe['merent'] = round(dataframe['merent'], 2)
        dataframe['G_merent'] = np.where((dataframe['identity'] == identity_val) & (dataframe['identity'] == dataframe['identity'].shift(1)) & (dataframe['index_row'] >= index_row) & (np.isnan(dataframe['G_merent']) == False), (dataframe['merent'] - dataframe['merent'].shift(1)) / dataframe['merent'].shift(1), dataframe['G_merent'])
    elif variable_fix == "e":
        dataframe.loc[index_row, 'merent'] = fix
        dataframe['G_merent'] = np.where((dataframe['identity'] == identity_val) & (dataframe['identity'] == dataframe['identity'].shift(1)) & (dataframe['index_row'] == index_row), (dataframe['merent'] - dataframe['merent'].shift(1)) / dataframe['merent'].shift(1), dataframe['G_merent'])
        dataframe['merent'] = np.where((dataframe['identity'] == identity_val) & (dataframe['index_row'] > index_row), dataframe['merent'].shift(1) * (1 + dataframe['G_merent']), dataframe['merent'])
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

import numpy as np
import pandas as pd
import re
from IPython.core.display import display, HTML

# Function to identify if a submarket has a flag for review
def flag_examine(data, identity_val, filt, curryr, currmon, flag_cols):
    dataframe = data.copy()
    has_flag = 0
    if filt == True:
        dataframe = dataframe[dataframe['identity'] == identity_val]

    cols_to_keep = flag_cols + ['identity', 'flag_skip']
    dataframe = dataframe[cols_to_keep]
    dataframe[flag_cols] = np.where((dataframe[flag_cols] > 0), 1, dataframe[flag_cols])
    rol_flag_cols = [x for x in flag_cols if "rol" in x]
    for x in rol_flag_cols:
        dataframe[x + "_test"] = dataframe.groupby('identity')[x].transform('sum')
        dataframe[x] = np.where(dataframe[x] == 1, dataframe[x] / dataframe[x + "_test"], dataframe[x])
    dataframe['sum_flags'] = dataframe[flag_cols].sum(axis=1)
    dataframe['sum_commas'] = dataframe['flag_skip'].str.count(',')
    dataframe['sum_skips'] = np.where((dataframe['flag_skip'] == ''), 0, np.nan)
    dataframe['sum_skips'] = np.where((dataframe['flag_skip'] != ''), dataframe['sum_commas'] + 1, dataframe['sum_skips'])
    dataframe['total_flags'] = dataframe.groupby('identity')['sum_flags'].transform('sum')
    dataframe['total_skips'] = dataframe.groupby('identity')['sum_skips'].transform('sum')
    dataframe['flags_left'] = dataframe['total_flags'] - dataframe['total_skips']
    dataframe = dataframe[dataframe['flags_left'] > 0]

    if len(dataframe) == 0:
        if filt == True:
            identity_val = identity_val
            has_flag = 2
            flag_list = ['v_flag']
        else: 
            identity_val = False
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
                identity_val = identity_val
                has_flag = 2
            else: 
                flag_list = ['v_flag']
                identity_val = False
        

    return flag_list, identity_val, has_flag

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