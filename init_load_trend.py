from pathlib import Path
import pandas as pd
import numpy as np
from IPython.core.display import display, HTML
import sys
import os
from os import listdir
from os.path import isfile, join
from datetime import datetime
import re
import time
import multiprocessing as mp
import gc

from trend_process_sq_insight import process_sq_insight


def load_msqs(sector_val, file_path):
    cols_to_keep = ['id', 'yr', 'qtr', 'currmon', 'yearx', 'month', 'metcode', 'subid', 'submkt', 'availxM', 'existsx']
    if sector_val == "apt":
        msq = pd.read_stata(file_path, columns= cols_to_keep + ['availx', 'totunitx', 'avgrenx', 'avgrenxM'])
    elif sector_val == "ind":
        msq = pd.read_stata(file_path, columns= cols_to_keep + ['type2', 'totavailx', 'ind_size', 'renx', 'renxM'])
    elif sector_val == "off":
        msq = pd.read_stata(file_path, columns=cols_to_keep + ['type2', 'totavailx', 'sizex', 'renx', 'renxM'])
    elif sector_val == "ret":
        msq = pd.read_stata(file_path, columns=cols_to_keep + ['type1', 'availx', 'sizex', 'nsizex', 'renx', 'renxM', 'nrenx', 'nrenxM'])
    # Keep only relevant periods and rows that the property was in existance for
    msq = msq[((msq['yr'] > 2008) | ((msq['yr'] == 2008) & (msq['qtr'] == 4)))]
    msq = msq[msq['existsx'] == 1]
    msq = msq.drop(['existsx'], axis=1)

    return msq

def get_home():
    if os.name == "nt": return "//odin/reisadmin/"
    else: return "/home/"

def load_init_input(sector_val, curryr, currmon):
    if sector_val == "ind":
        frames = []
        for subsector in ["DW", "F"]:
            file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/InputFiles/indsub_{}_{}m{}-ysis.csv".format(get_home(), sector_val, str(curryr), str(currmon), subsector, str(curryr), str(currmon)))
            file_path = Path(file_path)
            data_in = pd.read_csv(file_path, encoding = 'utf-8',  na_values= "", keep_default_na = False)
            frames.append(data_in)
        data_in = frames[0].append(frames[1], ignore_index=True)
    
    elif sector_val == "apt" or sector_val == "off":
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/InputFiles/{}sub_{}m{}-ysis.csv".format(get_home(), sector_val, str(curryr), str(currmon), sector_val, str(curryr), str(currmon)))
        data_in = pd.read_csv(file_path, encoding = 'utf-8',  na_values= "", keep_default_na = False)
        cols = list(data_in.columns)
        data_in['subsector'] = sector_val.title()
        new_cols = ['subsector'] + cols
        data_in = data_in[new_cols]

    elif sector_val == "ret":
        frames = []
        for subsector in ['C', 'N', 'NC']:
            if subsector == "C" or subsector == "N":
                file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/InputFiles/retsub_{}_{}m{}-ysis.csv".format(get_home(), sector_val, str(curryr), str(currmon), subsector, str(curryr), str(currmon)))
            else:
                file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/InputFiles/retsub_{}_{}m{}-tier3-ysis.csv".format(get_home(), sector_val, str(curryr), str(currmon), subsector, str(curryr), str(currmon)))
            data_in = pd.read_csv(file_path, encoding = 'utf-8',  na_values= "", keep_default_na = False)
            frames.append(data_in)
            
        data_in = frames[0].append(frames[1], ignore_index=True)
        data_in = data_in.append(frames[2], ignore_index=True)

    return data_in

def refresh_data(sector_val, curryr, currmon, data_in, data_refresh_in):

    data = data_in.copy()
    data_refresh = data_refresh_in.copy()

    data_refresh['subid'] = data_refresh['subid'].astype(int)
    data_refresh['yr'] = data_refresh['yr'].astype(int)
    data_refresh['currmon'] = np.where(data_refresh['currmon'].isnull() == True, 13, data_refresh['currmon'])
    data_refresh['currmon'] = data_refresh['currmon'].astype(int)
    data['subid'] = data['subid'].astype(int)
    data['yr'] = data['yr'].astype(int)
    data['currmon'] = data['currmon'].astype(int)

    data_refresh = data_refresh[((data_refresh['yr'] > 2008) | ((data_refresh['yr'] == 2008) & (data_refresh['qtr'] == 4)))]

    if sector_val == "apt":
        data_refresh = data_refresh.rename(columns={'sub1to99_Gavgrenx': 'sub1to99_Grenx'})
    if sector_val == "ret":
        data_refresh = data_refresh.rename(columns={'sub1to99_Gnrenx': 'sub1to99_Grenx'})

    if sector_val == "ind" or sector_val == "ret":
        data_refresh['join_ident'] = data_refresh['metcode'] + data_refresh['subid'].astype(str) + data_refresh['subsector'] + data_refresh['yr'].astype(str) + data_refresh['qtr'].astype(str) + data_refresh['currmon'].astype(str)
        data['join_ident'] = data['metcode'] + data['subid'].astype(str) + data['subsector'] + data['yr'].astype(str) + data['qtr'].astype(str) + data['currmon'].astype(str)
    else:
        data_refresh['join_ident'] = data_refresh['metcode'] + data_refresh['subid'].astype(str) + data_refresh['yr'].astype(str) + data_refresh['qtr'].astype(str) + data_refresh['currmon'].astype(str)
        data['join_ident'] = data['metcode'] + data['subid'].astype(str) + data['subsector'] + data['yr'].astype(str) + data['qtr'].astype(str) + data['currmon'].astype(str)

    survey_cols = ['avail10d', 'avail00d', 'dqinvren10', 'dqren10d', 'dqren00d', 'ncrenlev', 'covvac', 'covren', 'ss_vac_chg', 'ss_rent_chg', 'pastsubsqvac',
                    'pastsubsqrent', 'sub1to99_Grenx', 'newnc_thismo', 'newncsf', 'newncava', 'newncrev', 'subsq_props', 'rentchgs', 
                    'cons_roldiff', 'vac_roldiff', 'gmrent_roldiff']
    data = data.drop(survey_cols, axis=1)
    data = data.join(data_refresh.set_index('join_ident')[survey_cols], on='join_ident')

    if sector_val != "apt":
        data[['avail10d', 'avail00d']] = round((data[['avail10d', 'avail00d']] * -1), -3)
    else:
        data[['avail10d', 'avail00d']] = round((data[['avail10d', 'avail00d']] * -1), 0)

    data['avail10d_temp'] = np.where((data['avail10d'].isnull() == True), 0, data['avail10d'])
    data['avail00d_temp'] = np.where((data['avail00d'].isnull() == True), 0, data['avail00d'])
    data['avail0d'] = np.where((data['avail10d'].isnull() == False) | (data['avail00d'].isnull() == False), data['avail10d_temp'] + data['avail00d_temp'], np.nan)
    data = data.drop(['avail10d_temp', 'avail00d_temp'],axis=1)

    prelim_cols = ['p_inv', 'p_cons', 'p_avail', 'p_occ', 'p_abs', 'p_mrent', 'p_G_mrent', 'p_merent', 'p_G_merent', 'p_gap'] 
    data = data.join(data_refresh.set_index('join_ident')[prelim_cols], on='join_ident')
    has_diff = False
    diff_cols = []
    refresh_list = []
    for col in prelim_cols:
        diff_cols.append(col + "_diff")
        diff_cols.append(col + "_has_diff")
        data[col + "_diff"] = data[col] - data[col[2:] + "_oob"]
        data[col + "_has_diff"] = np.where((abs(data[col + "_diff"]) >= 0.0005) & (data[col].isnull() == False), 1, 0)
        testing = data.copy()
        testing = testing[testing[col + "_has_diff"] == 1]
        if len(testing) > 0:
            has_diff = True
            if sector_val == "ind" or sector_val == "ret":
                testing['identity'] = testing['metcode'] + testing['subid'].astype(str) + testing['subsector']
            else:
                testing['identity'] = testing['metcode'] + testing['subid'].astype(str)
            sub_list = testing['identity'].unique()
            sub_add = [x for x in sub_list if x not in refresh_list]
            refresh_list += sub_add
        data[col[2:]] = np.where(data[col + "_has_diff"] == 1, data[col], data[col[2:]])
    if len(refresh_list) > 0: 
        refresh_list.sort()
    
    if has_diff == True:
        data['vac'] = data['avail'] / data['inv']
        data['vac'] = round(data['vac'], 4)
        data['vac_chg'] = np.where((data['metcode'] == data['metcode'].shift(1)) & (data['subid'] == data['subid'].shift(1)) & (data['subsector'] == data['subsector'].shift(1)), data['vac'] - data['vac'].shift(1), np.nan)
    
        for x in prelim_cols:
            data[x[2:] + "_oob"] = np.where(data[x + "_has_diff"] == 1, data[x], data[x[2:] + "_oob"])
        data['vac_oob'] = np.where(data["p_avail_has_diff"] == 1, data['vac'], data['vac_oob'])
        data['vac_chg_oob'] = np.where(data["p_avail_has_diff"] == 1, data['vac_chg'], data['vac_chg_oob'])
        
        data[['inv_cons_comment', 'avail_comment', 'mrent_comment', 'erent_comment']] = data[['inv_cons_comment', 'avail_comment', 'mrent_comment', 'erent_comment']].fillna("")

    data = data.drop(['join_ident'], axis=1)
    data = data.drop(diff_cols, axis=1)
    data = data.drop(prelim_cols, axis=1)

    if has_diff == True:
        decision_data = pd.read_pickle(Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/OutputFiles/decision_log_{}.pickle".format(get_home(), sector_val, str(curryr), str(currmon), sector_val)))

        decision_data = decision_data.join(data_refresh.set_index('join_ident')[prelim_cols])

        diff_cols = []
        for col in prelim_cols:
            diff_cols.append(col + "_diff")
            diff_cols.append(col + "_has_diff")
            decision_data[col + "_diff"] = decision_data[col] - decision_data[col[2:] + "_oob"]
            decision_data[col + "_has_diff"] = np.where((abs(decision_data[col + "_diff"]) > 0.001) & (decision_data[col].isnull() == False), 1, 0)
            decision_data[col[2:] + "_oob"] = np.where(decision_data[col + "_has_diff"] == 1, decision_data[col], decision_data[col[2:] + "_oob"])
            if col != "p_occ":
                decision_data[col[2:] + "_new"] = np.where(decision_data[col + "_has_diff"] == 1, np.nan, decision_data[col[2:] + "_new"])
        
        for x, y in zip(['i_user', 'c_user', 'v_user', 'g_user', 'e_user'], ['inv_new', 'cons_new', 'avail_new', 'mrent_new', 'merent_new']):
            decision_data[x] = np.where((decision_data[x].isnull() == False) & (decision_data[y].isnull() == True), np.nan, decision_data[x])

        decision_data['cons_diff'] = decision_data['cons_oob'] - decision_data['rol_cons']
        decision_data['vac_diff'] = decision_data['vac_oob'] - decision_data['rol_vac']
        decision_data['mrent_diff'] = (decision_data['mrent_oob'] - decision_data['rol_mrent']) / decision_data['rol_mrent']
        decision_data['merent_diff'] = (decision_data['merent_oob'] - decision_data['rol_merent']) / decision_data['rol_merent']
        
        decision_data['inv_new'] = np.where((abs(decision_data['cons_diff']) > 0), decision_data['inv_oob'], decision_data['inv_new'])
        decision_data['cons_new'] = np.where((abs(decision_data['cons_diff']) > 0), decision_data['cons_oob'], decision_data['cons_new'])
        
        decision_data['vac_new'] = np.where((abs(decision_data['vac_diff']) >= 0.001), decision_data['vac_oob'], decision_data['vac_new'])
        decision_data['vac_chg_new'] = np.where((abs(decision_data['vac_diff']) >= 0.001), decision_data['vac_chg_oob'], decision_data['vac_chg_new'])
        decision_data['avail_new'] = np.where((abs(decision_data['vac_diff']) >= 0.001), decision_data['avail_oob'], decision_data['avail_new'])
        decision_data['abs_new'] = np.where((abs(decision_data['vac_diff']) >= 0.001), decision_data['abs_oob'], decision_data['abs_new'])
        
        decision_data['mrent_new'] = np.where((abs(decision_data['mrent_diff']) >= 0.001), decision_data['mrent_oob'], decision_data['mrent_new'])
        decision_data['G_mrent_new'] = np.where((abs(decision_data['mrent_diff']) >= 0.001), decision_data['G_mrent_oob'], decision_data['G_mrent_new'])
        
        decision_data['merent_new'] = np.where((abs(decision_data['merent_diff']) >= 0.001), decision_data['merent_oob'], decision_data['merent_new'])
        decision_data['G_merent_new'] = np.where((abs(decision_data['merent_diff']) >= 0.001), decision_data['G_merent_oob'], decision_data['G_merent_new'])

        decision_data['gap_new'] = np.where((abs(decision_data['mrent_diff']) > 0.001) | (abs(decision_data['merent_diff'] > 0.001)), decision_data['gap_oob'], decision_data['gap_new'])
        
        decision_data['i_user'] = np.where((abs(decision_data['cons_diff']) > 0) & (decision_data['i_user'].isnull() == True), "Cons Auto Rebench", decision_data['i_user'])
        decision_data['c_user'] = np.where((abs(decision_data['cons_diff']) > 0) & (decision_data['c_user'].isnull() == True), "Cons Auto Rebench", decision_data['c_user'])
        decision_data['v_user'] = np.where((abs(decision_data['vac_diff']) >= 0.001) & (decision_data['v_user'].isnull() == True), "Cons Auto Rebench", decision_data['v_user'])
        decision_data['g_user'] = np.where((abs(decision_data['mrent_diff']) >= 0.001) & (decision_data['g_user'].isnull() == True), "Cons Auto Rebench", decision_data['g_user'])
        decision_data['e_user'] = np.where((abs(decision_data['mrent_diff']) >= 0.001) & (decision_data['e_user'].isnull() == True), "Cons Auto Rebench", decision_data['e_user'])
        decision_data['i_user'] = np.where((abs(decision_data['cons_diff']) > 0) & (decision_data['i_user'].isnull() == False) & (decision_data['i_user'].str.contains('Cons Auto Rebench') == False), "Cons Auto Rebench, " + decision_data['i_user'], decision_data['i_user'])
        decision_data['c_user'] = np.where((abs(decision_data['cons_diff']) > 0) & (decision_data['c_user'].isnull() == False) & (decision_data['c_user'].str.contains('Cons Auto Rebench') == False), "Cons Auto Rebench, " + decision_data['c_user'], decision_data['c_user'])
        decision_data['v_user'] = np.where((abs(decision_data['vac_diff']) >= 0.001) & (decision_data['v_user'].isnull() == False) & (decision_data['v_user'].str.contains('Cons Auto Rebench') == False), "Cons Auto Rebench, " + decision_data['v_user'], decision_data['v_user'])
        decision_data['g_user'] = np.where((abs(decision_data['mrent_diff']) >= 0.001) & (decision_data['g_user'].isnull() == False) & (decision_data['g_user'].str.contains('Cons Auto Rebench') == False), "Cons Auto Rebench, " + decision_data['g_user'], decision_data['g_user'])
        decision_data['e_user'] = np.where((abs(decision_data['mrent_diff']) >= 0.001) & (decision_data['e_user'].isnull() == False) & (decision_data['e_user'].str.contains('Cons Auto Rebench') == False), "Cons Auto Rebench, " + decision_data['e_user'], decision_data['e_user'])


        decision_data = decision_data.drop(['cons_diff', 'vac_diff', 'mrent_diff', 'merent_diff'], axis=1)
        decision_data = decision_data.drop(diff_cols, axis=1)
        decision_data = decision_data.drop(prelim_cols, axis=1)
    else:
        decision_data = pd.DataFrame()

    return data, refresh_list, decision_data

def initial_load(sector_val, curryr, currmon, msq_load):
    
    # Load the input file - if this is the first time the program is run, the oob data should be loaded in, and if this is not the first time, then the edits data should be loaded in
    # If the msqs have been refreshed, we will also load the oob file, as there may be new prelim values that should be used and patched in 
    
    file_path = "{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/OutputFiles/{}_mostrecentsave.pickle".format(get_home(), sector_val, str(curryr), str(currmon), sector_val)
    isFile = os.path.isfile(file_path)
    if isFile == True: 
        data = pd.read_pickle(file_path)
        file_used = "edits"
        print("Using Saved File")
    else:
        data = load_init_input(sector_val, curryr, currmon)
        file_used = "oob"
        print("Initial Load")
    
    # If the MSQs were refreshed, replace the original oob figures for the key vars where there is a diff now due to the msq changes
    # Also replace the survey data that is generated by sqinsight and the two trend STATA programs, as those values may also now be different due to changes at the MSQs
    if file_used == "edits" and msq_load == "Y":
        data_refresh = load_init_input(sector_val, curryr, currmon)
        data_temp, refresh_list, decision_data_temp = refresh_data(sector_val, curryr, currmon, data, data_refresh)
    else:
        refresh_list = []
        data_temp = pd.DataFrame()
        decision_data_temp = pd.DataFrame()

    return data, data_temp, decision_data_temp, refresh_list, file_used
    
def process_initial_load(data, sector_val, curryr, currmon, msq_load, file_used):

    # For Apt, load last months ysis input so we can get what the sqcons that was there last time for use in the c_sqdiff flag
    if sector_val == "apt":
        if currmon == 1:
            pastmon = 12
            pastyr = curryr - 1
        else: 
            pastmon = currmon - 1
            pastyr = curryr
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/InputFiles/{}sub_{}m{}-ysis.csv".format(get_home(), sector_val, str(pastyr), str(pastmon), sector_val, str(pastyr), str(pastmon)))
        past_data = pd.read_csv(file_path, encoding = 'utf-8',  na_values= "", keep_default_na = False)
        past_data['currmon'] = past_data['currmon'].astype(float)
        past_data['currmon'] = np.where((past_data['currmon'].isnull()== True), 13, past_data['currmon'])
        past_data['subsector'] = "Apt"
        past_data['subid'] = past_data['subid'].astype(int)
        past_data['yr'] = past_data['yr'].astype(int)
        past_data['currmon'] = past_data['currmon'].astype(int)
        past_data['identity_row'] = np.where((past_data['currmon'] != 13), past_data['metcode'] + past_data['subid'].astype(str) + past_data['subsector'] + past_data['yr'].astype(str) + past_data['currmon'].astype(str), past_data['metcode'] + past_data['subid'].astype(str) + past_data['subsector'] + past_data['yr'].astype(str) + past_data['qtr'].astype(str))
        past_data = past_data.drop_duplicates('identity_row')
        past_data = past_data.set_index('identity_row')
        past_data['p_sqcons'] = past_data['sqcons']
        past_data = past_data[['p_sqcons']]

    # We need to fill the currmon column with a value, as having nans here will cause issues. Replace with a dummy variable        
    data['currmon'] = np.where((np.isnan(data['currmon']) == True), 13, data['currmon'])

    # It seems as though the columns for subid yr, and currmon are sometimes read in as floats, so convert them to ints explicitly here
    data['subid'] = data['subid'].astype(int)
    data['yr'] = data['yr'].astype(int)
    data['currmon'] = data['currmon'].astype(int)

    # Drop extra columns that the program wont use if this is the first run of the month - helps reduce dimensionality and size of dataframe
    if file_used == "oob":
        data = data.drop(['sector', 'avail01d', 'dqren11d', 'dqren01d', 'sqinv', 'sqcons', 'sqvac', 'sqvac_chg', 'sqavail', 'sqocc', 'sqabs', 'sqsren', 'sq_Gmrent'], axis=1)

        # Convert surveyed avail vars to the consistent sign with abs in pub, and round to thousandths
        if sector_val != "apt":
            data[['avail10d', 'avail00d']] = round((data[['avail10d', 'avail00d']] * -1), -3)
        else:
            data[['avail10d', 'avail00d']] = round((data[['avail10d', 'avail00d']] * -1), 0)

        data['avail10d_temp'] = np.where((data['avail10d'].isnull() == True), 0, data['avail10d'])
        data['avail00d_temp'] = np.where((data['avail00d'].isnull() == True), 0, data['avail00d'])
        data['avail0d'] = np.where((data['avail10d'].isnull() == False) | (data['avail00d'].isnull() == False), data['avail10d_temp'] + data['avail00d_temp'], np.nan)
        data = data.drop(['avail10d_temp', 'avail00d_temp'],axis=1)

        # Create a column that will keep track if the user overrides a flag and does not think a change is warranted
        data['flag_skip'] = ''
        
        # Rename the p columns with the oob prefix for consistency with forecast program
        for x in list(data.columns):
            if x[0:2] == "p_" and x != "p_sqcons":
                data = data.rename(columns={x:  x[2:] + "_oob"})
        data['vac_oob'] = data['vac']
        data['vac_chg_oob'] = data['vac_chg']
        if sector_val == "ret":
            data['conv_oob'] = data['conv']
            data['demo_oob'] = data['demo']

        # Create columns to store the shim comments
        data['inv_cons_comment'] = ""
        data['avail_comment'] = ""
        data['mrent_comment'] = ""
        data['erent_comment'] = ""

        # Rename columns so that there is consistency across sectors
        if sector_val == "apt" or sector_val == "off" or sector_val == "ret":
            data = data.rename(columns={'rol_g_mrent': 'rol_G_mrent', 'rol_g_merent': 'rol_G_merent'})
        if sector_val == "apt":
            data = data.rename(columns={'sub1to99_Gavgrenx': 'sub1to99_Grenx'})
        if sector_val == "ret":
            data = data.rename(columns={'sub1to99_Gnrenx': 'sub1to99_Grenx'})

        # If this is month 2 or 3 of the quarter, bring down the qrol vars into the m1 or m1 and m2 month rows
        if currmon in [2, 5, 8, 11]:
            data['qrol_vac'] = np.where((data['yr'] == curryr) & (data['currmon'] == currmon - 1), data['qrol_vac'].shift(1), data['qrol_vac'])
            data['qrol_mrent'] = np.where((data['yr'] == curryr) & (data['currmon'] == currmon - 1), data['qrol_mrent'].shift(1), data['qrol_mrent'])
            data['qrol_merent'] = np.where((data['yr'] == curryr) & (data['currmon'] == currmon - 1), data['qrol_merent'].shift(1), data['qrol_merent'])
        if currmon in [3, 6, 9, 12]:
            data['qrol_vac'] = np.where((data['yr'] == curryr) & (data['currmon'] == currmon - 2), data['qrol_vac'].shift(1), data['qrol_vac'])
            data['qrol_mrent'] = np.where((data['yr'] == curryr) & (data['currmon'] == currmon - 2), data['qrol_mrent'].shift(1), data['qrol_mrent'])
            data['qrol_merent'] = np.where((data['yr'] == curryr) & (data['currmon'] == currmon - 2), data['qrol_merent'].shift(1), data['qrol_merent'])
            data['qrol_vac'] = np.where((data['yr'] == curryr) & (data['currmon'] == currmon - 1), data['qrol_vac'].shift(2), data['qrol_vac'])
            data['qrol_mrent'] = np.where((data['yr'] == curryr) & (data['currmon'] == currmon - 1), data['qrol_mrent'].shift(2), data['qrol_mrent'])
            data['qrol_merent'] = np.where((data['yr'] == curryr) & (data['currmon'] == currmon - 1), data['qrol_merent'].shift(2), data['qrol_merent'])

        # Since we only need qrol for months in the current quarter for the rebench check (older periods will just check against mrol), set qrol to mrol in those periods
        if currmon in [1,2,3]:
            currmon_cut = 12
        elif currmon in [4,5,6]:
            currmon_cut = 3
        elif currmon in [7,8,9]:
            currmon_cut = 6
        elif currmon in [10,11,12]: 
            currmon_cut = 9

        data['qrol_vac'] = np.where((data['yr'] == curryr) & (data['currmon'] > currmon_cut), data['qrol_vac'], data['rol_vac'])
        data['qrol_mrent'] = np.where((data['yr'] == curryr) & (data['currmon'] > currmon_cut), data['qrol_mrent'], data['rol_mrent'])
        data['qrol_merent'] = np.where((data['yr'] == curryr) & (data['currmon'] > currmon_cut), data['qrol_merent'], data['rol_merent'])

    orig_cols = list(data.columns)
    if sector_val == "apt" and file_used == "oob":
        orig_cols = orig_cols + ['p_sqcons']
    
    # Label Submarkets as Legacy or Expansion Metros for Industrial
    expansion_list = ["AA", "AB", "AK", "AN", "AQ", "BD", "BF", "BI", "BR", "BS", "CG", "CM", "CN", "CS", "DC", 
                    "DM", "DN", "EP", "FC", "FM", "FR", "GD", "GN", "GR", "HR", "HL", "HT", "KX", "LL", "LO", 
                    "LR", "LV", "LX", "MW", "NF", "NM", "NO", "NY", "OK", "OM", "PV", "RE", "RO", "SC", "SH", 
                    "SR", "SS", "ST", "SY", "TC", "TM", "TO", "TU", "VJ", "VN", "WC", "WK", "WL", "WS"]    

    if sector_val == "ind":
        data['expansion'] = np.where(data['metcode'].isin(expansion_list), "Exp", "Leg")
        data = data[(data['expansion'] == "Leg") | (data['subsector'] == "DW")]
    else:
        data['expansion'] = "Leg"
    
    # Create the submarket groupby identity
    data['identity'] = data['metcode'] + data['subid'].astype(str) + data['subsector']

    # Create the metro groupby identity
    if sector_val == "ret":
        data['identity_met'] = data['metcode'] + "Ret"
    else:
        data['identity_met'] = data['metcode'] + data['subsector']
    
    # Create a variable that we can use to rollup to the US level
    if sector_val != "ind":
        data['identity_us'] = "US" + data['tier'].astype(str)
    else:
        data['identity_us'] = "US" + data['subsector'] + data['expansion']
    
    # Create the row identity
    if sector_val == "ind":
        data['identity_row'] = data['metcode'] + data['subid'].astype(str) + data['subsector'] + data['yr'].astype(str) + data['currmon'].astype(str)
    else:
        data['identity_row'] = np.where((data['currmon'] != 13), data['metcode'] + data['subid'].astype(str) + data['subsector'] + data['yr'].astype(str) + data['currmon'].astype(str), data['metcode'] + data['subid'].astype(str) + data['subsector'] + data['yr'].astype(str) + data['qtr'].astype(str))
    
    # set the index of the dataframe as the row identity
    data = data.set_index('identity_row')
    
    # identify the row that is the current month
    data['curr_tag'] = np.where((data['yr'] == curryr) & (data['currmon'] == currmon), 1, 0)

    # Calculate ROL vac chg
    data['rol_vac_chg'] = np.where((data['identity'] == data['identity'].shift(1)), data['rol_vac'] - data['rol_vac'].shift(1), np.nan)
    data['rol_vac_chg'] = round(data['rol_vac_chg'], 4)

    # Calculate gap change and ROL gap chg
    data['gap_chg'] = np.where((data['identity'] == data['identity'].shift(1)), data['gap'] - data['gap'].shift(1), np.nan)
    data['gap_chg'] = round(data['gap_chg'], 4)
    data['rol_gap_chg'] = np.where((data['identity'] == data['identity'].shift(1)), data['rol_gap'] - data['rol_gap'].shift(1), np.nan)
    data['rol_gap_chg'] = round(data['rol_gap_chg'], 4)
    
    # Join the past sq_cons data in if this is run for apt
    if sector_val == "apt" and file_used == "oob":
        data = data.join(past_data, on='identity_row')
        del past_data
        gc.collect()

    # Load last month's archived msq data. This will allow us to identify what props are being rebenched in the nc window
    if file_used == "oob":
        if currmon == 1:
            p_mon = 12
            p_yr = curryr - 1
        else:
            p_mon = currmon - 1
            p_yr = curryr
        if sector_val == "apt":
            pathlist = Path("/home/central/square/data/{}/production/archive_{}_{}_final".format(sector_val, p_yr, p_mon)).glob('**/*msq.dta')
        elif sector_val == "off":
            pathlist = Path("/home/central/square/data/{}/production/archive/archive_{}m{}_final".format(sector_val, p_yr, p_mon)).glob('**/*msq.dta')
        elif sector_val == "ind":
            pathlist = Path("/home/central/square/data/{}/production/{}m{}_final_squaredmsqs".format(sector_val, p_yr, p_mon)).glob('**/*msq.dta')
        elif sector_val == "ret":
            pathlist = Path("/home/central/square/data/{}/production/archive_{}_{}_final".format(sector_val, p_yr, p_mon)).glob('**/*msq.dta')
        paths = []
        for path in pathlist:
            paths.append(str(path))
        pool = mp.Pool(int(mp.cpu_count()*0.7))
        result_async = [pool.apply_async(load_msqs, args = (sector_val, path, )) for path in
                        paths]
        results = [r.get() for r in result_async]

        p_data_in = pd.DataFrame()
        p_data_in = p_data_in.append(results, ignore_index=True)
        p_data_in.sort_values(by=['metcode', 'id', 'yr', 'qtr', 'currmon'], ascending=[True, True, True, True, True], inplace=True)
        p_data_in = p_data_in.reset_index(drop=True)

        pool.close()
        
        if sector_val == "apt":
            p_data_in = p_data_in.rename(columns={'totunitx': 'sizex', 'availx': 'totavailx', 'avgrenx': 'renx', 'avgrenxM': 'renxM'})
        elif sector_val == "ind":
            p_data_in = p_data_in.rename(columns={'ind_size': 'sizex'})
        elif sector_val == "ret":
            p_data_in = p_data_in.rename(columns={'availx': 'totavailx'})

        if sector_val == "ind":
            p_data_in['type2'] = np.where((p_data_in['type2'] == "D") | (p_data_in['type2'] == "W"), "DW", "F")

        if sector_val == "ret":
            p_data_in = p_data_in[(p_data_in['type1'] == "C") | (p_data_in['type1'] == "N")]
        
        if sector_val == "apt" or sector_val == "off":
            p_data_in['identity_met'] = p_data_in['metcode'] + sector_val.title()
        elif sector_val == "ret":
            p_data_in['identity_met'] = p_data_in['metcode'] + p_data_in['type1']
        elif sector_val == "ind":
            p_data_in['identity_met'] = p_data_in['metcode'] + p_data_in['type2']

        
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_msq_data_prior_month.pickle".format(get_home(), sector_val))
        p_data_in.to_pickle(file_path)
        del p_data_in
        gc.collect()

    # Load all msqs for the sector and combine into one dataframe
    # Only need to do this if the msqs havent been refreshed, so analyst has option to skip this section and rely on the data that was saved upon first load of module
    if msq_load == "Y" or file_used == "oob":                

        pathlist = Path("/home/central/square/data/{}/production/msq/output".format(sector_val)).glob('**/*msq.dta')
        r = re.compile("..msq\.dta")
        paths = []
        for path in pathlist:
            path_in_str = str(path)
            testing = path_in_str[-9:]
            testing_folder = path_in_str[-16:-10]
            modified = os.path.getmtime(path)
            m_year, m_month, m_day = time.localtime(modified)[:-6]
            if r.match(testing) != None and testing_folder == "output" and (m_year > curryr or (m_month >= currmon and m_year == curryr)):
                paths.append(path_in_str)

        pool = mp.Pool(int(mp.cpu_count()*0.7))
        result_async = [pool.apply_async(load_msqs, args = (sector_val, path, )) for path in
                        paths]
        results = [r.get() for r in result_async]

        msq_data_in = pd.DataFrame()
        msq_data_in = msq_data_in.append(results, ignore_index=True)
        msq_data_in['subid'] = msq_data_in['subid'].astype(int)
        msq_data_in['yr'] = msq_data_in['yr'].astype(int)
        msq_data_in['qtr'] = msq_data_in['qtr'].astype(int)
        msq_data_in['yearx'] = msq_data_in['yearx'].astype(int)
        msq_data_in.sort_values(by=['metcode', 'id', 'yr', 'qtr', 'currmon'], ascending=[True, True, True, True, True], inplace=True)
        msq_data_in = msq_data_in.reset_index(drop=True)

        pool.close()
        
        if sector_val == "apt":
            msq_data_in = msq_data_in.rename(columns={'totunitx': 'sizex', 'availx': 'totavailx', 'avgrenx': 'renx', 'avgrenxM': 'renxM'})
        elif sector_val == "ind":
            msq_data_in = msq_data_in.rename(columns={'ind_size': 'sizex'})
        elif sector_val == "ret":
            msq_data_in = msq_data_in.rename(columns={'availx': 'totavailx'})

        if sector_val == "ind":
            msq_data_in['type2'] = np.where((msq_data_in['type2'] == "D") | (msq_data_in['type2'] == "W"), "DW", "F")

        if sector_val == "ret":
            msq_data_in = msq_data_in[(msq_data_in['type1'] == "C") | (msq_data_in['type1'] == "N")]
        
        if sector_val == "apt" or sector_val == "off":
            msq_data_in['identity_met'] = msq_data_in['metcode'] + sector_val.title()
        elif sector_val == "ret":
            msq_data_in['identity_met'] = msq_data_in['metcode'] + msq_data_in['type1']
        elif sector_val == "ind":
            msq_data_in['identity_met'] = msq_data_in['metcode'] + msq_data_in['type2']
        # Tag props that have lagged rent surveys within the range to be included in the survey packet rent chg inventory
        temp = msq_data_in.copy()
        temp = temp[np.isnan(temp['currmon']) == False]
        if sector_val != "ret":
            temp = temp[temp['renxM']==0]
        else:
            temp = temp[temp['nrenxM']==0]
        temp['curr_tag'] = np.where((temp['yr'] == curryr) & (temp['currmon'] == currmon), 1, 0)
        temp = temp[temp['curr_tag'] == 0]
        temp = temp.drop_duplicates('id')
        temp = temp[['id']]
        temp['has_l_surv'] = 1
        temp = temp.set_index('id')
        msq_data_in = msq_data_in.join(temp, on='id')
        del temp
        gc.collect()
        msq_data_in['has_l_surv'] = msq_data_in['has_l_surv'].fillna(0)

        if sector_val == "apt" or sector_val == "off":
            msq_data_in['identity'] = msq_data_in['metcode'] + msq_data_in['subid'].astype(str) + sector_val.title()
        elif sector_val == "ret":
            msq_data_in['identity'] = msq_data_in['metcode'] + msq_data_in['subid'].astype(str) + msq_data_in['type1']
        elif sector_val == "ind":
            msq_data_in['identity'] = msq_data_in['metcode'] + msq_data_in['subid'].astype(str) + msq_data_in['type2']
        
        # Special caveat here for office is that 99 subnames are ok if they are in a sub that represents non-cbd. But this is only the case for rolling up metsq data, for the review packet, it looks like DQ removed even these cases
        msq_data_in['balance_test'] = msq_data_in['submkt'].str.slice(0,2)
        if sector_val != "off":
            msq_data_in = msq_data_in[msq_data_in['balance_test'] != '99']
        elif sector_val == "off":
            msq_data_in = msq_data_in[(msq_data_in['balance_test'] != '99') | ((msq_data_in['subid'] >= 81) & (msq_data_in['metcode'] != "FM"))]

        msq_data_only_qtr = msq_data_in.copy()
        msq_data_only_qtr = msq_data_only_qtr[msq_data_only_qtr['yearx'] < 2009]
        msq_data_only_qtr = msq_data_only_qtr[np.isnan(msq_data_only_qtr['currmon']) == True]
        msq_data_only_qtr = msq_data_only_qtr.drop_duplicates(['yr', 'qtr'])
        msq_data_only_qtr['only_qtr'] = 1
        msq_data_only_qtr['qtr_ident'] = msq_data_only_qtr['yr'].astype(str) + msq_data_only_qtr['qtr'].astype(str)
        msq_data_only_qtr = msq_data_only_qtr.set_index('qtr_ident')
        msq_data_only_qtr = msq_data_only_qtr[['only_qtr']]

        msq_data_in['qtr_ident'] = msq_data_in['yr'].astype(str) + msq_data_in['qtr'].astype(str)
        msq_data_in = msq_data_in.join(msq_data_only_qtr, on='qtr_ident')
        del msq_data_only_qtr
        gc.collect()

        msq_data_in['currmon'] = np.where((msq_data_in['only_qtr'] == 1) & (msq_data_in['qtr'] == 1), 3, msq_data_in['currmon'])
        msq_data_in['currmon'] = np.where((msq_data_in['only_qtr'] == 1) & (msq_data_in['qtr'] == 2), 6, msq_data_in['currmon'])
        msq_data_in['currmon'] = np.where((msq_data_in['only_qtr'] == 1) & (msq_data_in['qtr'] == 3), 9, msq_data_in['currmon'])
        msq_data_in['currmon'] = np.where((msq_data_in['only_qtr'] == 1) & (msq_data_in['qtr'] == 4), 12, msq_data_in['currmon'])
        msq_data_in['currmon'] = np.where((msq_data_in['currmon'].isnull() == True) & (msq_data_in['qtr'] == 1), 3, msq_data_in['currmon'])
        msq_data_in['currmon'] = np.where((msq_data_in['currmon'].isnull() == True) & (msq_data_in['qtr'] == 2), 6, msq_data_in['currmon'])
        msq_data_in['currmon'] = np.where((msq_data_in['currmon'].isnull() == True) & (msq_data_in['qtr'] == 3), 9, msq_data_in['currmon'])
        msq_data_in['currmon'] = np.where((msq_data_in['currmon'].isnull() == True) & (msq_data_in['qtr'] == 4), 12, msq_data_in['currmon'])
        msq_data_in['yr'] = msq_data_in['yr'].astype(int)
        msq_data_in['qtr'] = msq_data_in['qtr'].astype(int)
        msq_data_in['currmon'] = msq_data_in['currmon'].astype(int)

        if sector_val == "ind":
            msq_data_in.sort_values(by=['id', 'type2', 'metcode', 'yr', 'qtr', 'currmon'], inplace=True)
            msq_data_in['join_ident'] = msq_data_in['metcode'] + msq_data_in['type2'] + msq_data_in['yr'].astype(str) + msq_data_in['qtr'].astype(str) + msq_data_in['currmon'].astype(str)
        else:
            msq_data_in.sort_values(by=['id', 'metcode', 'yr', 'qtr', 'currmon'], inplace=True)
            msq_data_in['join_ident'] = msq_data_in['metcode'] + msq_data_in['yr'].astype(str) + msq_data_in['qtr'].astype(str) + msq_data_in['currmon'].astype(str)

        if sector_val == "ret":
            msq_data_in['type1'] = np.where(msq_data_in['subid'] == 70, 'NC', msq_data_in['type1'])

        msq_data_in = msq_data_in.drop(['submkt', 'qtr_ident'],axis=1)
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_msq_data.pickle".format(get_home(), sector_val))
        msq_data_in.to_pickle(file_path)

        # Calculate avail and rent survey coverage by met for 2 year historical period, and use that to set the coverage thresholds for flags
        msq_data_in['currmon_fill'] = np.where(msq_data_in['currmon'].isnull() == True, msq_data_in['qtr'] * 3, msq_data_in['currmon'])
        msq_data_in = msq_data_in[(msq_data_in['yr'] >= curryr - 1) | ((msq_data_in['yr'] == curryr - 2) & (msq_data_in['currmon_fill'] > currmon))]
        msq_data_in = msq_data_in.reset_index(drop=True)
        if sector_val == "ind":
            msq_data_in['identity_met'] = msq_data_in['metcode'] + msq_data_in['type2']
        else:
            msq_data_in['identity_met'] = msq_data_in['metcode']
        msq_data_in['met_inv'] = msq_data_in.groupby(['identity_met', 'yr', 'qtr', 'currmon'])['sizex'].transform('sum')
        msq_data_in['avail_inv_surv'] = msq_data_in[msq_data_in['availxM'] == 0].groupby(['identity_met', 'yr', 'qtr', 'currmon'])['sizex'].transform('sum')
        msq_data_in['rent_inv_surv'] = msq_data_in[msq_data_in['renxM'] == 0].groupby(['identity_met', 'yr', 'qtr', 'currmon'])['sizex'].transform('sum')
        msq_data_in.sort_values(by=['identity_met', 'yr', 'qtr', 'currmon', 'avail_inv_surv'], ascending=[True, True, True, True, False], inplace=True)
        msq_data_in['avail_inv_surv'] = msq_data_in.groupby(['identity_met', 'yr', 'qtr', 'currmon'])['avail_inv_surv'].ffill()
        msq_data_in.sort_values(by=['identity_met', 'yr', 'qtr', 'currmon', 'rent_inv_surv'], ascending=[True, True, True, True, False], inplace=True)
        msq_data_in['rent_inv_surv'] = msq_data_in.groupby(['identity_met', 'yr', 'qtr', 'currmon'])['rent_inv_surv'].ffill()
        msq_data_in['per_surv_avail'] = msq_data_in['avail_inv_surv'] / msq_data_in['met_inv']
        msq_data_in['per_surv_rent'] = msq_data_in['rent_inv_surv'] / msq_data_in['met_inv']
        msq_data_in = msq_data_in.drop_duplicates(['identity_met', 'yr', 'qtr', 'currmon'])
        msq_data_in[['per_surv_avail', 'per_surv_rent']] = msq_data_in[['per_surv_avail', 'per_surv_rent']].fillna(0)
        msq_data_in['avg_per_surv_avail'] = msq_data_in.groupby('identity_met')['per_surv_avail'].transform('mean')
        msq_data_in['avg_per_surv_rent'] = msq_data_in.groupby('identity_met')['per_surv_rent'].transform('mean')
        msq_data_in = msq_data_in.drop_duplicates('identity_met')
        msq_data_in = msq_data_in[['identity_met', 'avg_per_surv_avail', 'avg_per_surv_rent']]
        msq_data_in['met_v_scov_percentile'] = msq_data_in['avg_per_surv_avail'].rank(pct=True)
        msq_data_in['met_v_scov_percentile'] = round(msq_data_in['met_v_scov_percentile'], 1)
        msq_data_in['met_r_scov_percentile'] = msq_data_in['avg_per_surv_rent'].rank(pct=True)
        msq_data_in['met_r_scov_percentile'] = round(msq_data_in['met_r_scov_percentile'], 1)

        msq_data_in['v_diff'] = abs(msq_data_in['met_v_scov_percentile'] - 0.30)
        msq_data_in['r_diff'] = abs(msq_data_in['met_r_scov_percentile'] - 0.30)
        v_threshold = msq_data_in.sort_values(by=['v_diff'], ascending=[True]).reset_index().loc[0]['avg_per_surv_avail']
        r_threshold = msq_data_in.sort_values(by=['r_diff'], ascending=[True]).reset_index().loc[0]['avg_per_surv_rent']
        v_threshold_mod = min(max(v_threshold, 0.04), 0.1)
        r_threshold_mod = min(max(r_threshold, 0.04), 0.1)
        cov_thresh = pd.DataFrame(index=[0], data={'v_cov': v_threshold_mod, 'r_cov': r_threshold_mod, 'v_cov_true': v_threshold, 'r_cov_true': r_threshold}, columns=['v_cov', 'r_cov', 'v_cov_true', 'r_cov_true'])

        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_surv_coverage.pickle".format(get_home(), sector_val))
        cov_thresh.to_pickle(file_path)
        del cov_thresh
        del msq_data_in
        gc.collect()

        # Call the function to load and process the sq insight stats. The processed files will be saved to the network and can then be read in at any time
        if currmon < 4:
            currqtr = 1
        elif currmon < 7:
            currqtr = 2
        elif currmon < 10: 
            currqtr = 3
        else:
            currqtr = 4
        process_sq_insight(sector_val, curryr, currmon, currqtr)     

        # Calculate metro level sq vars
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_msq_data.pickle".format(get_home(), sector_val))
        msq_data1 = pd.read_pickle(file_path)
        
        if sector_val == "apt":
            round_val = 0
        else:
            round_val = -3

        data_all_periods = data.copy()
        if sector_val != "ret":
            data_all_periods = data_all_periods.drop_duplicates(['subsector', 'metcode', 'yr', 'qtr', 'currmon'])
        else:
            data_all_periods = data_all_periods.drop_duplicates(['metcode', 'yr', 'qtr', 'currmon'])
        msq_data = data_all_periods.copy()
        del data_all_periods
        gc.collect()
        msq_data['currmon'] = np.where((np.isnan(msq_data['currmon']) == True) & (msq_data['qtr'] == 1), 3, msq_data['currmon'])
        msq_data['currmon'] = np.where((np.isnan(msq_data['currmon']) == True) & (msq_data['qtr'] == 2), 6, msq_data['currmon'])
        msq_data['currmon'] = np.where((np.isnan(msq_data['currmon']) == True) & (msq_data['qtr'] == 3), 9, msq_data['currmon'])
        msq_data['currmon'] = np.where((np.isnan(msq_data['currmon']) == True) & (msq_data['qtr'] == 4), 12, msq_data['currmon'])
        msq_data['currmon'] = np.where((np.isnan(msq_data['currmon']) == True) & (msq_data['qtr'] == 5) & ((msq_data['qtr'].shift(1) != 4) | ((msq_data['metcode'] != msq_data['metcode'].shift(1)) & (msq_data['subsector'] != msq_data['subsector'].shift(1)))), 12, msq_data['currmon'])

        if sector_val == "ind":
            msq_data['join_ident'] = msq_data['metcode'] + msq_data['subsector'] + msq_data['yr'].astype(str) + msq_data['qtr'].astype(str) + msq_data['currmon'].astype(str)
        else:
            msq_data['join_ident'] = msq_data['metcode'] + msq_data['yr'].astype(str) +  + msq_data['qtr'].astype(str) + msq_data['currmon'].astype(str)   

        if sector_val == "ind":
            drop_list = ['metcode', 'yr', 'qtr', 'currmon', 'type2']
            msq_data = msq_data[['join_ident', 'subsector', 'metcode', 'yr', 'qtr', 'currmon']]
        else:
            drop_list = ['metcode', 'yr', 'qtr', 'currmon']
            msq_data = msq_data[['join_ident'] + drop_list]

        msq_data1['currmon'] = np.where((msq_data1['only_qtr'] == 1) & (msq_data1['currmon'] % 3 != 0), np.nan, msq_data1['currmon'])
        msq_data1 = msq_data1[(np.isnan(msq_data1['currmon']) == False)]
        msq_data1['metsqinv'] = msq_data1.groupby('join_ident')['sizex'].transform('sum')
        msq_data1['metsqinv'] = round(msq_data1['metsqinv'], round_val)
        msq_data1['metsqavail'] = msq_data1.groupby('join_ident')['totavailx'].transform('sum')
        msq_data1['metsqavail'] = round(msq_data1['metsqavail'], round_val)
        msq_data1['metsqvac'] = msq_data1['metsqavail'] / msq_data1['metsqinv']
        msq_data1['metsqvac'] = round(msq_data1['metsqvac'], 4)
        msq_data1['metsqoccstk'] = msq_data1['metsqinv'] - msq_data1['metsqavail']
        if sector_val != "ret":
            msq_data1['mrev'] = msq_data1['renx'] * msq_data1['sizex']
        else:
            msq_data1['mrev'] = msq_data1['nrenx'] * msq_data1['sizex']
        msq_data1['met_rev'] = msq_data1.groupby('join_ident')['mrev'].transform('sum')
        msq_data1['metsqsren'] = msq_data1['met_rev'] / msq_data1['metsqinv']
        msq_data1['metsqsren'] = round(msq_data1['metsqsren'], 2)

        msq_data1 = msq_data1.drop_duplicates('join_ident')
        
        if sector_val == "ind":
            msq_data1['metsqabs'] = np.where((msq_data1['metcode'] == msq_data1['metcode'].shift(1)) & (msq_data1['type2'] == msq_data1['type2'].shift(1)), msq_data1['metsqoccstk'] - msq_data1['metsqoccstk'].shift(1), np.nan)
            msq_data1['metsqvacchg'] = np.where((msq_data1['metcode'] == msq_data1['metcode'].shift(1)) & (msq_data1['type2'] == msq_data1['type2'].shift(1)), msq_data1['metsqvac'] - msq_data1['metsqvac'].shift(1), np.nan)
            msq_data1['metsq_Gmrent'] = np.where((msq_data1['metcode'] == msq_data1['metcode'].shift(1)) & (msq_data1['type2'] == msq_data1['type2'].shift(1)), (msq_data1['metsqsren'] - msq_data1['metsqsren'].shift(1)) / msq_data1['metsqsren'].shift(1), np.nan)
        else:
            msq_data1['metsqabs'] = np.where((msq_data1['metcode'] == msq_data1['metcode'].shift(1)), msq_data1['metsqoccstk'] - msq_data1['metsqoccstk'].shift(1), np.nan)
            msq_data1['metsqvacchg'] = np.where((msq_data1['metcode'] == msq_data1['metcode'].shift(1)), msq_data1['metsqvac'] - msq_data1['metsqvac'].shift(1), np.nan)
            msq_data1['metsq_Gmrent'] = np.where((msq_data1['metcode'] == msq_data1['metcode'].shift(1)), (msq_data1['metsqsren'] - msq_data1['metsqsren'].shift(1)) / msq_data1['metsqsren'].shift(1), np.nan)
        msq_data1['metsq_Gmrent'] = round( msq_data1['metsq_Gmrent'],3)
        if sector_val == "ind":
            msq_data1 = msq_data1[['join_ident','metcode', 'yr', 'qtr', 'currmon', 'type2', 'metsqinv', 'metsqavail', 'metsqvac', 'metsqvacchg', 'metsqoccstk', 'metsqabs', 'metsqsren', 'metsq_Gmrent']]
        else:
            msq_data1 = msq_data1[['join_ident','metcode', 'yr', 'qtr', 'currmon', 'metsqinv', 'metsqavail', 'metsqvac', 'metsqvacchg', 'metsqoccstk', 'metsqabs', 'metsqsren', 'metsq_Gmrent']]

        msq_data = msq_data.join(msq_data1.set_index('join_ident').drop(drop_list, axis=1), on='join_ident')
        del msq_data1
        gc.collect()

        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_msq_data.pickle".format(get_home(), sector_val))
        msq_data2 = pd.read_pickle(file_path)

        if currmon == 1:
            msq_data2 = msq_data2[((msq_data2['yr'] == curryr) & (msq_data2['currmon'] == currmon)) | ((msq_data2['yr'] == curryr - 1) & (msq_data2['currmon'] == 12))]
        else:
            msq_data2 = msq_data2[((msq_data2['yr'] == curryr) & (msq_data2['currmon'] == currmon)) | ((msq_data2['yr'] == curryr) & (msq_data2['currmon'] == currmon - 1))]
        if sector_val != "ret":
            msq_data2['10_tag'] = np.where((msq_data2['id'] == msq_data2['id'].shift(1)) & (msq_data2['renxM'] == 0) & (msq_data2['renxM'].shift(1) == 1), 1, 0)
            msq_data2 = msq_data2[(msq_data2['10_tag'] == 1) | ((msq_data2['10_tag'].shift(-1) == 1) & (msq_data2['id'] == msq_data2['id'].shift(-1)))]
            msq_data2['metsqinv'] = msq_data2.groupby('join_ident')['sizex'].transform('sum')
            msq_data2['metdqinvren10'] = msq_data2.groupby('join_ident')['sizex'].transform('sum')
        else:
            msq_data2['10_tag'] = np.where((msq_data2['id'] == msq_data2['id'].shift(1)) & (msq_data2['nrenxM'] == 0) & (msq_data2['nrenxM'].shift(1) == 1), 1, 0)
            msq_data2 = msq_data2[(msq_data2['10_tag'] == 1) | ((msq_data2['10_tag'].shift(-1) == 1) & (msq_data2['id'] == msq_data2['id'].shift(-1)))]
            msq_data2['metsqinv'] = msq_data2.groupby('join_ident')['nsizex'].transform('sum')
            msq_data2['metdqinvren10'] = msq_data2.groupby('join_ident')['nsizex'].transform('sum')
        
        
        if sector_val != "ret":
            msq_data2['mrev'] = msq_data2['renx'] * msq_data2['sizex']
        else:
            msq_data2['mrev'] = msq_data2['nrenx'] * msq_data2['nsizex']
        msq_data2['met_rev'] = msq_data2.groupby('join_ident')['mrev'].transform('sum')
        msq_data2['metsqsren'] = msq_data2['met_rev'] / msq_data2['metsqinv']
        msq_data2['metsqsren'] = round(msq_data2['metsqsren'], 2)
        msq_data2['metdqren10d'] = np.where((msq_data2['id'] == msq_data2['id'].shift(1)), (msq_data2['metsqsren'] - msq_data2['metsqsren'].shift(1)) / msq_data2['metsqsren'].shift(1), np.nan)
        msq_data2 = msq_data2.drop_duplicates('join_ident')
        msq_data2 = msq_data2[(msq_data2['yr'] == curryr) & (msq_data2['currmon'] == currmon)]
        msq_data2['metdqinvren10'] = round(msq_data2['metdqinvren10'], round_val)
        msq_data2['metdqren10d'] = round( msq_data2['metdqren10d'], 3)
        if sector_val == "ind":
            msq_data2 = msq_data2[['join_ident','metcode', 'yr', 'qtr', 'currmon', 'type2', 'metdqinvren10', 'metdqren10d']]
        else:
            msq_data2 = msq_data2[['join_ident','metcode', 'yr', 'qtr', 'currmon', 'metdqinvren10', 'metdqren10d']]

        msq_data = msq_data.join(msq_data2.set_index('join_ident').drop(drop_list, axis=1), on='join_ident')
        del msq_data2
        gc.collect()

        # Note: In an effort to align the sq cons data with the trend data, will move away from the method of only reporting square rollup in the third month of the quarter for periods that do not have a monthly breakout in the msq
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_msq_data.pickle".format(get_home(), sector_val))
        msq_data3 = pd.read_pickle(file_path)

        msq_data3 = msq_data3[((msq_data3['yearx'] > 2008) | ((msq_data3['yearx'] == 2008) & (msq_data3['month'] >= 10)))]
        msq_data3['qtr'] = np.where(msq_data3['month'] <= 3, 1, 4)
        msq_data3['qtr'] = np.where((msq_data3['month'] <= 6) & (msq_data3['month'] >= 4), 2, msq_data3['qtr'])
        msq_data3['qtr'] = np.where((msq_data3['month'] <= 9) & (msq_data3['month'] >= 7), 3, msq_data3['qtr'])
        msq_data3['qtr'] = np.where((np.isnan(msq_data3['month']) == True), 2, msq_data3['qtr'])

        msq_data3['month'] = np.where(np.isnan(msq_data3['month']) == True, 6, msq_data3['month'])
        msq_data3['yearx'] = msq_data3['yearx'].astype(int)
        msq_data3['month'] = msq_data3['month'].astype(int)

        if sector_val == "ind":
            msq_data3['join_ident'] = msq_data3['metcode'] + msq_data3['type2'] + msq_data3['yearx'].astype(str) + msq_data3['qtr'].astype(str) + msq_data3['month'].astype(str)
        else:
            msq_data3['join_ident'] = msq_data3['metcode'] + msq_data3['yearx'].astype(str) + msq_data3['qtr'].astype(str) + msq_data3['month'].astype(str)
        msq_data3 = msq_data3.drop_duplicates('id')
        msq_data3['metsqcons'] = msq_data3.groupby('join_ident')['sizex'].transform('sum')
        msq_data3['metncavail'] = msq_data3[(msq_data3['yearx'] == curryr) & (msq_data3['month'] == currmon)].groupby('join_ident')['totavailx'].transform('sum')
        msq_data3['metsqcons'] = round(msq_data3['metsqcons'], round_val)
        msq_data3['metncavail'] = round(msq_data3['metncavail'], round_val)
        msq_data3['metncvac'] = msq_data3['metncavail'] / msq_data3['metsqcons']
        msq_data3['metncvac'] = round(msq_data3['metncvac'], 4)
        msq_data3[['metsqcons', 'metncavail', 'metncvac']] = msq_data3[['metsqcons', 'metncavail', 'metncvac']].fillna(0)
        msq_data3.sort_values(by=['yr', 'qtr', 'currmon'], ascending=[True, True, False], inplace=True)
        msq_data3 = msq_data3.drop_duplicates('join_ident')
        if sector_val == "ind":
            msq_data3 = msq_data3[['join_ident','metcode', 'yr', 'qtr', 'currmon', 'type2', 'metsqcons', 'metncavail', 'metncvac']]
        else:
            msq_data3 = msq_data3[['join_ident', 'metcode', 'yr', 'qtr', 'currmon', 'metsqcons', 'metncavail', 'metncvac']]

        msq_data = msq_data.join(msq_data3.set_index('join_ident').drop(drop_list, axis=1), on='join_ident')
        del msq_data3
        gc.collect()

        msq_data = msq_data.set_index('join_ident')
        msq_data['metdqinvren10'] = msq_data['metdqinvren10'].fillna(0)
        msq_data['metsqcons'] = msq_data['metsqcons'].fillna(0)

        if sector_val == "ind":
            msq_data.sort_values(by=['subsector', 'metcode', 'yr', 'qtr', 'currmon'], ascending=[True, True, True, True, True], inplace=True)
        else:
            msq_data.sort_values(by=['metcode', 'yr', 'qtr', 'currmon'], ascending=[True, True, True, True], inplace=True)
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_sq_metstats.pickle".format(get_home(), sector_val))
        msq_data.to_pickle(file_path)
        del msq_data
        gc.collect()

        # Calculate the sub level sq vars. Will do this on the fly, since if msqs are update and edits have already been made, even refreshing the input file with the flat file output from DQ's program wont help update these vars, becasue that flat file wont be read in
        
        data_all_periods = data.copy()
        data_all_periods = data_all_periods.drop_duplicates(['subsector', 'metcode', 'subid', 'yr', 'qtr', 'currmon'])
        msq_data = data_all_periods.copy()
        del data_all_periods
        gc.collect()
        msq_data['currmon'] = np.where((np.isnan(msq_data['currmon']) == True) & (msq_data['qtr'] == 1), 3, msq_data['currmon'])
        msq_data['currmon'] = np.where((np.isnan(msq_data['currmon']) == True) & (msq_data['qtr'] == 2), 6, msq_data['currmon'])
        msq_data['currmon'] = np.where((np.isnan(msq_data['currmon']) == True) & (msq_data['qtr'] == 3), 9, msq_data['currmon'])
        msq_data['currmon'] = np.where((np.isnan(msq_data['currmon']) == True) & (msq_data['qtr'] == 4), 12, msq_data['currmon'])
        msq_data['currmon'] = np.where((np.isnan(msq_data['currmon']) == True) & (msq_data['qtr'] == 5) & ((msq_data['qtr'].shift(1) != 4) | ((msq_data['metcode'] != msq_data['metcode'].shift(1)) & (msq_data['subid'] != msq_data['subid'].shift(1)) & (msq_data['subsector'] != msq_data['subsector'].shift(1)))), 12, msq_data['currmon'])

        if sector_val == "ind" or sector_val == "ret":
            msq_data['join_ident'] = msq_data['metcode'] + msq_data['subid'].astype(str) + msq_data['subsector'] + msq_data['yr'].astype(str) + msq_data['qtr'].astype(str) + msq_data['currmon'].astype(str)
        else:
            msq_data['join_ident'] = msq_data['metcode'] + msq_data['subid'].astype(str) + msq_data['yr'].astype(str)  + msq_data['qtr'].astype(str) + msq_data['currmon'].astype(str)   

        if sector_val == "ind":
            drop_list = ['metcode', 'subid', 'yr', 'qtr', 'currmon', 'type2']
            msq_data = msq_data[['join_ident', 'subsector', 'metcode', 'subid', 'yr', 'qtr', 'currmon']]
        elif sector_val == "ret":
            drop_list = ['metcode', 'subid', 'yr', 'qtr', 'currmon', 'type1']
            msq_data = msq_data[['join_ident', 'subsector', 'metcode', 'subid', 'yr', 'qtr', 'currmon']]
        else:
            drop_list = ['metcode', 'subid', 'yr', 'qtr', 'currmon']
            msq_data = msq_data[['join_ident'] + drop_list]
        
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_msq_data.pickle".format(get_home(), sector_val))
        msq_data1 = pd.read_pickle(file_path)
        if sector_val == "ind":
            msq_data1['join_ident'] = msq_data1['metcode'] + msq_data1['subid'].astype(str) + msq_data1['type2'] + msq_data1['yr'].astype(str) + msq_data1['qtr'].astype(str) + msq_data1['currmon'].astype(str)
            msq_data1.sort_values(by=['type2', 'metcode', 'subid', 'yr', 'qtr', 'currmon'], inplace=True)
        elif sector_val == "ret":
            msq_data1['join_ident'] = msq_data1['metcode'] + msq_data1['subid'].astype(str) + msq_data1['type1'] + msq_data1['yr'].astype(str) + msq_data1['qtr'].astype(str) + msq_data1['currmon'].astype(str)
            msq_data1.sort_values(by=['type1', 'metcode', 'subid', 'yr', 'qtr', 'currmon'], inplace=True)
        else:
            msq_data1['join_ident'] = msq_data1['metcode'] + msq_data1['subid'].astype(str) + msq_data1['yr'].astype(str) + msq_data1['qtr'].astype(str) + msq_data1['currmon'].astype(str)
            msq_data1.sort_values(by=['metcode', 'subid', 'yr', 'qtr', 'currmon'], inplace=True)
        msq_data1['currmon'] = np.where((msq_data1['only_qtr'] == 1) & (msq_data1['currmon'] % 3 != 0), np.nan, msq_data1['currmon'])
        msq_data1 = msq_data1[(np.isnan(msq_data1['currmon']) == False)]
        msq_data1['sqinv'] = msq_data1.groupby('join_ident')['sizex'].transform('sum')
        msq_data1['sqinv'] = round(msq_data1['sqinv'], round_val)
        msq_data1['sqavail'] = msq_data1.groupby('join_ident')['totavailx'].transform('sum')
        msq_data1['sqavail'] = round(msq_data1['sqavail'], round_val)
        msq_data1['sqvac'] = msq_data1['sqavail'] / msq_data1['sqinv']
        msq_data1['sqvac'] = round(msq_data1['sqvac'], 4)
        msq_data1['sqocc'] = msq_data1['sqinv'] - msq_data1['sqavail']
        if sector_val != "ret":
            msq_data1['mrev'] = msq_data1['renx'] * msq_data1['sizex']
        else:
            msq_data1['mrev'] = msq_data1['nrenx'] * msq_data1['sizex']
        msq_data1['sub_rev'] = msq_data1.groupby('join_ident')['mrev'].transform('sum')
        msq_data1['sqsren'] = msq_data1['sub_rev'] / msq_data1['sqinv']
        # DQ doesnt round before calcing sq rent growth, so for consistency, will also not round. But should we???
        #msq_data1['sqsren'] = round(msq_data1['sqsren'], 2)
        msq_data1 = msq_data1.drop_duplicates('join_ident')
        if sector_val == "ind":
            msq_data1['sqabs'] = np.where((msq_data1['metcode'] == msq_data1['metcode'].shift(1)) & (msq_data1['type2'] == msq_data1['type2'].shift(1)) & (msq_data1['subid'] == msq_data1['subid'].shift(1)), msq_data1['sqocc'] - msq_data1['sqocc'].shift(1), np.nan)
            msq_data1['sqvac_chg'] = np.where((msq_data1['metcode'] == msq_data1['metcode'].shift(1)) & (msq_data1['type2'] == msq_data1['type2'].shift(1)) & (msq_data1['subid'] == msq_data1['subid'].shift(1)), msq_data1['sqvac'] - msq_data1['sqvac'].shift(1), np.nan)
            msq_data1['sq_Gmrent'] = np.where((msq_data1['metcode'] == msq_data1['metcode'].shift(1)) & (msq_data1['type2'] == msq_data1['type2'].shift(1)) & (msq_data1['subid'] == msq_data1['subid'].shift(1)), (msq_data1['sqsren'] - msq_data1['sqsren'].shift(1)) / msq_data1['sqsren'].shift(1), np.nan)
        elif sector_val == "ret":
            msq_data1['sqabs'] = np.where((msq_data1['metcode'] == msq_data1['metcode'].shift(1)) & (msq_data1['type1'] == msq_data1['type1'].shift(1)) & (msq_data1['subid'] == msq_data1['subid'].shift(1)), msq_data1['sqocc'] - msq_data1['sqocc'].shift(1), np.nan)
            msq_data1['sqvac_chg'] = np.where((msq_data1['metcode'] == msq_data1['metcode'].shift(1)) & (msq_data1['type1'] == msq_data1['type1'].shift(1)) & (msq_data1['subid'] == msq_data1['subid'].shift(1)), msq_data1['sqvac'] - msq_data1['sqvac'].shift(1), np.nan)
            msq_data1['sq_Gmrent'] = np.where((msq_data1['metcode'] == msq_data1['metcode'].shift(1)) & (msq_data1['type1'] == msq_data1['type1'].shift(1)) & (msq_data1['subid'] == msq_data1['subid'].shift(1)), (msq_data1['sqsren'] - msq_data1['sqsren'].shift(1)) / msq_data1['sqsren'].shift(1), np.nan)
        else:
            msq_data1['sqabs'] = np.where((msq_data1['metcode'] == msq_data1['metcode'].shift(1)) & (msq_data1['subid'] == msq_data1['subid'].shift(1)), msq_data1['sqocc'] - msq_data1['sqocc'].shift(1), np.nan)
            msq_data1['sqvac_chg'] = np.where((msq_data1['metcode'] == msq_data1['metcode'].shift(1)) & (msq_data1['subid'] == msq_data1['subid'].shift(1)), msq_data1['sqvac'] - msq_data1['sqvac'].shift(1), np.nan)
            msq_data1['sq_Gmrent'] = np.where((msq_data1['metcode'] == msq_data1['metcode'].shift(1)) & (msq_data1['subid'] == msq_data1['subid'].shift(1)), (msq_data1['sqsren'] - msq_data1['sqsren'].shift(1)) / msq_data1['sqsren'].shift(1), np.nan)
        msq_data1['sq_Gmrent'] = round( msq_data1['sq_Gmrent'],3)
        msq_data1['sqsren'] = round(msq_data1['sqsren'], 2)
        if sector_val == "ind":
            msq_data1 = msq_data1[['join_ident','metcode', 'subid', 'yr', 'qtr', 'currmon', 'type2', 'sqinv', 'sqavail', 'sqvac', 'sqvac_chg', 'sqocc', 'sqabs', 'sqsren', 'sq_Gmrent']]
        elif sector_val == "ret":
            msq_data1 = msq_data1[['join_ident','metcode', 'subid', 'yr', 'qtr', 'currmon', 'type1', 'sqinv', 'sqavail', 'sqvac', 'sqvac_chg', 'sqocc', 'sqabs', 'sqsren', 'sq_Gmrent']]
        else:
            msq_data1 = msq_data1[['join_ident','metcode', 'subid', 'yr', 'qtr', 'currmon', 'sqinv', 'sqavail', 'sqvac', 'sqvac_chg', 'sqocc', 'sqabs', 'sqsren', 'sq_Gmrent']]
        
        msq_data = msq_data.join(msq_data1.set_index('join_ident').drop(drop_list, axis=1), on='join_ident')
        del msq_data1
        gc.collect()
        
        # Note: In an effort to align the sq cons data with the trend data, will move away from the method of only reporting square rollup in the third month of the quarter for periods that do not have a monthly breakout in the msq
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_msq_data.pickle".format(get_home(), sector_val))
        msq_data3 = pd.read_pickle(file_path)
        
        if sector_val == "ind":
            msq_data3['join_ident'] = msq_data3['metcode'] + msq_data3['subid'].astype(str) + msq_data3['type2'] + msq_data3['yr'].astype(str) + msq_data3['qtr'].astype(str) + msq_data3['currmon'].astype(str)
            msq_data3.sort_values(by=['type2', 'metcode', 'subid', 'yr', 'qtr', 'currmon'], inplace=True)
        elif sector_val == "ret":
            msq_data3['join_ident'] = msq_data3['metcode'] + msq_data3['subid'].astype(str) + msq_data3['type1'] + msq_data3['yr'].astype(str) + msq_data3['qtr'].astype(str) + msq_data3['currmon'].astype(str)
            msq_data3.sort_values(by=['type1', 'metcode', 'subid', 'yr', 'qtr', 'currmon'], inplace=True)
        else:
            msq_data3['join_ident'] = msq_data3['metcode'] + msq_data3['subid'].astype(str) + msq_data3['yr'].astype(str) + msq_data3['qtr'].astype(str) + msq_data3['currmon'].astype(str)
            msq_data3.sort_values(by=['metcode', 'subid', 'yr', 'qtr', 'currmon'], inplace=True)
        msq_data3 = msq_data3[((msq_data3['yearx'] > 2008) | ((msq_data3['yearx'] == 2008) & (msq_data3['month'] >= 10)))]
        msq_data3['qtr'] = np.where(msq_data3['month'] <= 3, 1, 4)
        msq_data3['qtr'] = np.where((msq_data3['month'] <= 6) & (msq_data3['month'] >= 4), 2, msq_data3['qtr'])
        msq_data3['qtr'] = np.where((msq_data3['month'] <= 9) & (msq_data3['month'] >= 7), 3, msq_data3['qtr'])
        msq_data3['qtr'] = np.where((np.isnan(msq_data3['month']) == True), 2, msq_data3['qtr'])

        msq_data3['month'] = np.where(np.isnan(msq_data3['month']) == True, 6, msq_data3['month'])
        msq_data3['yearx'] = msq_data3['yearx'].astype(int)
        msq_data3['month'] = msq_data3['month'].astype(int)

        if sector_val == "ind":
            msq_data3['join_ident'] = msq_data3['metcode'] + msq_data3['subid'].astype(str) + msq_data3['type2'] + msq_data3['yearx'].astype(str) + msq_data3['qtr'].astype(str) + msq_data3['month'].astype(str)
        elif sector_val == "ret":
            msq_data3['join_ident'] = msq_data3['metcode'] + msq_data3['subid'].astype(str) + msq_data3['type1'] + msq_data3['yearx'].astype(str) + msq_data3['qtr'].astype(str) + msq_data3['month'].astype(str)
        else:
            msq_data3['join_ident'] = msq_data3['metcode'] + msq_data3['subid'].astype(str) + msq_data3['yearx'].astype(str) + msq_data3['qtr'].astype(str) + msq_data3['month'].astype(str)
        msq_data3 = msq_data3.drop_duplicates('id')
        msq_data3['sqcons'] = msq_data3.groupby('join_ident')['sizex'].transform('sum')
        msq_data3['sqcons'] = msq_data3['sqcons'].fillna(0)
        msq_data3.sort_values(by=['yr', 'qtr', 'currmon'], ascending=[True, True, False], inplace=True)
        msq_data3 = msq_data3.drop_duplicates('join_ident')
        if sector_val == "ind":
            msq_data3 = msq_data3[['join_ident','metcode', 'subid', 'yr', 'qtr', 'currmon', 'type2', 'sqcons']]
        elif sector_val == "ret":
            msq_data3 = msq_data3[['join_ident','metcode', 'subid', 'yr', 'qtr', 'currmon', 'type1', 'sqcons']]
        else:
            msq_data3 = msq_data3[['join_ident', 'metcode', 'subid', 'yr', 'qtr', 'currmon', 'sqcons']]

        msq_data = msq_data.join(msq_data3.set_index('join_ident').drop(drop_list, axis=1), on='join_ident')
        del msq_data3
        gc.collect()
        
        msq_data = msq_data.set_index('join_ident')
        msq_data['sqcons'] = msq_data['sqcons'].fillna(0)
        if sector_val != "apt":
            msq_data['sqcons'] = round(msq_data['sqcons'], -3)

        if sector_val == "ind":
            msq_data.sort_values(by=['subsector', 'metcode', 'subid', 'yr', 'qtr', 'currmon'], ascending=[True, True, True, True, True, True], inplace=True)
        else:
            msq_data.sort_values(by=['metcode', 'subid', 'yr', 'qtr', 'currmon'], ascending=[True, True, True, True, True], inplace=True)
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_sq_substats.pickle".format(get_home(), sector_val))
        msq_data.to_pickle(file_path)
        del msq_data
        gc.collect()
        
    # Join in the most up to date sq met rollups
    if sector_val == "ind":
        data['join_ident'] = data['metcode'] + data['subsector'] + data['yr'].astype(str) + data['qtr'].astype(str) + data['currmon'].astype(str)
    else:
        data['join_ident'] = data['metcode'] + data['yr'].astype(str) + data['qtr'].astype(str) + data['currmon'].astype(str)
    file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_sq_metstats.pickle".format(get_home(), sector_val))
    msq_data_met = pd.read_pickle(file_path)
    msq_data_met = msq_data_met.drop(['metcode', 'yr', 'qtr', 'currmon'], axis=1)
    if sector_val == "ind":
        msq_data_met = msq_data_met.drop(['subsector'], axis=1)
    data = data.join(msq_data_met, on='join_ident')
    data = data.drop(['join_ident'], axis=1)
    
    # Join in the most up to date sq sub rollups
    if sector_val == "ind" or sector_val == "ret":
        data['join_ident'] = data['metcode'] + data['subid'].astype(str) + data['subsector'] + data['yr'].astype(str) + data['qtr'].astype(str) + data['currmon'].astype(str)
    else:
        data['join_ident'] = data['metcode'] + data['subid'].astype(str) + data['yr'].astype(str) + data['qtr'].astype(str) + data['currmon'].astype(str)
    file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_sq_substats.pickle".format(get_home(), sector_val))
    msq_data_sub = pd.read_pickle(file_path)
    msq_data_sub = msq_data_sub.drop(['metcode', 'subid', 'yr', 'qtr', 'currmon'], axis=1)
    if sector_val == "ind" or sector_val == "ret":
        msq_data_sub = msq_data_sub.drop(['subsector'], axis=1)

    data = data.join(msq_data_sub, on='join_ident')
    data = data.drop(['join_ident'], axis=1)

    # Join in the most up to date sq insight stats (although this will only be refreshed if the actual sqinsight.do file is run after making edits to the msqs)
    cols_before_join = list(data.columns)

    file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_sq_insight_met_rent.pickle".format(get_home(), sector_val))
    sq_insight_met_rent = pd.read_pickle(file_path)
    file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_sq_insight_sub_rent.pickle".format(get_home(), sector_val))
    sq_insight_sub_rent = pd.read_pickle(file_path)
    file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_sq_insight_met_vac.pickle".format(get_home(), sector_val))
    sq_insight_met_vac = pd.read_pickle(file_path)
    file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_sq_insight_sub_vac.pickle".format(get_home(), sector_val))
    sq_insight_sub_vac = pd.read_pickle(file_path)

    if sector_val == "ret":
        insight_NC = data.copy()
        insight_NC = insight_NC[insight_NC['curr_tag'] == 1]
        insight_NC['met_sqinv'] = insight_NC.groupby(['metcode'])['sqinv'].transform('sum')
        insight_NC['sub_sqinv'] = insight_NC.groupby(['metcode', 'subid'])['sqinv'].transform('sum')
        insight_NC = insight_NC.drop_duplicates(['metcode', 'subid'])
        insight_NC = insight_NC[['metcode', 'subid', 'met_sqinv', 'sub_sqinv']]
        insight_NC['identity'] = insight_NC['metcode'] + insight_NC['subid'].astype(str) + "NC"
        insight_NC['identity_met'] = insight_NC['metcode'] + "NC"
        insight_NC['subsector'] = "NC"
        insight_NC = insight_NC.set_index('identity_met')
        insight_NC = insight_NC.join(sq_insight_met_rent, on='identity_met')
        insight_NC = insight_NC.join(sq_insight_met_vac, on='identity_met')
        insight_NC.reset_index().set_index('identity')
        insight_NC = insight_NC.join(sq_insight_sub_rent, on='identity')
        insight_NC = insight_NC.join(sq_insight_sub_vac, on='identity')
        insight_NC['met_sur_r_cov_perc'] = insight_NC['met_rntchginv'] / insight_NC['met_sqinv']
        insight_NC['sub_sur_r_cov_perc'] = insight_NC['sub_rntchginv'] / insight_NC['sub_sqinv']
        insight_NC['met_sur_v_cov_perc'] = insight_NC['met_vacchginv'] / insight_NC['met_sqinv']
        insight_NC['sub_sur_v_cov_perc'] = insight_NC['sub_vacchginv'] / insight_NC['sub_sqinv']
        insight_NC = insight_NC.drop(['met_sqinv', 'sub_sqinv'], axis=1)
        insight_NC = insight_NC.rename(columns={'sub_rentdrops': 'rentdrops', 'sub_rentflats': 'rentflats', 'sub_rentincrs': 'rentincrs', 'sub_vacdrops': 'vacdrops', 'sub_vacflats': 'vacflats', 'sub_vacincrs': 'vacincrs'})
        insight_NC = insight_NC.rename(columns={'met_totabs': 'met_sur_totabs', 'sub_totabs': 'sub_sur_totabs'})
        insight_NC[['met_rntchginv', 'sub_rntchginv', 'met_vacchginv', 'sub_vacchginv']] = insight_NC[['met_rntchginv', 'sub_rntchginv', 'met_vacchginv', 'sub_vacchginv']].fillna(0)
        insight_NC = insight_NC.drop(['us_avg_mos_to_last_rensur', 'us_rntchginv', 'us_g_renx_mo_wgt', 'us_avg_mos_to_last_vacsur', 'us_vacchginv'], axis=1)
    
    if sector_val == "ret":
        data['identity_met'] = data['metcode'] + data['subsector']
        data = data.join(sq_insight_met_rent, on='identity_met')
        data['identity_met'] = data['metcode'] + "Ret"
    else:
        data = data.join(sq_insight_met_rent, on='identity_met')
    data = data.join(sq_insight_sub_rent, on='identity')
    if sector_val == "ret":
        data['identity_met'] = data['metcode'] + data['subsector']
        data = data.join(sq_insight_met_vac, on='identity_met')
        data['identity_met'] = data['metcode'] + "Ret"
    else:
        data = data.join(sq_insight_met_vac, on='identity_met')
    data = data.join(sq_insight_sub_vac, on='identity')

    del sq_insight_met_rent
    del sq_insight_sub_rent
    del sq_insight_met_vac
    del sq_insight_sub_vac
    gc.collect()

    data = data.rename(columns={'sub_rentdrops': 'rentdrops', 'sub_rentflats': 'rentflats', 'sub_rentincrs': 'rentincrs', 'sub_vacdrops': 'vacdrops', 'sub_vacflats': 'vacflats', 'sub_vacincrs': 'vacincrs'})
    data = data.rename(columns={'met_totabs': 'met_sur_totabs', 'sub_totabs': 'sub_sur_totabs'})

    if sector_val == "ret":
        cols_after_join = list(data.columns)
        insight_cols = list(np.setdiff1d(cols_after_join, cols_before_join))
        insight_cols.remove('sub_rntchginv')
        insight_cols.remove('us_rntchginv')
        insight_cols.remove('sub_vacchginv')
        insight_cols.remove('us_vacchginv')

    data[['met_rntchginv', 'sub_rntchginv', 'met_vacchginv', 'sub_vacchginv']] = data[['met_rntchginv', 'sub_rntchginv', 'met_vacchginv', 'sub_vacchginv']].fillna(0)

    if sector_val == "ret":
        data['met_sqinv'] = data[data['curr_tag'] == 1].groupby('subsector')['sqinv'].transform('sum')
    else:
        data['met_sqinv'] = data[data['curr_tag'] == 1].groupby('identity_met')['sqinv'].transform('sum')
    data['sub_sqinv'] = data[data['curr_tag'] == 1].groupby('identity')['sqinv'].transform('sum')
    data['us_sqinv'] = data[data['curr_tag'] == 1].groupby('subsector')['sqinv'].transform('sum')

    data['met_sur_r_cov_perc'] = data['met_rntchginv'] / data['met_sqinv']
    data['sub_sur_r_cov_perc'] = data['sub_rntchginv'] / data['sub_sqinv']
    data['us_sur_r_cov_perc'] = data['us_rntchginv'] / data['us_sqinv']
    data['met_sur_v_cov_perc'] = data['met_vacchginv'] / data['met_sqinv']
    data['sub_sur_v_cov_perc'] = data['sub_vacchginv'] / data['sub_sqinv']
    data['us_sur_v_cov_perc'] = data['us_vacchginv'] / data['us_sqinv']
    
    data = data.drop(['sub_rntchginv', 'us_rntchginv', 'sub_vacchginv', 'us_vacchginv', 'met_sqinv', 'sub_sqinv', 'us_sqinv'], axis=1)

    # Fill in a zero for vars that are blank and should be zero
    data[['rentdrops', 'rentflats', 'rentincrs', 'vacdrops', 'vacflats', 'vacincrs', 'gmrent_roldiff', 'covren', 'covvac']] = data[['rentdrops', 'rentflats', 'rentincrs', 'vacdrops', 'vacflats', 'vacincrs', 'gmrent_roldiff', 'covren', 'covvac']].fillna(0)
    
    if sector_val == "apt":
        a_round_val = 0
    else:
        a_round_val = -3
    
    data['met_sur_totabs'] = round(data['met_sur_totabs'], a_round_val)
    data['sub_sur_totabs'] = round(data['sub_sur_totabs'], a_round_val)
    data['newncava'] = round(data['newncava'], a_round_val)
    data['newnc_thismo'] = round(data['newnc_thismo'], a_round_val)
    data['newncsf'] = round(data['newncsf'], a_round_val)

    # DC wants to see C, N and NC cuts regardless of subsector in the key metrics view, so will create flat file with those breakouts so it can be loaded in and displayed
    # Want to reduce the number of cols being carried around, and there could be some confusion with column names, so this is the best choice I think
    # Also save the NC stats file, as those will be used for the met packet, not the individual subsectors
    if sector_val == "ret":
        insight = data.copy()
        insight = insight[insight['curr_tag'] == 1]
        cols_to_keep = ['subsector', 'metcode', 'subid', 'met_sur_r_cov_perc', 'sub_sur_r_cov_perc', 'met_sur_v_cov_perc', 'sub_sur_v_cov_perc'] + insight_cols
        insight = insight[cols_to_keep]

        def split_insight(dataframe_in, prefix):
            dataframe = dataframe_in.copy()
            dataframe['identity'] = dataframe['metcode'] + dataframe['subid'].astype(str)
            dataframe = dataframe[dataframe['subsector'] == prefix]
            dataframe = dataframe.drop(['subsector', 'metcode', 'subid'], axis=1)
            dataframe = dataframe.set_index('identity')

            for x in list(dataframe.columns):
                dataframe.rename(columns={x: prefix.lower() + "_" + x}, inplace=True)

            return dataframe

        insight_c = split_insight(insight, "C")
        insight_n = split_insight(insight, "N")
        insight_nc = split_insight(insight_NC, "NC")
        insight_combined = insight_nc.join(insight_c)
        insight_combined = insight_combined.join(insight_n)

        
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/ret_nc_insight_formetpacket.pickle".format(get_home()))
        insight_nc.to_pickle(file_path)

        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/ret_combined_insight_data.pickle".format(get_home()))
        insight_combined.to_pickle(file_path)

        del insight_NC
        del insight_c
        del insight_n
        del insight_nc
        del insight_combined
        del insight
        gc.collect()

    # Fill in nans for the first period abs in 2020q2 new subs
    data['abs'] = np.where((data['yr'] == 2019) & (data['currmon'] == 1) & (data['identity'] != data['identity'].shift(1)), np.nan, data['abs'])

    # Use last months final msq to determine what ids are new to the sq pool this month, and are in the nc rebench window
    file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_msq_data.pickle".format(get_home(), sector_val))
    curr = pd.read_pickle(file_path)
    curr = curr[(curr['yr'] == curryr) & (curr['currmon'] == currmon)]
    curr = curr[curr['yearx'] >= curryr - 3]
    curr = curr[curr['balance_test'] != '99']
    curr = curr[['id', 'yr', 'qtr', 'currmon', 'yearx', 'month', 'sizex', 'totavailx', 'renx', 'identity']]
    curr['currmon_tag'] = np.where((curr['yearx'] == curryr) & (curr['month'] == currmon), 1, 0)
    
    prior = pd.read_pickle("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_msq_data_prior_month.pickle".format(get_home(), sector_val))
    prior = prior[['id']]
    if isinstance(curr.reset_index().loc[0]['id'], str) and not isinstance(prior.reset_index().loc[0]['id'], str):
        prior['id'] = prior['id'].astype(str).str.split('.').str[0]
    elif not isinstance(curr.reset_index().loc[0]['id'], str) and isinstance(prior.reset_index().loc[0]['id'], str):
        prior['id'] = prior['id'].astype(int)
    prior = prior.drop_duplicates('id')
    prior['in_last_month'] = 1
    prior = prior.set_index('id')
    curr = curr.join(prior, on='id')
    del prior
    curr = curr[curr['in_last_month'].isnull() == True]
    curr = curr[curr['renx'].isnull() == False]
    curr.sort_values(by=['id', 'yr', 'qtr', 'currmon'], ascending=[True, True, True, True], inplace=True)
    curr = curr.drop_duplicates('id')
    new_ids = list(curr['id'])
    newnc_dict = {}
    for index, row in curr.iterrows():
        newnc_dict[row['identity'] + "," + str(int(row['id']))] = {'id': row['id'], 'yearx': row['yearx'], 'month': row['month'], 
                                                                    'sizex': row['sizex'], 'totavailx': row['totavailx'], 'renx': row['renx']}
    del curr
    gc.collect()

    # Use the MSQ data set to calculate the total surveyed abs from NC properties in currmon for use in flags
    file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_msq_data.pickle".format(get_home(), sector_val))
    data_surabs = pd.read_pickle(file_path)
    data_surabs['surveyed'] = np.where(data_surabs['availxM'] == 0, 1, 0)
    data_surabs['count_survs'] = data_surabs.groupby('id')['surveyed'].transform('sum')
    data_surabs['nc_first_surv'] = np.where((data_surabs['yr'] == curryr) & (data_surabs['currmon'] == currmon) & (data_surabs['count_survs'] == 1) & (data_surabs['availxM'] == 0) & (data_surabs['yearx'] >= curryr - 3), 1, 0)
    data_surabs['new_to_sq'] = np.where(data_surabs['id'].isin(new_ids), 1, 0)
    data_surabs['currmon_abs'] = np.where((data_surabs['yr'] == curryr) & (data_surabs['currmon'] == currmon) & (data_surabs['new_to_sq'] == 1) & (data_surabs['totavailx'].shift(1).isnull() == False) & (data_surabs['id'] == data_surabs['id'].shift(1)), data_surabs['totavailx'].shift(1) - data_surabs['totavailx'], np.nan)
    data_surabs = data_surabs[(data_surabs['yr'] == curryr) & (data_surabs['currmon'] == currmon)]
    data_surabs = data_surabs[((data_surabs['yearx'] == curryr) & (data_surabs['month'] == currmon)) | (data_surabs['nc_first_surv'] == 1)]
    data_surabs['nc_surabs'] = np.where((data_surabs['availxM'] == 0), data_surabs['sizex'] - data_surabs['totavailx'], 0)
    data_surabs['nc_surabs'] = np.where((data_surabs['availxM'] == 0) & (data_surabs['new_to_sq'] == 1) & (data_surabs['currmon_abs'].isnull() == False), data_surabs['currmon_abs'], data_surabs['nc_surabs'])
    data_surabs['nc_surabs'] = np.where((data_surabs['new_to_sq'] == 1) & (data_surabs['nc_surabs'] < 0), 0, data_surabs['nc_surabs'])

    data_surabs['sum_nc_surabs'] = data_surabs.groupby('identity')['nc_surabs'].transform('sum')
    data_surabs_all = data_surabs.copy()
    data_surabs = data_surabs[['identity', 'sum_nc_surabs']]
    data_surabs = data_surabs.rename(columns={'sum_nc_surabs': 'nc_surabs'})
    data_surabs = data_surabs.drop_duplicates('identity')
    
    if sector_val == "apt":
        data_surabs['nc_surabs'] = round(data_surabs['nc_surabs'], 0)
    else:
        data_surabs['nc_surabs'] = round(data_surabs['nc_surabs'], -3)
    data_surabs = data_surabs.set_index('identity')
    data = data.join(data_surabs, on='identity')
    del data_surabs
    gc.collect()
    data['nc_surabs'] = data['nc_surabs'].fillna(0)
    if sector_val == "ind":
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/OutputFiles/nc_surabs.csv".format(get_home(), sector_val, curryr, currmon))
        data[data['nc_surabs'] != 0].sort_values(by=['identity', 'yr', 'currmon'], ascending=[True, False, False]).drop_duplicates('identity')[['identity', 'nc_surabs']].to_csv(file_path)

    # Get the ids that have greater than zero nc surabs, for display in tooltip for key metrics table
    nc_sur_props = data_surabs_all.copy()
    del data_surabs_all
    gc.collect()
    nc_sur_props = nc_sur_props[nc_sur_props['nc_surabs'] > 0]
    nc_sur_props = nc_sur_props[['id', 'identity', 'nc_surabs', 'yearx', 'month']]
    ncsur_prop_dict = {}
    for index, row in nc_sur_props.iterrows():
        ncsur_prop_dict[row['identity'] + "," + str(int(row['id']))] = {'id': row['id'], 'nc_surabs': row['nc_surabs'], 'yearx': row['yearx'], 'month': row['month']}

    del nc_sur_props
    gc.collect()

    # Use the MSQ data set to calculate the top ids for total surveyed abs for the 10 pool (surveyed this month, squared last month) in currmon for display in tooltip for key metrics table
    file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_msq_data.pickle".format(get_home(), sector_val))
    sur_avail = pd.read_pickle(file_path)
    sur_avail['10_tag'] = np.where((sur_avail['availxM'] == 0) & (sur_avail['availxM'].shift(1) == 1) & (sur_avail['id'] == sur_avail['id'].shift(1)) & (sur_avail['yr'] == curryr) & (sur_avail['currmon'] == currmon), 1, 0)
    sur_avail = sur_avail[(sur_avail['10_tag'] == 1) | (sur_avail['10_tag'].shift(-1) == 1)]
    sur_avail['abs'] = sur_avail['totavailx'].shift(1) - sur_avail['totavailx']
    sur_avail = sur_avail[(sur_avail['yr'] == curryr) & (sur_avail['currmon'] == currmon)]
    sur_avail[abs(sur_avail['abs']) > 0]
    sur_avail.sort_values(by=['abs'], ascending=[False], key=abs, inplace=True)
    sur_avail = sur_avail.groupby('identity').head(5).reset_index(drop=True)
    sur_avail = sur_avail[['id', 'identity', 'abs']]
    avail_10_dict = {}
    for index, row in sur_avail.iterrows():
        avail_10_dict[row['identity'] + "," + str(int(row['id']))] = {'id': row['id'], 'abs': row['abs']}

    del sur_avail
    gc.collect()

    # Use the MSQ data set to calculate the top ids for rent chg regardless of survey status in currmon for display in tooltip for key metrics table
    file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_msq_data.pickle".format(get_home(), sector_val))
    sq_avail = pd.read_pickle(file_path)
    sq_avail = sq_avail[((sq_avail['yr'] == curryr) & (sq_avail['currmon'] == currmon)) | ((sq_avail['yr'].shift(-1) == curryr) & (sq_avail['currmon'].shift(-1) == currmon))]
    sq_avail['abs'] = np.where(sq_avail['id'] == sq_avail['id'].shift(1), sq_avail['totavailx'].shift(1) - sq_avail['totavailx'], np.nan)
    sq_avail = sq_avail[(sq_avail['yr'] == curryr) & (sq_avail['currmon'] == currmon)]
    sq_avail = sq_avail[['id', 'identity', 'abs', 'availxM']]
    sq_avail = sq_avail[abs(sq_avail['abs']) > 0]
    sq_avail.sort_values(by=['abs'], ascending=[False], key=abs, inplace=True)
    sq_avail = sq_avail.groupby('identity').head(5).reset_index(drop=True)
    sq_avail_dict = {}
    for index, row in sq_avail.iterrows():
        sq_avail_dict[row['identity'] + "," + str(int(row['id']))] = {'id': row['id'], 'abs': row['abs'], 'availxM': row['availxM']}
    
    del sq_avail
    gc.collect()

    # Use the MSQ data set to calculate the top ids for rent chg for the 10 pool (surveyed this month, squared last month) in currmon for display in tooltip for key metrics table
    file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_msq_data.pickle".format(get_home(), sector_val))
    sur_rg = pd.read_pickle(file_path)
    sur_rg['10_tag'] = np.where((sur_rg['renxM'] == 0) & (sur_rg['renxM'].shift(1) == 1) & (sur_rg['id'] == sur_rg['id'].shift(1)) & (sur_rg['yr'] == curryr) & (sur_rg['currmon'] == currmon), 1, 0)
    sur_rg = sur_rg[(sur_rg['10_tag'] == 1) | (sur_rg['10_tag'].shift(-1) == 1)]
    sur_rg['rg'] = (sur_rg['renx'] - sur_rg['renx'].shift(1)) / sur_rg['renx'].shift(1)
    sur_rg = sur_rg[(sur_rg['yr'] == curryr) & (sur_rg['currmon'] == currmon)]
    sur_rg['rg'] = round(sur_rg['rg'], 3)
    sur_rg = sur_rg[abs(sur_rg['rg']) > 0]
    sur_rg.sort_values(by=['rg'], ascending=[False], key=abs, inplace=True)
    sur_rg = sur_rg.groupby('identity').head(5).reset_index(drop=True)
    sur_rg = sur_rg[['id', 'identity', 'rg']]
    rg_10_dict = {}
    for index, row in sur_rg.iterrows():
        rg_10_dict[row['identity'] + "," + str(int(row['id']))] = {'id': row['id'], 'rg': row['rg']}

    del sur_rg
    gc.collect()

    # Use the MSQ data set to calculate the top ids for rent chg regardless of survey status in currmon for display in tooltip for key metrics table
    file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_msq_data.pickle".format(get_home(), sector_val))
    sq_rg = pd.read_pickle(file_path)
    sq_rg = sq_rg[((sq_rg['yr'] == curryr) & (sq_rg['currmon'] == currmon)) | ((sq_rg['yr'].shift(-1) == curryr) & (sq_rg['currmon'].shift(-1) == currmon))]
    sq_rg['rg'] = np.where(sq_rg['id'] == sq_rg['id'].shift(1), (sq_rg['renx'] - sq_rg['renx'].shift(1)) / sq_rg['renx'].shift(1), np.nan)
    sq_rg = sq_rg[(sq_rg['yr'] == curryr) & (sq_rg['currmon'] == currmon)]
    sq_rg = sq_rg[['id', 'identity', 'rg', 'renxM']]
    sq_rg['rg'] = round(sq_rg['rg'], 3)
    sq_rg = sq_rg[abs(sq_rg['rg']) > 0]
    sq_rg.sort_values(by=['rg'], ascending=[False], key=abs, inplace=True)
    sq_rg = sq_rg.groupby('identity').head(5).reset_index(drop=True)
    sq_rg_dict = {}
    for index, row in sq_rg.iterrows():
        sq_rg_dict[row['identity'] + "," + str(int(row['id']))] = {'id': row['id'], 'rg': row['rg'], 'renxM': row['renxM']}

    del sq_rg
    gc.collect()
    
    return data, orig_cols, ncsur_prop_dict, avail_10_dict, sq_avail_dict, rg_10_dict, sq_rg_dict, newnc_dict