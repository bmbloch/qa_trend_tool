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

from trend_process_sq_insight import process_sq_insight

def get_home():
    if os.name == "nt": return "//odin/reisadmin/"
    else: return "/home/"

def initial_load(sector_val, curryr, currmon, msq_load):

    if currmon < 4:
        currqtr = 1
    elif currmon < 7:
        currqtr = 2
    elif currmon < 10: 
        currqtr = 3
    else:
        currqtr = 4

    # Determine if the user wants to load a trunc dataset for faster processing during testing
    get_args = sys.argv
    if len(get_args) > 1:
        load_trunc = True
    else:
        load_trunc = False

    # Load the input file - if this is the first time the program is run, the oob data should be loaded in, and if this is not the first time, then the edits data should be loaded in
    try:
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/OutputFiles/{}_mostrecentsave.pickle".format(get_home(), sector_val, str(curryr), str(currmon), sector_val))
        data = pd.read_pickle(file_path)
        file_used = "edits"
        file_load_error = False
        print("Using Saved File")
    except:
        file_used = "oob"
    if file_used == "oob":
        try:
            if sector_val == "ind":
                frames = []
                for subsector in ["DW", "F"]:
                    file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/InputFiles/indsub_{}_{}m{}-ysis.csv".format(get_home(), sector_val, str(curryr), str(currmon), subsector, str(curryr), str(currmon)))
                    if load_trunc == True:
                        if subsector == "DW":
                            nrows = 3393
                        elif subsector == "F":
                            nrows = 2584
                        data = pd.read_csv(file_path, encoding = 'utf-8',  na_values= "", keep_default_na = False, nrows=nrows)
                    else:
                        data = pd.read_csv(file_path, encoding = 'utf-8',  na_values= "", keep_default_na = False)
                    frames.append(data)
                data = frames[0].append(frames[1], ignore_index=True)
            elif sector_val == "apt" or sector_val == "off":
                file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/InputFiles/{}sub_{}m{}-ysis.csv".format(get_home(), sector_val, str(curryr), str(currmon), sector_val, str(curryr), str(currmon)))
                data = pd.read_csv(file_path, encoding = 'utf-8',  na_values= "", keep_default_na = False)
                cols = list(data.columns)
                data['subsector'] = sector_val.title()
                new_cols = ['subsector'] + cols
                data = data[new_cols]
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
                    past_data['currmon'] = np.where((np.isnan(past_data['currmon'])== True), 13, past_data['currmon'])
                    past_data['subsector'] = "Apt"
                    past_data['subid'] = past_data['subid'].astype(int)
                    past_data['yr'] = past_data['yr'].astype(int)
                    past_data['currmon'] = past_data['currmon'].astype(int)
                    past_data['identity_row'] = np.where((past_data['currmon'] != 13), past_data['metcode'] + past_data['subid'].astype(str) + past_data['subsector'] + past_data['yr'].astype(str) + past_data['currmon'].astype(str), past_data['metcode'] + past_data['subid'].astype(str) + past_data['subsector'] + past_data['yr'].astype(str) + past_data['qtr'].astype(str))
                    past_data = past_data.set_index('identity_row')
                    past_data['p_sqcons'] = past_data['sqcons']
                    past_data = past_data[['p_sqcons']]
            elif sector_val == "ret":
                frames = []
                for subsector in ['C', 'N', "NC"]:
                    if subsector == "C" or subsector == "N":
                        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/InputFiles/retsub_{}_{}m{}-ysis.csv".format(get_home(), sector_val, str(curryr), str(currmon), subsector, str(curryr), str(currmon)))
                    else:
                        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/{}/{}m{}/InputFiles/retsub_{}_{}m{}-tier3-ysis.csv".format(get_home(), sector_val, str(curryr), str(currmon), subsector, str(curryr), str(currmon)))
                    data = pd.read_csv(file_path, encoding = 'utf-8',  na_values= "", keep_default_na = False)
                    frames.append(data)
                data = frames[0].append(frames[1], ignore_index=True)
                data = data.append(frames[2], ignore_index=True)
            file_load_error = False
        except:
            file_load_error = True

    # If the file loaded succesfully, continue with data processing
    if file_load_error == False:
        # We need to fill the currmon column with a value, as having nans here will cause issues. Replace with a dummy variable        
        data['currmon'] = np.where((np.isnan(data['currmon']) == True), 13, data['currmon'])

        # It seems as though the columns for subid yr, and currmon are sometimes read in as floats, so convert them to ints explicitly here
        data['subid'] = data['subid'].astype(int)
        data['yr'] = data['yr'].astype(int)
        data['currmon'] = data['currmon'].astype(int)

        # Drop extra columns that the program wont use if this is the first run of the month - helps reduce dimensionality and size of dataframe
        if file_used == "oob":
            data = data.drop(['sector', 'dbtime', 'dqinvav11', 'avail11d', 'dqinvav10', 'dqinvav00', 'dqinvav01', 'avail01d', 'dqinvren11', 
            'dqren11d', 'dqinvren00', 'dqinvren01', 'dqren01d', 'ncreninv', 'pastsubsqinv', 'invdiffpast', 'vacdiffpast', 
            'rntdiffpast', 'sqcons_full', 'gap_door1', 'gap_door2', 'gap_door3', 'prelimtime', 'p_askrev', 'p_effrev', 'shimtime', 'inv_shim', 'cons_shim', 
            'avail_shim', 'mrent_shim', 'merent_shim', 'finals_time', 'askrev', 'effrev', 'actualsurveytime', 'rentdrops', 'rentflats', 'rentincrs', 'avg_rentchg', 'avg_rentchg_mo', 'avg_mos_to_last_rensur', 
            'stddev_avg_rentchg', 'MET_avg_rentchg', 'MET_avg_rentchg_mo', 'MET_avg_mos_to_last_rensur', 'MET_stddev_avg_rentchg', 'US_avg_rentchg', 'US_avg_rentchg_mo', 
            'US_avg_mos_to_last_rensur', 'audit', 'inv_rol_isdiff', 'cons_rol_isdiff', 'vac_rol_isdiff', 'gmrent_rol_isdiff', 'inv_roldiff'], axis=1)

            if sector_val == "apt" or sector_val == "off" or sector_val == "ret":
                data = data.drop(['conv_shim', 'demo_shim'], axis=1)

            if sector_val == "ind":
                data = data.drop(['trend_yr1'], axis=1)

            # Convert surveyed avail vars to the consistent sign with abs in pub, and round to thousandths
            if sector_val != "apt":
                data[['avail10d', 'avail00d']] = round((data[['avail10d', 'avail00d']] * -1), -3)
            else:
                data[['avail10d', 'avail00d']] = round((data[['avail10d', 'avail00d']] * -1), 0)

            # Create a column that will keep track if the user overrides a flag and does not think a change is warranted
            data['flag_skip'] = ''

            # Rename the p columns with the oob prefix for consistency with forecast program
            for x in list(data.columns):
                if x[0:2] == "p_" and x != "p_sqcons":
                    data = data.rename(columns={x:  x[2:] + "_oob"})
            data['vac_oob'] = data['vac']
            data['vac_chg_oob'] = data['vac_chg']

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
            data = data.join(past_data)

        # Load the currmon row of all msqs to calculate extra data points and also the rollups
        # This takes  awhile (loading stata dtas is slow) so will give the analyst the option to only refresh if there was an actual edit made to the msqs
        if msq_load == "Y" or (file_used == "oob" and load_trunc == False):
            print("start load msqs")
            data_in = pd.DataFrame()
            pathlist = Path("{}central/square/data/{}/production/msq/output".format(get_home(), sector_val)).glob('**/*.dta')
            r = re.compile("..msq\.dta")
            for path in pathlist:
                path_in_str = str(path)
                testing = path_in_str[-9:]
                testing_folder = path_in_str[-16:-10]
                modified = os.path.getmtime(path)
                m_year, m_month, m_day = time.localtime(modified)[:-6]
                if r.match(testing) != None and testing_folder == "output" and (m_year > curryr or (m_month >= currmon and m_year == curryr)):
                    cols_to_keep = ['id', 'yr', 'qtr', 'currmon', 'yearx', 'month', 'metcode', 'subid', 'submkt', 'availxM', 'existsx']
                    if sector_val == "apt":
                        test = pd.read_stata(path_in_str, columns= cols_to_keep + ['availx', 'totunitx', 'avgrenx', 'avgrenxM'])
                    elif sector_val == "ind":
                        test = pd.read_stata(path_in_str, columns= cols_to_keep + ['type2', 'totavailx', 'ind_size', 'renx', 'renxM'])
                    elif sector_val == "off":
                        test = pd.read_stata(path_in_str, columns=cols_to_keep + ['type2', 'totavailx', 'sizex', 'renx', 'renxM'])
                    elif sector_val == "ret":
                        test = pd.read_stata(path_in_str, columns=cols_to_keep + ['type1', 'availx', 'sizex', 'nsizex', 'renx', 'renxM', 'nrenx', 'nrenxM'])
                    # Keep only relevant periods and rows that the property was in existance for
                    test = test[((test['yr'] > 2008) | ((test['yr'] == 2008) & (test['qtr'] == 4)))]
                    test = test[test['existsx'] == 1]
                    data_in = data_in.append(test)
            print("end load msqs")
            if sector_val == "apt":
                data_in = data_in.rename(columns={'totunitx': 'sizex', 'availx': 'totavailx', 'avgrenx': 'renx', 'avgrenxM': 'renxM'})
            elif sector_val == "ind":
                data_in = data_in.rename(columns={'ind_size': 'sizex'})
            elif sector_val == "ret":
                data_in = data_in.rename(columns={'availx': 'totavailx'})

            if sector_val == "ind":
                data_in['type2'] = np.where((data_in['type2'] == "D") | (data_in['type2'] == "W"), "DW", "F")

            if sector_val == "ret":
                data_in = data_in[(data_in['type1'] == "C") | (data_in['type1'] == "N")]
            
            if sector_val == "apt" or sector_val == "off":
                data_in['identity_met'] = data_in['metcode'] + sector_val.title()
            elif sector_val == "ret":
                data_in['identity_met'] = data_in['metcode'] + data_in['type1']
            elif sector_val == "ind":
                data_in['identity_met'] = data_in['metcode'] + data_in['type2']

            # Tag props that have lagged rent surveys within the range to be included in the survey packet rent chg inventory
            temp = data_in.copy()
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
            data_in = data_in.join(temp, on='id')
            data_in['has_l_surv'] = data_in['has_l_surv'].fillna(0)

            file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_msq_data.pickle".format(get_home(), sector_val))
            data_in.to_pickle(file_path)
        
            # Call the function to load and process the sq insight stats. The processed files will be saved to the network and can then be read in at any time
            process_sq_insight(sector_val, curryr, currmon, currqtr)     

            # Load in the met level sq metrics generated by DQs indtrend programs and join to main dataset
            file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_msq_data.pickle".format(get_home(), sector_val))
            msq_data_in = pd.read_pickle(file_path)

            # Special caveat here for office is that 99 subnames are ok if they are in a sub that represents non-cbd. But this is only the case for rolling up metsq data, for the review packet, it looks like DQ removed even these cases
            msq_data_in['balance_test'] = msq_data_in['submkt'].str.slice(0,2)
            if sector_val != "off":
                msq_data_in = msq_data_in[msq_data_in['balance_test'] != '99']
            elif sector_val == "off":
                msq_data_in = msq_data_in[(msq_data_in['balance_test'] != '99') | ((msq_data_in['subid'] >= 81) & (msq_data_in['metcode'] != "FM"))]

            msq_data_only_qtr = msq_data_in.copy()
            msq_data_only_qtr = msq_data_only_qtr[np.isnan(msq_data_only_qtr['currmon']) == True]
            msq_data_only_qtr = msq_data_only_qtr.drop_duplicates(['yr', 'qtr'])
            msq_data_only_qtr['only_qtr'] = 1
            msq_data_only_qtr['qtr_ident'] = msq_data_only_qtr['yr'].astype(str) + msq_data_only_qtr['qtr'].astype(str)
            msq_data_only_qtr = msq_data_only_qtr.set_index('qtr_ident')
            msq_data_only_qtr = msq_data_only_qtr[['only_qtr']]

            msq_data_in['qtr_ident'] = msq_data_in['yr'].astype(str) + msq_data_in['qtr'].astype(str)
            msq_data_in = msq_data_in.join(msq_data_only_qtr, on='qtr_ident')

            msq_data_in['currmon'] = np.where((msq_data_in['only_qtr'] == 1) & (msq_data_in['qtr'] == 1), 3, msq_data_in['currmon'])
            msq_data_in['currmon'] = np.where((msq_data_in['only_qtr'] == 1) & (msq_data_in['qtr'] == 2), 6, msq_data_in['currmon'])
            msq_data_in['currmon'] = np.where((msq_data_in['only_qtr'] == 1) & (msq_data_in['qtr'] == 3), 9, msq_data_in['currmon'])
            msq_data_in['currmon'] = np.where((msq_data_in['only_qtr'] == 1) & (msq_data_in['qtr'] == 4), 12, msq_data_in['currmon'])
            msq_data_in['currmon'] = msq_data_in['currmon'].astype(int)

            if sector_val == "ind":
                msq_data_in.sort_values(by=['id', 'type2', 'metcode', 'yr', 'qtr', 'currmon'], inplace=True)
                msq_data_in['join_ident'] = msq_data_in['metcode'] + msq_data_in['type2'] + msq_data_in['yr'].astype(str) + msq_data_in['qtr'].astype(str) + msq_data_in['currmon'].astype(str)
            else:
                msq_data_in.sort_values(by=['id', 'metcode', 'yr', 'qtr', 'currmon'], inplace=True)
                msq_data_in['join_ident'] = msq_data_in['metcode'] + msq_data_in['yr'].astype(str) + msq_data_in['qtr'].astype(str) + msq_data_in['currmon'].astype(str)

            if sector_val == "apt":
                round_val = 0
            else:
                round_val = -3

            msq_data1 = msq_data_in.copy()
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


            msq_data2 = msq_data_in.copy()
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

            # Note: In an effort to align the sq cons data with the trend data, will move away from the method of only reporting square rollup in the third month of the quarter for periods that do not have a monthly breakout in the msq
            msq_data3 = msq_data_in.copy()
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

            data_all_periods = data.copy()
            if sector_val != "ret":
                data_all_periods = data_all_periods.drop_duplicates(['subsector', 'metcode', 'yr', 'qtr', 'currmon'])
            else:
                data_all_periods = data_all_periods.drop_duplicates(['metcode', 'yr', 'qtr', 'currmon'])
            msq_data = data_all_periods.copy()
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
                
            msq_data = msq_data.join(msq_data1.set_index('join_ident').drop(drop_list, axis=1), on='join_ident')
            msq_data = msq_data.join(msq_data2.set_index('join_ident').drop(drop_list, axis=1), on='join_ident')
            msq_data = msq_data.join(msq_data3.set_index('join_ident').drop(drop_list, axis=1), on='join_ident')
            msq_data = msq_data.set_index('join_ident')
            msq_data['metdqinvren10'] = msq_data['metdqinvren10'].fillna(0)
            msq_data['metsqcons'] = msq_data['metsqcons'].fillna(0)

            if sector_val == "ind":
                msq_data.sort_values(by=['subsector', 'metcode', 'yr', 'qtr', 'currmon'], ascending=[True, True, True, True, True], inplace=True)
            else:
                msq_data.sort_values(by=['metcode', 'yr', 'qtr', 'currmon'], ascending=[True, True, True, True], inplace=True)
            file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_sq_metstats.pickle".format(get_home(), sector_val))
            msq_data.to_pickle(file_path)

            
        # Join in the most up to date sq met rollups
        if sector_val == "ind":
            data['join_ident'] = data['metcode'] + data['subsector'] + data['yr'].astype(str) + data['qtr'].astype(str) + data['currmon'].astype(str)
        else:
            data['join_ident'] = data['metcode'] + data['yr'].astype(str) + data['qtr'].astype(str) + data['currmon'].astype(str)
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_sq_metstats.pickle".format(get_home(), sector_val))
        msq_data= pd.read_pickle(file_path)
        msq_data = msq_data.drop(['metcode', 'yr', 'qtr', 'currmon'], axis=1)
        if sector_val == "ind":
            msq_data = msq_data.drop(['subsector'], axis=1)
        data = data.join(msq_data, on='join_ident')
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

        # Fill in nans for the first period abs in 2020q2 new subs
        data['abs'] = np.where((data['yr'] == 2019) & (data['currmon'] == 1) & (data['identity'] != data['identity'].shift(1)), np.nan, data['abs'])

        # Use the MSQ data set to calculate the total surveyed abs from NC properties in currmon for use in flags
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_msq_data.pickle".format(get_home(), sector_val))
        data_surabs = pd.read_pickle(file_path)
        data_surabs['surveyed'] = np.where(data_surabs['availxM'] == 0, 1, 0)
        data_surabs['count_survs'] = data_surabs.groupby('id')['surveyed'].transform('sum')
        data_surabs['nc_first_surv'] = np.where((data_surabs['yr'] == curryr) & (data_surabs['currmon'] == currmon) & (data_surabs['count_survs'] == 1) & (data_surabs['availxM'] == 0) & (data_surabs['yearx'] >= curryr - 3), 1, 0)
        data_surabs = data_surabs[(data_surabs['yr'] == curryr) & (data_surabs['currmon'] == currmon)]
        data_surabs = data_surabs[((data_surabs['yearx'] == curryr) & (data_surabs['month'] == currmon)) | (data_surabs['nc_first_surv'] == 1)]
        data_surabs['nc_surabs'] = np.where((data_surabs['availxM'] == 0), data_surabs['sizex'] - data_surabs['totavailx'], 0)
        
        if sector_val == "apt" or sector_val == "off":
            data_surabs['identity'] = data_surabs['metcode'] + data_surabs['subid'].astype(str) + sector_val.title()
        elif sector_val == "ret":
            data_surabs['identity'] = data_surabs['metcode'] + data_surabs['subid'].astype(str) + data_surabs['type1']
        elif sector_val == "ind":
            data_surabs['identity'] = data_surabs['metcode'] + data_surabs['subid'].astype(str) + data_surabs['type2']

        data_surabs['sum_nc_surabs'] = data_surabs.groupby('identity')['nc_surabs'].transform('sum')
        data_surabs = data_surabs[['identity', 'sum_nc_surabs']]
        data_surabs = data_surabs.rename(columns={'sum_nc_surabs': 'nc_surabs'})
        data_surabs = data_surabs.drop_duplicates('identity')
        
        if sector_val == "apt":
            data_surabs['nc_surabs'] = round(data_surabs['nc_surabs'], 0)
        else:
            data_surabs['nc_surabs'] = round(data_surabs['nc_surabs'], -3)
        data_surabs = data_surabs.set_index('identity')
        data = data.join(data_surabs, on='identity')
        data['nc_surabs'] = data['nc_surabs'].fillna(0)
        
    # If the input file did not load successfully, alert the user
    elif file_load_error == True:
        data = pd.DataFrame()
        orig_cols = []
        file_used = "error"
    
    return data, orig_cols, file_used