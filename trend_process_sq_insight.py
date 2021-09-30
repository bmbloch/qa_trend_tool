import pandas as pd
import numpy as np
from pathlib import Path
import re
import os
import sys
from IPython.core.display import display, HTML
import gc

def get_home():
    if os.name == "nt": return "//odin/reisadmin/"
    else: return "/home/"

#  Load select variables from the sqinsight output as support stats
def process_sq_insight(sector_val, curryr, currmon, currqtr):

    paths = []
    if sector_val == "ind" or sector_val == "ret":
        if sector_val == "ind":
            loop_list = ["DW", "F"]
        elif sector_val == "ret":
            loop_list = ["C", "N", "NC"]
        for subsector in loop_list:
            file_path_met_rent = Path("{}central/square/data/{}/sqpoolinsight/{}/{}rentchg_surv_stats_{}m{}.out".format(get_home(), sector_val, subsector, sector_val, curryr, currmon))
            file_path_sub_rent = Path("{}central/square/data/{}/sqpoolinsight/{}/{}rentchg_surv_stats_submkt_{}m{}.out".format(get_home(), sector_val, subsector, sector_val, curryr, currmon))
            file_path_met_vac = Path("{}central/square/data/{}/sqpoolinsight/{}/{}vacchg_surv_stats_{}m{}.out".format(get_home(), sector_val, subsector, sector_val, curryr, currmon))
            file_path_sub_vac = Path("{}central/square/data/{}/sqpoolinsight/{}/{}vacchg_surv_stats_submkt_{}m{}.out".format(get_home(), sector_val, subsector, sector_val, curryr, currmon))
            paths.append(file_path_met_rent)
            paths.append(file_path_sub_rent)
            paths.append(file_path_met_vac)
            paths.append(file_path_sub_vac)
    else:
        file_path_met_rent = Path("{}central/square/data/{}/sqpoolinsight/{}rentchg_surv_stats_{}m{}.out".format(get_home(), sector_val, sector_val, curryr, currmon))
        file_path_sub_rent = Path("{}central/square/data/{}/sqpoolinsight/{}rentchg_surv_stats_submkt_{}m{}.out".format(get_home(), sector_val, sector_val, curryr, currmon))
        file_path_met_vac = Path("{}central/square/data/{}/sqpoolinsight/{}vacchg_surv_stats_{}m{}.out".format(get_home(), sector_val, sector_val, curryr, currmon))
        file_path_sub_vac = Path("{}central/square/data/{}/sqpoolinsight/{}vacchg_surv_stats_submkt_{}m{}.out".format(get_home(), sector_val, sector_val, curryr, currmon))
        paths.append(file_path_met_rent)
        paths.append(file_path_sub_rent)
        paths.append(file_path_met_vac)
        paths.append(file_path_sub_vac)

    frames = []
    for path in paths:
        df_cols = []
        data_list = []
        count = 0
        with open(path, "r") as file:
            for line in file:
                if "\t" in line:
                    line_split = line.split("\t")
                else:
                    line_split = line.split(",")
                if count == 0:
                    df_cols = line_split
                    df_cols = [x.lower() for x in df_cols]
                    sq_insight = pd.DataFrame(columns = df_cols)
                    count += 1
                else:
                    data_list.append(line_split)
                    count += 1
    
        sq_insight = sq_insight.append(pd.DataFrame(data_list, columns = df_cols))

        
        for i, col in enumerate(sq_insight.columns):
            sq_insight.iloc[:, i] = sq_insight.iloc[:, i].str.replace('"', '')

        last_col = list(sq_insight.columns)[-1]
        sq_insight = sq_insight.rename(columns={last_col: last_col[:-1]})
        sq_insight[last_col[:-1]] = sq_insight[last_col[:-1]].str.slice(0, -1)


        no_conv_list = ['metcode', 'yr', 'qtr', 'currmon', 'subid', 'subsector']
        for x in list(sq_insight.columns):
            if x not in no_conv_list:
                sq_insight[x] = np.where(sq_insight[x] == '', np.nan, sq_insight[x])
                sq_insight[x] = sq_insight[x].astype(float)


        if sector_val == "ind":
            col_list = list(sq_insight.columns)
            if "DW" in str(path):
                sq_insight['subsector'] = "DW"
            elif "F" in str(path):
                sq_insight['subsector'] = "F"
            sq_insight = sq_insight[['subsector'] + col_list]
        if sector_val == "ret":
            col_list = list(sq_insight.columns)
            if "C" in str(path) and "NC" not in str(path):
                sq_insight['subsector'] = "C"
            elif "N" in str(path) and "NC" not in str(path):
                sq_insight['subsector'] = "N"
            elif "NC" in str(path):
                sq_insight['subsector'] = "NC"
            sq_insight = sq_insight[['subsector'] + col_list]
    
        frames.append(sq_insight)

    if sector_val == "off" or sector_val == "apt":
        sq_insight_met_rent = frames[0]
        sq_insight_sub_rent = frames[1]
        sq_insight_met_vac = frames[2]
        sq_insight_sub_vac = frames[3]
    elif sector_val == "ind":
        sq_insight_met_rent = frames[0]
        sq_insight_met_rent = sq_insight_met_rent.append(frames[4])
        sq_insight_sub_rent = frames[1]
        sq_insight_sub_rent = sq_insight_sub_rent.append(frames[5])
        sq_insight_met_vac = frames[2]
        sq_insight_met_vac = sq_insight_met_vac.append(frames[6])
        sq_insight_sub_vac = frames[3]
        sq_insight_sub_vac = sq_insight_sub_vac.append(frames[7])
    elif sector_val == "ret":
        sq_insight_met_rent = frames[0]
        sq_insight_met_rent = sq_insight_met_rent.append(frames[4])
        sq_insight_met_rent = sq_insight_met_rent.append(frames[8])
        sq_insight_sub_rent = frames[1]
        sq_insight_sub_rent = sq_insight_sub_rent.append(frames[5])
        sq_insight_sub_rent = sq_insight_sub_rent.append(frames[9])
        sq_insight_met_vac = frames[2]
        sq_insight_met_vac = sq_insight_met_vac.append(frames[6])
        sq_insight_met_vac = sq_insight_met_vac.append(frames[10])
        sq_insight_sub_vac = frames[3]
        sq_insight_sub_vac = sq_insight_sub_vac.append(frames[7])
        sq_insight_sub_vac = sq_insight_sub_vac.append(frames[11])

    sq_insight_met_rent = sq_insight_met_rent.reset_index(drop=True)
    sq_insight_sub_rent = sq_insight_sub_rent.reset_index(drop=True)
    sq_insight_met_vac = sq_insight_met_vac.reset_index(drop=True)
    sq_insight_sub_vac = sq_insight_sub_vac.reset_index(drop=True)

    sq_insight_met_rent[['yr', 'currmon', 'qtr']] = sq_insight_met_rent[['yr', 'currmon', 'qtr']].astype(int)
    sq_insight_sub_rent[['yr', 'currmon', 'qtr']] = sq_insight_sub_rent[['yr', 'currmon', 'qtr']].astype(int)
    sq_insight_met_vac[['yr', 'currmon', 'qtr']] = sq_insight_met_vac[['yr', 'currmon', 'qtr']].astype(int)
    sq_insight_sub_vac[['yr', 'currmon', 'qtr']] = sq_insight_sub_vac[['yr', 'currmon', 'qtr']].astype(int)

    # If this is an aggregation month for ind, save the currmon vars that were weighted and join to msq data set for use in review packet
    if sector_val == "ind" and currmon in [2,3,6,9,12]:
        temp = sq_insight_met_rent.copy()
        temp['identity_met'] = temp['metcode'] + temp['subsector']
        temp = temp[(temp['yr'] == curryr) & (temp['currmon'] == currmon)]
        temp = temp[['identity_met', 'g_renx_mo_wgt', 'avg_mos_to_last_rensur']]
        temp = temp.rename(columns={'g_renx_mo_wgt': 'sur_rentchgs_avgdiff_mo_wgt', 'avg_mos_to_last_rensur': 'sur_rentchgs_avgmos_tolastsur'})
        temp = temp.set_index('identity_met')
        file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_msq_data.pickle".format(get_home(), sector_val))
        msq_data = pd.read_pickle(file_path)
        msq_data = msq_data.join(temp, on='identity_met')
        msq_data.to_pickle(file_path)
        del msq_data
        gc.collect()
    
    # if sector_val == "ind":
    #     print("Take out qtr aggregation for ind once the new sq code is live")
    def roll_sq_insight(dataframe_in, curryr, currmon, currqtr, sector_val, var_type, level_type):
        dataframe = dataframe_in.copy()
        if var_type == "rent":
            if level_type == "sub":
                if sector_val == "ret" or sector_val == "ind":
                    dataframe['identity'] = dataframe['metcode'] + dataframe['subid'].astype(str) + dataframe['subsector']
                else:
                    dataframe['identity'] = dataframe['metcode'] + dataframe['subid'].astype(str) + sector_val.title()
            else:
                if sector_val == "ret" or sector_val == "ind":
                    dataframe['identity'] = dataframe['metcode'] + dataframe['subsector']
                else:
                    dataframe['identity'] = dataframe['metcode'] + sector_val.title()

        elif var_type == "vac":
            if sector_val == "ind" or sector_val == "ret":
                if level_type == "sub":
                    dataframe['identity'] = dataframe['metcode'] + dataframe['subid'].astype(str) + dataframe['subsector']
                else:
                    dataframe['identity'] = dataframe['metcode'] + dataframe['subsector']
            elif sector_val == "off" or sector_val == "apt":
                if level_type == "sub":
                    dataframe['identity'] = dataframe['metcode'] + dataframe['subid'].astype(str) + sector_val.title()
                else:
                    dataframe['identity'] = dataframe['metcode'] + sector_val.title()
        
        dataframe = dataframe.set_index('identity')

        dataframe = dataframe[(dataframe['yr'] == curryr) & (dataframe['currmon'] == currmon)]

        if sector_val == "apt":
            if level_type == "met":
                dataframe = dataframe.rename(columns={'g_avgrenx_mo_wgt': 'g_renx_mo_wgt', 'us_g_avgrenx_mo_wgt': 'us_g_renx_mo_wgt'})
            else:
                dataframe = dataframe.rename(columns={'g_avgrenx_mo_wgt': 'g_renx_mo_wgt'})
        if sector_val == "ret":
            if level_type == "met":
                dataframe = dataframe.rename(columns={'g_nrenx_mo_wgt': 'g_renx_mo_wgt', 'us_g_nrenx_mo_wgt': 'us_g_renx_mo_wgt'})
            else:
                dataframe = dataframe.rename(columns={'g_nrenx_mo_wgt': 'g_renx_mo_wgt'})    
        
        if var_type == "rent":
            if level_type == "met":
                cols_to_keep = ['avg_mos_to_last_rensur', 'rntchginv', 'g_renx_mo_wgt', 'us_avg_mos_to_last_rensur', 'us_rntchginv', 'us_g_renx_mo_wgt', 'rentchgs', 'rentdrops', 'rentflats', 'rentincrs', 'avg_rentchg', 'avg_rentchg_mo', 'stddev_avg_rentchg']
            elif level_type == "sub":
                cols_to_keep = ['avg_mos_to_last_rensur', 'rntchginv', 'g_renx_mo_wgt', 'rentdrops', 'rentflats', 'rentincrs']
        elif var_type == "vac":
            if level_type == "met":
                cols_to_keep = ['avg_mos_to_last_vacsur', 'vacchginv', 'us_avg_mos_to_last_vacsur', 'us_vacchginv', 'totabs', 'wtdvacchg']
            elif level_type == "sub":
                cols_to_keep = ['avg_mos_to_last_vacsur', 'vacchginv', 'vacdrops', 'vacflats', 'vacincrs', 'totabs', 'wtdvacchg']

        dataframe = dataframe[['yr', 'currmon'] + cols_to_keep]

        cols_to_keep = []
        if level_type == "met":
            for x in list(dataframe.columns):
                if x[0:2] != "us" and x != "yr" and x != "currmon":
                    dataframe = dataframe.rename(columns={x: 'met_' + x})
        elif level_type == "sub":
            for x in list(dataframe.columns):
                if x != "yr" and x != "currmon":
                    dataframe = dataframe.rename(columns={x: 'sub_' + x})

        if level_type == "met":
            dataframe = dataframe.reset_index().rename(columns={'identity': 'identity_met'}).set_index('identity_met')
        elif level_type == "sub":
            dataframe = dataframe.reset_index().rename(columns={'identity': 'identity_sub'}).set_index('identity_sub')

        for x in dataframe:
            dataframe[x] = np.where((dataframe['yr'] == curryr) & (dataframe['currmon'] == currmon), dataframe[x], np.nan)
        dataframe = dataframe.drop(['yr', 'currmon'], axis=1)

        return dataframe

    sq_insight_met_rent = roll_sq_insight(sq_insight_met_rent, curryr, currmon, currqtr, sector_val, "rent", "met")
    sq_insight_sub_rent = roll_sq_insight(sq_insight_sub_rent, curryr, currmon, currqtr, sector_val, "rent", "sub")
    sq_insight_met_vac = roll_sq_insight(sq_insight_met_vac, curryr, currmon, currqtr, sector_val, "vac", "met")
    sq_insight_sub_vac = roll_sq_insight(sq_insight_sub_vac, curryr, currmon, currqtr, sector_val, "vac", "sub")
    
    file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_sq_insight_met_rent.pickle".format(get_home(), sector_val))
    sq_insight_met_rent.to_pickle(file_path)
    file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_sq_insight_sub_rent.pickle".format(get_home(), sector_val))
    sq_insight_sub_rent.to_pickle(file_path)
    file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_sq_insight_met_vac.pickle".format(get_home(), sector_val))
    sq_insight_met_vac.to_pickle(file_path)
    file_path = Path("{}central/square/data/zzz-bb-test2/python/trend/intermediatefiles/{}_sq_insight_sub_vac.pickle".format(get_home(), sector_val))
    sq_insight_sub_vac.to_pickle(file_path)

    del sq_insight_met_rent
    del sq_insight_sub_rent
    del sq_insight_met_vac
    del sq_insight_sub_vac
    gc.collect()