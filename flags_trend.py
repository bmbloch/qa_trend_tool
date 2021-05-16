import pandas as pd
import numpy as np
import re
from IPython.core.display import display, HTML
from pathlib import Path

#from timer_trend import Timer
from support_functions_trend import get_issue

# This function sorts by the applicable flag and assigns an ascending ranking based on how far from the benchmark the sub is.
#@Timer("Calc Flag Ranking")
def calc_flag_ranking(dataframe_in, flag_names, calc_names):
    dataframe = dataframe_in.copy()

    temp_names = ['temp_' + flag_name for flag_name in flag_names]
    dataframe[calc_names] = np.where(dataframe[flag_names] == 0, np.nan, dataframe[calc_names])  
    dataframe[temp_names] = dataframe[calc_names].rank(ascending=False, method='first')
    dataframe[flag_names] = np.where(dataframe[flag_names] == 0, 0, dataframe[temp_names])
    dataframe = dataframe.drop(calc_names, axis=1)
    
    return dataframe

#@Timer("Calc Flags")
def calc_flags(data_in, curryr, currmon, sector_val, v_threshold, r_threshold):
    
    data = data_in.copy()

    calc_names = []

    # Flag if vac is different to rol vac and the cause is a cons rebench. Often this is not a problem, but still good to check it out
    # Only flag if the avail from nc is different from the row above, since once the nc comes on, the vac level will be different in all subsequent rows and we will overflag. Just want to focus on where the nc is actually causing the level change initially and in periods where some of it gets absorbed later
    data['calc_rolv'] = abs(data['vac'] - data['rol_vac'])
    calc_names.append('calc_rolv')
    data['c_flag_rolv'] = np.where((data['yr'] >= 2009) & (abs(data['vac'] - data['rol_vac']) >= 0.005) & (data['newncrev'] > 0) & (data['curr_tag'] == 0) & ((data['newncava'] != data['newncava'].shift(1)) | (data['newnc_thismo'] > 0) & (data['vac'] == data['vac_oob'])), 1, 0)
    
    # Flag if G_mrent is different to rol vac and the cause is a cons rebench. Often this is not a problem, but still good to check it out
    data['calc_rolg'] = abs(data['G_mrent'] - data['rol_G_mrent'])
    calc_names.append('calc_rolg')
    data['c_flag_rolg'] = np.where((data['yr'] >= 2009) & (abs(data['G_mrent'] - data['rol_G_mrent']) >= 0.003) & (data['newncrev'] > 0) & (data['curr_tag'] == 0) & (round(data['G_mrent'],3) == round(data['G_mrent_oob'],3)), 1, 0)
    
    # If this is an apt run, flag if sq_cons is not equal to pub cons, except if the sq_cons hasnt changed from last month
    if sector_val == "apt":
        data['calc'] = data['sqcons'] - data['cons']
        data['c_flag_sqdiff'] = np.where((data['yr'] >= 2009) & (data['sqcons'] - data['cons'] != data['p_sqcons'] - data['rol_cons']) & (data['sqcons'] != data['cons']) & (data['curr_tag'] == 0) & (np.isnan(data['p_sqcons']) == False), 1, 0)
        data['c_flag_sqdiff'] = np.where((data['sqcons'] != data['cons']) & (data['curr_tag'] == 1), 1, data['c_flag_sqdiff'])
        
        # EM requested that the actual difference be the output for the orig flags file so the ids causing the flag are easier to spot, so handle that case differently
        data["c_flag_sqdiff"] = np.where(data["c_flag_sqdiff"] == 1, data['calc'], data["c_flag_sqdiff"])
        data = data.drop(['calc'], axis=1)
    
    # Flag if a vacancy level is very low and there is no support from the actual square pool
    data['calc_vlow'] = (data['vac']) * -1
    calc_names.append('calc_vlow')
    data['v_flag_low'] = np.where(((data['vac'] < 0) |
                                     ((data['vac'] == 0) & (data['sqvac'] != 0)) |
                                     ((data['vac'] < 0.01) & (data['curr_tag'] == 1) & (data['vac'].shift(1) > 0.01) & (data['sqvac'] > 0.01))),
                                     1, 0)
    
    # Flag if vacancy level is above one hundred percent
    data['calc_vhigh'] = data['vac']
    calc_names.append('calc_vhigh')
    data['v_flag_high'] = np.where((data['vac'] > 1), 1,0)


    # Flag if vac chg is different than rol and there is no cons rebench to cause it
    # Use vac chg instead of vac, as that will flag only the period where the difference began, as opposed to all periods once the vac level was reset
    data['calc_vrol'] = abs(data['vac_chg'] - data['rol_vac_chg'])
    calc_names.append('calc_vrol')
    data['v_flag_rol'] = np.where((data['yr'] >= 2009) & (abs(data['vac_chg'] - data['rol_vac_chg']) >= 0.001) & 
                                    (abs(data['newncava'] - (data['avail'] - data['rol_avail'])) > 1000) & (data['curr_tag'] == 0) & (data['vac_chg'] == data['vac_chg_oob']), 1, 0)
    
    # Additional check that does not require vac_chg to match vac_chg_rol, to alert the user if there was a very large restatement by a shim
    data['v_flag_rol'] = np.where((data['yr'] >= 2009) & (abs(data['vac_chg'] - data['rol_vac_chg']) >= 0.001) & (abs(data['newncava'] - (data['avail'] - data['rol_avail'])) > 1000) & (data['curr_tag'] == 0) & (abs(data['vac_chg'] - data['vac_chg_oob']) >= 0.05), 1, data['v_flag_rol'])


    # Flag if sq vac level and pub vac level are very different
    if sector_val == "apt":
        data['calc_vsqlev'] = abs(data['vac'] - data['sqvac'])
        calc_names.append('calc_vsqlev')
        data['v_flag_sqlev'] = np.where((data['curr_tag'] == 1) & (abs(data['vac'] - data['sqvac']) > 0.02) & (abs(data['vac_chg_12'] - data['sqvac_chg_12']) > 0.02) &
                                         ((data['vac'] - data['sqvac']) * (data['vac_chg_12'] - data['sqvac_chg_12']) >= 0), 1, 0)
            

    # Flag if there is a large difference between published abs and sq abs
    data['calc_vsqabs'] = abs(data['abs'] - data['sqabs']) / data['inv']
    calc_names.append('calc_vsqabs')
    data['v_flag_sqabs'] = np.where((abs(data['abs'] - data['sqabs']) / data['inv'] >= 0.005) & (data['curr_tag'] == 1), 1, 0)
    

    # Flag if there is a large difference between published abs and surveyed abs
    data['sub_sur_totabs_fill'] = data['sub_sur_totabs'].fillna(0)
    data['calc_vsurabs'] = abs(data['abs'] - data['sub_sur_totabs_fill'] - data['nc_surabs']) / data['inv']
    calc_names.append('calc_vsurabs')
    data['v_flag_surabs'] = np.where((abs(data['abs'] - data['sub_sur_totabs_fill'] - data['nc_surabs']) / data['inv'] >= 0.005) & (data['curr_tag'] == 1), 1, 0)
    

    # Flag cases where the portion of published absorption not based on surveys is not in line with the published rent growth
    data['calc_vrsent'] = abs(data['abs'] - data['sub_sur_totabs_fill'])
    calc_names.append('calc_vrsent')
    data['sub_g_renx_mo_wgt_fill'] = data['sub_g_renx_mo_wgt'].fillna(0)
    data['met_g_renx_mo_wgt_fill'] = data['met_g_renx_mo_wgt'].fillna(0)
    data['v_flag_rsent'] = np.where((data['curr_tag'] == 1) & ((data['abs'] - data['sub_sur_totabs_fill'] - data['nc_surabs']) / data['inv'] >= 0.003) & (data['sub_g_renx_mo_wgt_fill'] <= -0.003) & (data['sub_sur_r_cov_perc'] >= r_threshold), 
                                    1, 0)
    data['v_flag_rsent'] = np.where((data['curr_tag'] == 1) & ((data['abs'] - data['sub_sur_totabs_fill']- data['nc_surabs']) / data['inv'] <= -0.003) & (data['sub_g_renx_mo_wgt_fill'] >= 0.003) & (data['sub_sur_r_cov_perc'] >= r_threshold), 
                                    1, data['v_flag_rsent'])
    data['v_flag_rsent'] = np.where((data['curr_tag'] == 1) & ((data['abs'] - data['sub_sur_totabs_fill']- data['nc_surabs']) / data['inv'] >= 0.003) & (data['met_g_renx_mo_wgt_fill'] <= -0.003) & (data['met_sur_r_cov_perc'] >= r_threshold) & (data['sub_sur_r_cov_perc'] < r_threshold), 
                                    1, data['v_flag_rsent'])
    data['v_flag_rsent'] = np.where((data['curr_tag'] == 1) & ((data['abs'] - data['sub_sur_totabs_fill']- data['nc_surabs']) / data['inv'] <= -0.003) & (data['met_g_renx_mo_wgt_fill'] >= 0.003) & (data['met_sur_r_cov_perc'] >= r_threshold) & (data['sub_sur_r_cov_perc'] < r_threshold), 
                                    1, data['v_flag_rsent'])

    data = data.drop(['sub_sur_totabs_fill', 'sub_g_renx_mo_wgt_fill', 'met_g_renx_mo_wgt_fill'], axis=1)


    # Flag cases where the trend vac level (or trend avail in the case of ind) is moving away from the sq level, and there is room to get closer to the monthized survey value for abs or the total survey value for abs depending on the case
    if sector_val == "apt":
        divisor = 100
    else:
        divisor = 1
    if sector_val != "ind":
        data['calc_vlev'] = abs(data['vac'] - data['sqvac'])
        calc_names.append('calc_vlev')
        data['v_flag_level'] = np.where((data['curr_tag'] == 1) & (data['vac'] > data['sqvac'] + 0.05) & ((data['abs'] - data['nc_surabs']) < data['sub_sur_totabs'] - (5000 / divisor)), 
                                    1, 0)
        data['v_flag_level'] = np.where((data['curr_tag'] == 1) & (data['vac'] < data['sqvac'] - 0.05) & (data['abs'] - data['nc_surabs'] > data['avail10d'] + (5000 / divisor)), 
                                    1, data['v_flag_level'])     
    else:
        data['calc_vlev'] = abs(data['avail'] - data['sqavail'])
        calc_names.append('calc_vlev')
        data['v_flag_level'] = np.where((data['curr_tag'] == 1) & (data['avail'] > data['sqavail']) & ((data['abs'] - data['nc_surabs']) < data['sub_sur_totabs'] - (5000 / divisor)), 
                                    1, 0)
        data['v_flag_level'] = np.where((data['curr_tag'] == 1) & (data['avail'] < data['sqavail']) & (data['abs'] - data['nc_surabs'] > data['avail10d'] + (5000 / divisor)), 
                                    1, data['v_flag_level'])  


    # Flag if G_mrent is different than rol and there is no cons rebench to cause it
    data['calc_grol'] = abs(data['G_mrent'] - data['rol_G_mrent'])
    calc_names.append('calc_grol')
    data['g_flag_rol'] = np.where((data['yr'] >= 2009) & (abs(data['G_mrent'] - data['rol_G_mrent']) >= 0.001) & (data['newncrev'] == data['newncrev'].shift(1)) & (data['curr_tag'] == 0) & (data['G_mrent'] == data['G_mrent_oob']), 1, 0)
    
    # Additional check that does not require rent chg to match rol rent chg, to alert the user if there was a very large restatement by a shim
    data['g_flag_rol'] = np.where((data['yr'] >= 2009) & (abs(data['G_mrent'] - data['rol_G_mrent']) >= 0.001) & (data['newncrev'] == data['newncrev'].shift(1)) & (data['curr_tag'] == 0) & (abs(data['G_mrent'] - data['G_mrent_oob']) >= 0.05), 1, data['g_flag_rol'])


    # Flag if sq rent level and pub rent level are very different in an ROL period
    if sector_val == "apt":
        data['calc_gsqlev'] = abs((data['mrent'] - data['sqsren']) / data['sqsren'])
        calc_names.append('calc_gsqlev')
        data['g_flag_sqlev'] = np.where((data['curr_tag'] == 1) & (abs((data['mrent'] - data['sqsren']) / data['sqsren']) > 0.15) & (abs(data['G_mrent_12'] - data['sq_Gmrent_12']) > 0.02) &
                                         (((data['mrent'] - data['sqsren']) / data['sqsren']) * (data['G_mrent_12'] - data['sq_Gmrent_12']) >= 0), 1, 0)
    

    # Flag cases where there is NC in the currmon and the rent growth is very large
    data['calc_gconsp'] = (data['G_mrent']) * -1
    calc_names.append('calc_gconsp')
    data['g_flag_consp'] = np.where((data['G_mrent'] > 0.01) & (data['cons'] > 0) & (data['curr_tag'] == 1), 1, 0)

    # Flag cases where there is NC in the currmon and the rent growth is negative
    data['calc_gconsn'] = data['G_mrent']
    calc_names.append('calc_gconsn')
    data['g_flag_consn'] = np.where((data['G_mrent'] < 0) & (data['cons'] / data['inv'] >= 0.02) & (data['curr_tag'] == 1), 1, 0)

    # Flag cases where there is a large published rent change and the cause is not cons
    data['calc_glarge'] = abs(data['G_mrent'])
    calc_names.append('calc_glarge')
    data['sub_g_renx_mo_wgt_fill'] = data['sub_g_renx_mo_wgt'].fillna(0)
    data['met_g_renx_mo_wgt_fill'] = data['met_g_renx_mo_wgt'].fillna(0)
    data['g_flag_large'] = np.where(((data['G_mrent'] > 0.015) | (data['G_mrent'] < -0.007)) & (data['g_flag_consp'] == 0) & (data['curr_tag'] == 1), 1, 0)

    # Dont flag if there is survey data to back it up with enough coverage
    data['g_flag_large'] = np.where((data['g_flag_large'] == 1) & (data['sub_sur_r_cov_perc'] >= r_threshold) & (data['G_mrent'] > 0) & (data['sub_g_renx_mo_wgt_fill'] > 0) & (data['sub_g_renx_mo_wgt_fill'] + 0.002 >= data['G_mrent']), 0, data['g_flag_large'])
    data['g_flag_large'] = np.where((data['g_flag_large'] == 1) & (data['sub_sur_r_cov_perc'] >= r_threshold) & (data['G_mrent'] < 0) & (data['sub_g_renx_mo_wgt_fill'] < 0) & (data['sub_g_renx_mo_wgt_fill'] - 0.002 <= data['G_mrent']), 0, data['g_flag_large'])
    data['g_flag_large'] = np.where((data['g_flag_large'] == 1) & (data['met_sur_r_cov_perc'] >= r_threshold) & (data['sub_sur_r_cov_perc'] < r_threshold) & (data['G_mrent'] > 0) & (data['met_g_renx_mo_wgt_fill'] > 0) & (data['sub_g_renx_mo_wgt_fill'] + 0.002 >= data['G_mrent']), 0, data['g_flag_large'])
    data['g_flag_large'] = np.where((data['g_flag_large'] == 1) & (data['met_sur_r_cov_perc'] >= r_threshold) & (data['sub_sur_r_cov_perc'] < r_threshold) & (data['G_mrent'] < 0) & (data['met_g_renx_mo_wgt_fill'] < 0) & (data['sub_g_renx_mo_wgt_fill'] - 0.002 <= data['G_mrent']), 0, data['g_flag_large'])


    # Flag cases where the published rent change is in the opposite direction from sq rent change
    data['calc_gsqdir'] = abs(data['G_mrent'] - data['sq_Gmrent'])
    calc_names.append('calc_gsqdir')
    data['g_flag_sqdir'] = np.where((data['curr_tag'] == 1) & (round(data['sq_Gmrent'], 3) > 0) & (data['G_mrent'] < 0), 
                                    1, 0)
    data['g_flag_sqdir'] = np.where((data['curr_tag'] == 1) & (round(data['sq_Gmrent'], 3) < 0) & (data['G_mrent'] > 0), 
                                    1, data['g_flag_sqdir'])


    # Flag cases where the published rent change is significantly different than the sq rent change
    data['calc_gsqdiff'] = abs(data['G_mrent'] - data['sq_Gmrent'])
    calc_names.append('calc_gsqdiff')
    data['g_flag_sqdiff'] = np.where((data['curr_tag'] == 1) & (abs(data['sq_Gmrent']  - data['G_mrent']) > 0.0034), 
                                    1, 0)

    # Dont flag if there is very strong survey magnitude that therefore might be getting applied from the lagged 3 periods of the trend rent recommendation formula
    data['g_flag_sqdiff'] = np.where((data['g_flag_sqdiff'] == 1) & (data['sq_Gmrent'] * data['G_mrent'] > 0) & (data['sub_g_renx_mo_wgt_fill'] * data['G_mrent'] > 0) & (abs(data['G_mrent']) <= abs(data['sub_g_renx_mo_wgt_fill'] / 3)) & (data['sub_sur_r_cov_perc'] > 0.02), 0, data['g_flag_sqdiff'])

    # Flag cases where the published rent change is significantly different than the surveyed rent change, with coverage thresholds set
    data['g_flag_surdiff'] = np.where((data['curr_tag'] == 1) & ((abs(data['G_mrent'] - data['sub_g_renx_mo_wgt_fill']) > 0.003) | (data['G_mrent'] * data['sub_g_renx_mo_wgt_fill'] < 0)) & 
                                    (data['sub_sur_r_cov_perc'] >= r_threshold), 
                                    1, 0)
    data['g_flag_surdiff'] = np.where((data['curr_tag'] == 1) & ((abs(data['G_mrent'] - data['met_g_renx_mo_wgt_fill']) > 0.003) | (data['G_mrent'] * data['met_g_renx_mo_wgt_fill'] < 0)) & 
                                    (data['met_sur_r_cov_perc'] >= r_threshold) & (data['sub_sur_r_cov_perc'] < r_threshold) & ((abs(data['G_mrent']) < data['us_g_renx_mo_wgt']) | (data['G_mrent'] * data['met_g_renx_mo_wgt_fill'] < 0)), 
                                    2, data['g_flag_surdiff'])
    
    data['g_flag_surdiff'] = np.where((data['G_mrent'] * data['sub_g_renx_mo_wgt_fill'] < 0) & (data['met_sur_r_cov_perc'] < r_threshold) & (data['sub_sur_r_cov_perc'] < r_threshold), 
                                    1, data['g_flag_surdiff'])

    data['g_flag_surdiff'] = np.where((data['sub_sur_r_cov_perc'] < r_threshold) & (data['met_sur_r_cov_perc'] >= r_threshold) & (data['G_mrent_perc'] > 0.75) & (data['G_mrent'] < 0) & (data['G_mrent'] < data['sub_g_renx_mo_wgt_fill'] / 2), 1, data['g_flag_surdiff'])
    data['g_flag_surdiff'] = np.where((data['sub_sur_r_cov_perc'] < r_threshold) & (data['met_sur_r_cov_perc'] >= r_threshold) & (data['G_mrent_perc'] > 0.75) & (data['G_mrent'] > 0) & (data['G_mrent'] > data['sub_g_renx_mo_wgt_fill'] / 2), 1, data['g_flag_surdiff'])


    threshold = 0.001
    data['g_flag_surdiff'] = np.where((data['curr_tag'] == 1) & (data['met_sur_r_cov_perc'] < r_threshold) & (data['sub_sur_r_cov_perc'] < r_threshold) & (data['G_mrent'] > data['us_g_renx_mo_wgt'] + threshold) & (round(data['us_g_renx_mo_wgt'],3) >= 0), 3, data['g_flag_surdiff'])
    data['g_flag_surdiff'] = np.where((data['curr_tag'] == 1) & (data['met_sur_r_cov_perc'] < r_threshold) & (data['sub_sur_r_cov_perc'] < r_threshold) & (round(data['G_mrent'],3) < 0) & (round(data['us_g_renx_mo_wgt'],3) >= 0) & (abs(data['G_mrent'] - data['us_g_renx_mo_wgt']) > 0.002), 3, data['g_flag_surdiff'])
    data['g_flag_surdiff'] = np.where((data['curr_tag'] == 1) & (data['met_sur_r_cov_perc'] < r_threshold) & (data['sub_sur_r_cov_perc'] < r_threshold) & (data['G_mrent'] < data['us_g_renx_mo_wgt'] - threshold) & (round(data['us_g_renx_mo_wgt'],3) < 0), 3, data['g_flag_surdiff'])
    data['g_flag_surdiff'] = np.where((data['curr_tag'] == 1) & (data['met_sur_r_cov_perc'] < r_threshold) & (data['sub_sur_r_cov_perc'] < r_threshold) & (round(data['G_mrent'],3) > 0) & (round(data['us_g_renx_mo_wgt'],3) <= 0) & (abs(data['G_mrent'] - data['us_g_renx_mo_wgt']) > 0.002), 3, data['g_flag_surdiff'])

    # Dont flag if there is very strong survey magnitude and enough is being applied in this month given the coverage
    data['g_flag_surdiff'] = np.where((data['g_flag_surdiff'] == 1) & (data['sub_g_renx_mo_wgt_fill'] * data['G_mrent'] > 0) & (abs(data['G_mrent']) >= abs(data['sub_g_renx_mo_wgt_fill'] * np.minimum(data['sub_sur_r_cov_perc'] * 1.5, 1))), 0, data['g_flag_surdiff'])

    # Dont flag if cons is the cause of the difference
    data['g_flag_surdiff'] = np.where((data['g_flag_surdiff'] > 0) & (data['G_mrent'] > 0) & (data['cons'] > 0) & (abs(data['G_mrent'] - data['sq_Gmrent']) < 0.0024), 0, data['g_flag_surdiff'])
    
    data['calc_gsurdiff'] = np.nan
    data['calc_gsurdiff'] = np.where((data['g_flag_surdiff'] == 1), abs(data['G_mrent'] - data['sub_g_renx_mo_wgt_fill']), data['calc_gsurdiff'])
    data['calc_gsurdiff'] = np.where((data['g_flag_surdiff'] == 2), abs(data['G_mrent'] - data['met_g_renx_mo_wgt_fill']), data['calc_gsurdiff'])
    data['calc_gsurdiff'] = np.where((data['g_flag_surdiff'] == 3), abs(data['G_mrent'] - 0), data['calc_gsurdiff'])
    calc_names.append('calc_gsurdiff')

    data = data.drop(['sub_g_renx_mo_wgt_fill', 'met_g_renx_mo_wgt_fill'], axis=1)

    data['g_flag_surdiff'] = np.where(data['g_flag_surdiff'] > 1, 1, data['g_flag_surdiff'])
    

    # Flag if gap is very low
    data['calc_elow'] = (data['gap']) * -1
    calc_names.append('calc_elow')
    data['e_flag_low'] =  np.where((data['gap'] < 0.02) & (data['gap'] < data['gap'].shift(1)) & ((data['curr_tag'] == 1) | (data['newncrev'] > 0)), 1, 0)

    # Flag if gap is very high
    data['calc_ehigh'] = data['gap']
    calc_names.append('calc_ehigh')
    data['e_flag_high'] =  np.where((data['gap'] > 0.30) & (data['gap'] > data['gap'].shift(1)) & ((data['curr_tag'] == 1) | (data['newncrev'] > 0)), 1, 0)

    # Flag if gap chg, which is modeled based on absorption, is moving strongly and the sub is already in the 5% percentile tail
    data['e_flag_perc'] =  np.where((data['curr_tag'] == 1) & (data['gap'] > data['gap_perc_5']) & (round(data['gap_chg'],3) > 0), 1, 0)
    data['e_flag_perc'] =  np.where((data['curr_tag'] == 1) & (data['gap'] < data['gap_perc_95']) & (round(data['gap_chg'],3) < 0), 2, data['e_flag_perc'])


    data['calce_eperc'] = np.where((data['e_flag_perc'] == 1), abs(data['gap'] - data['gap_perc_5']), np.nan)
    data['calc_eperc'] = np.where((data['e_flag_perc'] == 2), abs(data['gap'] - data['gap_perc_95']), data['calce_eperc'])
    calc_names.append('calc_eperc')
    data['e_flag_perc'] = np.where((data['e_flag_perc'] > 1), 1, data['e_flag_perc'])

    # Flag if effective rent change is in the opposite direction of market rent change
    data['calc_emdir'] = abs(data['G_mrent'] - data['G_merent'])
    calc_names.append('calc_emdir')
    data['e_flag_mdir'] =  np.where((data['curr_tag'] == 1) & (((round(data['G_mrent'],3) * round(data['G_merent'],3) < 0) & (abs(round(data['G_mrent'],3) - round(data['G_merent'],3)) > 0.003)) | ((round(data['G_mrent'],3) > 0) & (round(data['G_merent'],3) < -0.003))), 1, 0)

    flag_names = get_issue(False, False, False, False, False, False, False, False, False, False, "list", sector_val)
    flag_names = list(flag_names.keys())

    if sector_val == "apt":
        flag_names.remove("c_flag_sqdiff")

    data = calc_flag_ranking(data, flag_names, calc_names)
    
    return data