import pandas as pd
import numpy as np
import re
from IPython.core.display import display, HTML
from pathlib import Path
import timeit

from timer_trend import Timer
from support_functions_trend import get_issue

# Flag if vac is different to rol vac and the cause is a cons rebench. Often this is not a problem, but still good to check it out
# Only flag if the avail from nc is different from the row above, since once the nc comes on, the vac level will be different in all subsequent rows and we will overflag. Just want to focus on where the nc is actually causing the level change initially and in periods where some of it gets absorbed later
def c_rolv(data, curryr, currmon, sector_val, calc_names):
    
    if sector_val != "ind":
        data['c_flag_rolv'] = np.where((data['yr'] >= 2009) & (abs(data['vac'] - data['rol_vac']) >= 0.005) & (data['newncrev'] > 0) & (data['curr_tag'] == 0) & (data['vac'] == data['vac_oob']) & (data['conv'] == data['conv_oob']) & (data['demo'] == data['demo_oob']) & ((data['newncava'] != data['newncava'].shift(1)) | (data['newnc_thismo'] > 0)), 1, 0)
    else:
        data['c_flag_rolv'] = np.where((data['yr'] >= 2009) & (abs(data['vac'] - data['rol_vac']) >= 0.005) & (data['newncrev'] > 0) & (data['curr_tag'] == 0) & (data['vac'] == data['vac_oob']) & ((data['newncava'] != data['newncava'].shift(1)) | (data['newnc_thismo'] > 0)), 1, 0)
    
    data['calc_rolv'] = np.where(data['c_flag_rolv'] > 0, abs(data['vac'] - data['rol_vac']), np.nan)
    calc_names.append(list(data.columns)[-1])

    return data, calc_names

# Flag if G_mrent is different to rol vac and the cause is a cons rebench. Often this is not a problem, but still good to check it out
def c_rolg(data, curryr, currmon, sector_val, calc_names):

    data['c_flag_rolg'] = np.where((data['yr'] >= 2009) & (abs(data['G_mrent'] - data['rol_G_mrent']) >= 0.003) & (data['newncrev'] > 0) & (data['curr_tag'] == 0) & (round(data['G_mrent'],3) == round(data['G_mrent_oob'],3)), 1, 0)
    
    data['calc_rolg'] = np.where(data['c_flag_rolg'] > 0, abs(data['G_mrent'] - data['rol_G_mrent']), np.nan)
    calc_names.append(list(data.columns)[-1])

    return data, calc_names

# If this is an apt or off run, flag if sq_cons is not equal to pub cons, except if the sq_cons hasnt changed from last month
def c_sqdiff(data, curryr, currmon, sector_val, calc_names):

    if sector_val == "apt":
    
        data['c_flag_sqdiff'] = np.where((data['yr'] >= 2009) & (data['sqcons'] - data['cons'] != data['p_sqcons'] - data['rol_cons']) & (data['sqcons'] != data['cons']) & (data['curr_tag'] == 0) & (np.isnan(data['p_sqcons']) == False), 1, 0)
        data['c_flag_sqdiff'] = np.where((data['sqcons'] != data['cons']) & (data['curr_tag'] == 1), 1, data['c_flag_sqdiff'])
        
        # EM requested that the actual difference be the output for the orig flags file so the ids causing the flag are easier to spot, so handle that case differently
        data['c_flag_sqdiff'] = np.where(data['c_flag_sqdiff'] == 1, data['sqcons'] - data['cons'], data['c_flag_sqdiff'])
    
    elif sector_val == "off":
        data['c_flag_sqdiff'] = np.where((data['sqcons'] != data['cons']) & (data['yr'] >= curryr - 1), 1, 0)
        data['calc_csqdiff'] = np.where((data['c_flag_sqdiff'] == 1), abs(data['sqcons'] - data['cons']), np.nan)
        calc_names.append(list(data.columns)[-1])

    return data, calc_names

# Flag if a vacancy level is very low and there is no support from the actual square pool
def v_low(data, curryr, currmon, sector_val, calc_names):

    data['v_flag_low'] = np.where(((data['vac'] < 0) |
                                     ((data['vac'] == 0) & (data['sqvac'] != 0)) |
                                     ((data['vac'] < 0.01) & (data['curr_tag'] == 1) & (data['vac'].shift(1) > 0.01) & ((data['sqavail'] > data['avail']) | (data['sqinv'] > data['inv'])))),
                                     1, 0)
    
    data['calc_vlow'] = np.where(data['v_flag_low'] > 0, (data['vac']) * -1, np.nan)
    calc_names.append(list(data.columns)[-1])

    return data, calc_names

# Flag if vacancy level is above one hundred percent
def v_high(data, curryr, currmon, sector_val, calc_names):

    data['v_flag_high'] = np.where((data['vac'] > 1), 1, 0)
    
    data['calc_vhigh'] = np.where(data['v_flag_high'] > 0, data['vac'], np.nan)
    calc_names.append(list(data.columns)[-1])

    return data, calc_names

# Flag if vac chg is different than rol and there is no cons rebench to cause it
# Use vac chg instead of vac, as that will flag only the period where the difference began, as opposed to all periods once the vac level was reset
def v_rol(data, curryr, currmon, sector_val, calc_names):

    data['v_flag_rol'] = np.where((data['yr'] >= 2009) & (abs(data['vac_chg'] - data['rol_vac_chg']) >= 0.001) & 
                                    (abs(data['newncava'] - (data['avail'] - data['rol_avail'])) > 1000) & (data['curr_tag'] == 0) & (data['vac_chg'] == data['vac_chg_oob']), 1, 0)
    
    # Additional check that does not require vac_chg to match vac_chg_rol, to alert the user if there was a very large restatement by a shim
    data['v_flag_rol'] = np.where((data['yr'] >= 2009) & (abs(data['vac_chg'] - data['rol_vac_chg']) >= 0.001) & (abs(data['newncava'] - (data['avail'] - data['rol_avail'])) > 1000) & (data['curr_tag'] == 0) & (abs(data['vac_chg'] - data['vac_chg_oob']) >= 0.05), 1, data['v_flag_rol'])

    data['calc_vrol'] = np.where(data['v_flag_rol'] > 0, abs(data['vac_chg'] - data['rol_vac_chg']), np.nan)    
    calc_names.append(list(data.columns)[-1])

    return data, calc_names

# Flag if sq vac level and pub vac level are very different
def v_sqlev(data, curryr, currmon, sector_val, calc_names):

    data['v_flag_sqlev'] = np.where((data['curr_tag'] == 1) & (abs(data['vac'] - data['sqvac']) > 0.02) & (abs(data['vac_chg_12'] - data['sqvac_chg_12']) > 0.02) &
                                         ((data['vac'] - data['sqvac']) * (data['vac_chg_12'] - data['sqvac_chg_12']) >= 0), 1, 0)
        
    data['calc_vsqlev'] = np.where(data['v_flag_sqlev'] > 0, abs(data['vac'] - data['sqvac']), np.nan)
    calc_names.append(list(data.columns)[-1])

    return data, calc_names

# Flag if there is a large difference between published abs and sq abs
def v_sqabs(data, curryr, currmon, sector_val, calc_names):

    data['v_flag_sqabs'] = np.where((abs(data['abs'] - data['sqabs']) / data['inv'] >= 0.005) & (data['curr_tag'] == 1) & (data['avail_oob'] == data['avail']), 1, 0)
    
    data['calc_vsqabs'] = np.where(data['v_flag_sqabs'] > 0, abs(data['abs'] - data['sqabs']) / data['inv'], np.nan)
    calc_names.append(list(data.columns)[-1])

    return data, calc_names

# Flag if there is a large difference between published abs and surveyed abs
def v_surabs(data, curryr, currmon, sector_val, calc_names):

    data['sub_sur_totabs_fill'] = data['sub_sur_totabs'].fillna(0)
    data['v_flag_surabs'] = np.where((abs(data['abs'] - data['sub_sur_totabs_fill'] - data['nc_surabs']) / data['inv'] >= 0.005) & (data['curr_tag'] == 1), 1, 0)
    data['v_flag_surabs'] = np.where(((data['abs'] - data['nc_surabs']) * data['avail0d'] < 0) & (data['curr_tag'] == 1), 1, data['v_flag_surabs'])

    # Dont flag if abs is higher than avail0d and totsurabs and moving it closer to total would move the vacancy further away from sqvac
    data['v_flag_surabs'] = np.where((data['v_flag_surabs'] == 1) & (data['vac'] > data['sqvac']) & (data['abs'] > data['sub_sur_totabs']) & (data['abs'] <= data['avail0d']) & (data['abs'] < 0), 0, data['v_flag_surabs'])
    data['v_flag_surabs'] = np.where((data['v_flag_surabs'] == 1) & (data['vac'] < data['sqvac']) & (data['abs'] < data['sub_sur_totabs']) & (data['abs'] >= data['avail0d']) & (data['abs'] > 0), 0, data['v_flag_surabs'])
    
    # Per the other analysts' request, dont flag if between avail0d and total surabs. First clause only for non ind sectors
    if sector_val != "ind":
        data['v_flag_surabs'] = np.where((data['v_flag_surabs'] == 1) & (data['abs'] >= data['avail0d']) & (data['abs'] <= data['sub_sur_totabs']), 0, data['v_flag_surabs'])
    data['v_flag_surabs'] = np.where((data['v_flag_surabs'] == 1) & (abs(data['abs'] - data['avail0d']) / data['inv'] < 0.001) & (data['sub_sur_totabs'].isnull() == True), 0, data['v_flag_surabs'])

    data['calc_vsurabs'] = np.where(data['v_flag_surabs'] > 0, abs(data['abs'] - data['sub_sur_totabs_fill'] - data['nc_surabs']) / data['inv'], np.nan)
    calc_names.append(list(data.columns)[-1])

    return data, calc_names

# Flag cases where the portion of published absorption not based on surveys is not in line with the published rent growth
def v_rsent(data, curryr, currmon, sector_val, calc_names, r_threshold):

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

    data['calc_vrsent'] = np.where(data['v_flag_rsent'] > 0, abs(data['abs'] - data['sub_sur_totabs_fill']), np.nan)
    calc_names.append(list(data.columns)[-1])

    data = data.drop(['sub_sur_totabs_fill', 'sub_g_renx_mo_wgt_fill', 'met_g_renx_mo_wgt_fill'], axis=1)

    return data, calc_names

# Flag cases where the trend vac level (or trend avail in the case of ind) is moving away from the sq level, and there is room to get closer to the monthized survey value for abs or the total survey value for abs depending on the case   
def v_level(data, curryr, currmon, sector_val, calc_names):

    if sector_val == "apt":
        divisor = 100
    else:
        divisor = 1
    if sector_val != "ind":
        data['v_flag_level'] = np.where((data['curr_tag'] == 1) & (data['vac'] > data['sqvac'] + 0.05) & ((data['abs'] - data['nc_surabs']) < data['sub_sur_totabs'] - (5000 / divisor)), 
                                    1, 0)
        data['v_flag_level'] = np.where((data['curr_tag'] == 1) & (data['vac'] < data['sqvac'] - 0.05) & (data['abs'] - data['nc_surabs'] > data['avail0d'] + (5000 / divisor)), 
                                    1, data['v_flag_level'])    

        data['calc_vlev'] = np.where(data['v_flag_level'] > 0, abs(data['vac'] - data['sqvac']), np.nan) 
    else:
        data['v_flag_level'] = np.where((data['curr_tag'] == 1) & (data['avail'] > data['sqavail']) & ((data['abs'] - data['nc_surabs']) < data['sub_sur_totabs'] - (5000 / divisor)), 
                                    1, 0)
        data['v_flag_level'] = np.where((data['curr_tag'] == 1) & (data['avail'] < data['sqavail']) & (data['abs'] - data['nc_surabs'] > data['avail0d'] + (5000 / divisor)), 
                                    1, data['v_flag_level'])  
        
        data['calc_vlev'] = np.where(data['v_flag_level'] > 0, abs(data['avail'] - data['sqavail']), np.nan)
    
    calc_names.append(list(data.columns)[-1])

    return data, calc_names

# Flag if G_mrent is different than rol and there is no cons rebench to cause it
def g_rol(data, curryr, currmon, sector_val, calc_names):

    data['g_flag_rol'] = np.where((data['yr'] >= 2009) & (abs(data['G_mrent'] - data['rol_G_mrent']) >= 0.001) & (data['newncrev'] == data['newncrev'].shift(1)) & (data['curr_tag'] == 0) & (data['G_mrent'] == data['G_mrent_oob']), 1, 0)
    
    # Additional check that does not require rent chg to match oob rent chg, to alert the user if there was a very large restatement by a shim
    data['g_flag_rol'] = np.where((data['yr'] >= 2009) & (abs(data['G_mrent'] - data['rol_G_mrent']) >= 0.001) & (data['newncrev'] == data['newncrev'].shift(1)) & (data['curr_tag'] == 0) & (abs(data['G_mrent'] - data['G_mrent_oob']) >= 0.05), 1, data['g_flag_rol'])

    data['calc_grol'] = np.where(data['g_flag_rol'] > 0, abs(data['G_mrent'] - data['rol_G_mrent']), np.nan)
    calc_names.append(list(data.columns)[-1])

    return data, calc_names

# Flag if sq rent level and pub rent level are very different in an ROL period
def g_sqlev(data, curryr, currmon, sector_val, calc_names):

    data['g_flag_sqlev'] = np.where((data['curr_tag'] == 1) & (abs((data['mrent'] - data['sqsren']) / data['sqsren']) > 0.15) & (abs(data['G_mrent_12'] - data['sq_Gmrent_12']) > 0.02) &
                                         (((data['mrent'] - data['sqsren']) / data['sqsren']) * (data['G_mrent_12'] - data['sq_Gmrent_12']) >= 0), 1, 0)
        
    data['calc_gsqlev'] = np.where(data['g_flag_sqlev'] > 0, abs((data['mrent'] - data['sqsren']) / data['sqsren']), np.nan)
    calc_names.append(list(data.columns)[-1])

    return data, calc_names

# Flag cases where there is NC in the currmon and the rent growth is very large
def g_consp(data, curryr, currmon, sector_val, calc_names):

    data['g_flag_consp'] = np.where((data['G_mrent'] > 0.01) & (data['cons'] > 0) & (data['curr_tag'] == 1), 1, 0)
    
    data['calc_gconsp'] = np.where(data['g_flag_consp'] > 0, data['G_mrent'], np.nan)
    calc_names.append(list(data.columns)[-1])

    return data, calc_names

# Flag cases where there is NC in the currmon and the rent growth is negative
def g_consn(data, curryr, currmon, sector_val, calc_names):

    data['g_flag_consn'] = np.where((data['G_mrent'] < 0) & (data['cons'] / data['inv'] >= 0.02) & (data['curr_tag'] == 1), 1, 0)

    data['calc_gconsn'] = np.where(data['g_flag_consn'] > 0, data['G_mrent'] * -1, np.nan)
    calc_names.append(list(data.columns)[-1])

    return data, calc_names

# Flag cases where there is a large published rent change and the cause is not cons
def g_large(data, curryr, currmon, sector_val, calc_names, r_threshold):

    data['sub_g_renx_mo_wgt_fill'] = data['sub_g_renx_mo_wgt'].fillna(0).round(3)
    data['met_g_renx_mo_wgt_fill'] = data['met_g_renx_mo_wgt'].fillna(0).round(3)
    data['g_flag_large'] = np.where(((data['G_mrent'] > data['g_avg_pos']) | (data['G_mrent'] < data['g_avg_neg'])) & (data['g_flag_consp'] == 0) & (data['curr_tag'] == 1), 1, 0)
    
    # Dont flag if there is survey data to back it up with enough coverage
    data['g_flag_large'] = np.where((data['g_flag_large'] == 1) & (data['sub_sur_r_cov_perc'] >= r_threshold) & (data['G_mrent'] > 0) & (data['sub_g_renx_mo_wgt_fill'] > 0) & (data['sub_g_renx_mo_wgt_fill'] + 0.002 >= data['G_mrent']), 0, data['g_flag_large'])
    data['g_flag_large'] = np.where((data['g_flag_large'] == 1) & (data['sub_sur_r_cov_perc'] >= r_threshold) & (data['G_mrent'] < 0) & (data['sub_g_renx_mo_wgt_fill'] < 0) & (data['sub_g_renx_mo_wgt_fill'] - 0.002 <= data['G_mrent']), 0, data['g_flag_large'])
    data['g_flag_large'] = np.where((data['g_flag_large'] == 1) & (data['met_sur_r_cov_perc'] >= r_threshold) & (data['sub_sur_r_cov_perc'] < r_threshold) & (data['G_mrent'] > 0) & (data['met_g_renx_mo_wgt_fill'] > 0) & (data['sub_g_renx_mo_wgt_fill'] + 0.002 >= data['G_mrent']), 0, data['g_flag_large'])
    data['calc_glarge'] = np.where((data['g_flag_large'] == 1) & (data['met_sur_r_cov_perc'] >= r_threshold) & (data['sub_sur_r_cov_perc'] < r_threshold) & (data['G_mrent'] < 0) & (data['met_g_renx_mo_wgt_fill'] < 0) & (data['sub_g_renx_mo_wgt_fill'] - 0.002 <= data['G_mrent']), 0, data['g_flag_large'])

    data['calc_glarge'] = np.where(data['g_flag_large'] > 0, abs(data['G_mrent']), np.nan)
    calc_names.append(list(data.columns)[-1])

    return data, calc_names

# Flag cases where the published rent change is in the opposite direction from sq rent change
def g_sqdir(data, curryr, currmon, sector_val, calc_names):

    data['g_flag_sqdir'] = np.where((data['curr_tag'] == 1) & (round(data['sq_Gmrent'], 3) > 0) & (data['G_mrent'] < 0), 
                                    1, 0)
    data['g_flag_sqdir'] = np.where((data['curr_tag'] == 1) & (round(data['sq_Gmrent'], 3) < 0) & (data['G_mrent'] > 0), 
                                    1, data['g_flag_sqdir'])

    data['calc_gsqdir'] = np.where(data['g_flag_sqdir'] > 0, abs(data['G_mrent'] - data['sq_Gmrent']), np.nan)
    calc_names.append(list(data.columns)[-1])

    return data, calc_names

# Flag cases where the published rent change is significantly different than the sq rent change
def g_sqdiff(data, curryr, currmon, sector_val, calc_names):

    data['g_flag_sqdiff'] = np.where((data['curr_tag'] == 1) & ((abs(data['sq_Gmrent']  - data['G_mrent']) > 0.0034) | ((abs(round(data['sq_Gmrent'],3) - round(data['G_mrent'],3)) > 0.001) & (round(data['sub_g_renx_mo_wgt_fill'],3) == 0))), 
                                    1, 0)

    # Dont flag if there is very strong survey magnitude that therefore might be getting applied from the lagged 3 periods of the trend rent recommendation formula
    data['g_flag_sqdiff'] = np.where((data['g_flag_sqdiff'] == 1) & (data['sq_Gmrent'] * data['G_mrent'] > 0) & (data['sub_g_renx_mo_wgt_fill'] * data['G_mrent'] > 0) & (abs(data['G_mrent']) <= abs(data['sub_g_renx_mo_wgt_fill'] / 3)) & (data['sub_sur_r_cov_perc'] > 0.02), 0, data['g_flag_sqdiff'])

    # Dont flag if there is a shim and the new value is supported by survey data
    data['g_flag_sqdiff'] = np.where((data['g_flag_sqdiff'] == 1) & (data['G_mrent'] != data['G_mrent_oob']) & (abs(data['G_mrent']) <= abs(data['sub_g_renx_mo_wgt_fill']) + 0.001) & (data['G_mrent'] * data['sub_g_renx_mo_wgt_fill'] >= 0), 0, data['g_flag_sqdiff'])

    # Dont flag if this is apt and the difference is attributed to "catch up" code in trend aggregation program to get the 12 month sq/pub rent growths in line
    if sector_val == 'apt':
        data['g_flag_sqdiff'] = np.where((data['g_flag_sqdiff'] == 1) & (round(data['G_mrent'],3) == round(data['G_mrent_oob'],3)) & (abs(data['sq_Gmrent_12'] - data['G_mrent_12']) < 0.02) & (data['sq_Gmrent_12'] * data['G_mrent_12'] >= 0) & (((round(data['sq_Gmrent_12'],3) < round(data['G_mrent_12'],3)) & (data['G_mrent'] <= data['sq_Gmrent'])) | ((round(data['sq_Gmrent_12'],3) > round(data['G_mrent_12'],3)) & (data['G_mrent'] >= data['sq_Gmrent'])) | (round(data['sq_Gmrent_12'],3) == round(data['G_mrent_12'],3)) | (abs(data['sq_Gmrent_12'] - data['G_mrent_12'] <= 0.005))), 0, data['g_flag_sqdiff'])
        
    data['calc_gsqdiff'] = np.where(data['g_flag_sqdiff'] > 0, abs(data['G_mrent'] - data['sq_Gmrent']), np.nan)
    calc_names.append(list(data.columns)[-1])

    return data, calc_names

# Flag cases where the published rent change is significantly different than the surveyed rent change, with coverage thresholds set
def g_surdiff(data, curryr, currmon, sector_val, calc_names, r_threshold):

    data['g_flag_surdiff'] = np.where((data['curr_tag'] == 1) & ((abs(data['G_mrent'] - data['sub_g_renx_mo_wgt_fill']) > 0.003) | (data['G_mrent'] * data['sub_g_renx_mo_wgt_fill'] < 0)) & 
                                    (data['sub_sur_r_cov_perc'] >= r_threshold), 
                                    1, 0)
    
    data['g_flag_surdiff'] = np.where((data['G_mrent'] * data['sub_g_renx_mo_wgt_fill'] < 0) & ((data['G_mrent'] * data['met_g_renx_mo_wgt_fill'] <= 0) | (abs(data['G_mrent']) > 0.003) | (data['sub_sur_r_cov_perc'] >= data['met_sur_r_cov_perc'])) & (data['met_sur_r_cov_perc'] < r_threshold) & (data['sub_sur_r_cov_perc'] < r_threshold), 
                                    1, data['g_flag_surdiff'])

    threshold = 0.001
    data['g_flag_surdiff'] = np.where((data['curr_tag'] == 1) & (data['met_sur_r_cov_perc'] < r_threshold) & (data['sub_sur_r_cov_perc'] < r_threshold) & (round(data['G_mrent'],3) > round(data['us_g_renx_mo_wgt'],3) + threshold) & (round(data['us_g_renx_mo_wgt'],3) >= 0), 3, data['g_flag_surdiff'])
    data['g_flag_surdiff'] = np.where((data['curr_tag'] == 1) & (data['met_sur_r_cov_perc'] < r_threshold) & (data['sub_sur_r_cov_perc'] < r_threshold) & (round(data['G_mrent'],3) < 0) & (round(data['us_g_renx_mo_wgt'],3) >= 0) & (abs(data['G_mrent'] - data['us_g_renx_mo_wgt']) > 0.002), 3, data['g_flag_surdiff'])
    data['g_flag_surdiff'] = np.where((data['curr_tag'] == 1) & (data['met_sur_r_cov_perc'] < r_threshold) & (data['sub_sur_r_cov_perc'] < r_threshold) & (round(data['G_mrent'],3) < round(data['us_g_renx_mo_wgt'],3) - threshold) & (round(data['us_g_renx_mo_wgt'],3) < 0), 3, data['g_flag_surdiff'])
    data['g_flag_surdiff'] = np.where((data['curr_tag'] == 1) & (data['met_sur_r_cov_perc'] < r_threshold) & (data['sub_sur_r_cov_perc'] < r_threshold) & (round(data['G_mrent'],3) > 0) & (round(data['us_g_renx_mo_wgt'],3) <= 0) & (abs(data['G_mrent'] - data['us_g_renx_mo_wgt']) > 0.002), 3, data['g_flag_surdiff'])
   
    # Dont flag if chg is small in magnitude and in the direction of the surveys, even if low coverage
    data['g_flag_surdiff'] = np.where((data['g_flag_surdiff'] == 3) & (data['G_mrent'] * data['sub_g_renx_mo_wgt_fill'] > 0) & (abs(data['sub_g_renx_mo_wgt_fill']) > abs(data['G_mrent'])) & (abs(data['G_mrent']) <= 0.003), 0, data['g_flag_surdiff'])

    # Dont flag if there is very strong survey magnitude and enough is being applied in this month given the coverage
    data['g_flag_surdiff'] = np.where((data['g_flag_surdiff'] == 1) & (data['sub_g_renx_mo_wgt_fill'] * data['G_mrent'] > 0) & (abs(data['G_mrent']) >= abs(data['sub_g_renx_mo_wgt_fill'] * np.minimum(data['sub_sur_r_cov_perc'] * 1.5, 1))), 0, data['g_flag_surdiff'])

    # Dont flag if cons is the cause of the difference
    data['g_flag_surdiff'] = np.where((data['g_flag_surdiff'] > 0) & (data['G_mrent'] > 0) & (data['cons'] > 0) & (abs(data['G_mrent'] - data['sq_Gmrent']) < 0.0024), 0, data['g_flag_surdiff'])
    
    data['calc_gsurdiff'] = np.nan
    data['calc_gsurdiff'] = np.where((data['g_flag_surdiff'] == 1) | (data['g_flag_surdiff'] == 4), abs(data['G_mrent'] - data['sub_g_renx_mo_wgt_fill']), data['calc_gsurdiff'])
    data['calc_gsurdiff'] = np.where((data['g_flag_surdiff'] == 2), abs(data['G_mrent'] - data['met_g_renx_mo_wgt_fill']), data['calc_gsurdiff'])
    data['calc_gsurdiff'] = np.where((data['g_flag_surdiff'] == 3), abs(data['G_mrent'] - 0), data['calc_gsurdiff'])
    calc_names.append(list(data.columns)[-1])

    data = data.drop(['sub_g_renx_mo_wgt_fill', 'met_g_renx_mo_wgt_fill'], axis=1)

    data['g_flag_surdiff'] = np.where(data['g_flag_surdiff'] > 1, 1, data['g_flag_surdiff'])

    return data, calc_names

# Flag if gap is very low
def e_low(data, curryr, currmon, sector_val, calc_names):

    data['e_flag_low'] =  np.where((data['gap'] < 0.02) & (data['gap'] < data['gap'].shift(1)) & ((data['curr_tag'] == 1) | (data['newncrev'] > 0)), 1, 0)
    
    data['calc_elow'] = np.where(data['e_flag_low'] > 0, (data['gap']) * -1, np.nan)    
    calc_names.append(list(data.columns)[-1])

    return data, calc_names

# Flag if gap is very high
def e_high(data, curryr, currmon, sector_val, calc_names):

    data['e_flag_high'] =  np.where((data['gap'] > 0.30) & (data['gap'] > data['gap'].shift(1)) & ((data['curr_tag'] == 1) | (data['newncrev'] > 0)), 1, 0)
    
    data['calc_ehigh'] = np.where(data['e_flag_high'] > 0, data['gap'], np.nan)
    calc_names.append(list(data.columns)[-1])

    return data, calc_names

# Flag if gap chg, which is modeled based on absorption, is moving further into the extreme percentile tails of gap dist
def e_perc(data, curryr, currmon, sector_val, calc_names):

    data['e_flag_perc'] =  np.where((data['curr_tag'] == 1) & (data['gap'] > data['gap_perc_5']) & (data['gap_chg'] >= 0.001), 1, 0)
    data['e_flag_perc'] =  np.where((data['curr_tag'] == 1) & (data['gap'] < data['gap_perc_95']) & (data['gap_chg'] <= -0.001), 2, data['e_flag_perc'])


    data['calc_eperc'] = np.where((data['e_flag_perc'] == 1), abs(data['gap'] - data['gap_perc_5']), np.nan)
    data['calc_eperc'] = np.where((data['e_flag_perc'] == 2), abs(data['gap'] - data['gap_perc_95']), data['calc_eperc'])
    calc_names.append(list(data.columns)[-1])

    data['e_flag_perc'] = np.where((data['e_flag_perc'] > 1), 1, data['e_flag_perc'])

    return data, calc_names

# Flag if effective rent change is in the opposite direction of market rent change
def e_mdir(data, curryr, currmon, sector_val, calc_names):

    data['e_flag_mdir'] =  np.where((data['curr_tag'] == 1) & (((round(data['G_mrent'],3) * round(data['G_merent'],3) < 0) & (abs(round(data['G_mrent'],3) - round(data['G_merent'],3)) > 0.003)) | ((round(data['G_mrent'],3) > 0) & (round(data['G_merent'],3) < -0.003))), 1, 0)

    data['calc_emdir'] = np.where(data['e_flag_mdir'] > 0, abs(data['G_mrent'] - data['G_merent']), np.nan)
    calc_names.append(list(data.columns)[-1])

    return data, calc_names

def calc_flags(data_in, curryr, currmon, sector_val, v_threshold, r_threshold):
    
    data = data_in.copy()

    calc_names = []

    # Call the individual flag functions to get the current flags
    data, calc_names = c_rolv(data, curryr, currmon, sector_val, calc_names)
    data, calc_names = c_rolg(data, curryr, currmon, sector_val, calc_names)
    if sector_val == "apt" or sector_val == "off":
        data, calc_names = c_sqdiff(data, curryr, currmon, sector_val, calc_names)
    data, calc_names = v_low(data, curryr, currmon, sector_val, calc_names)
    data, calc_names = v_high(data, curryr, currmon, sector_val, calc_names)
    data, calc_names = v_rol(data, curryr, currmon, sector_val, calc_names)
    if sector_val == "apt":
        data, calc_names = v_sqlev(data, curryr, currmon, sector_val, calc_names)
    data, calc_names = v_sqabs(data, curryr, currmon, sector_val, calc_names)
    data, calc_names = v_surabs(data, curryr, currmon, sector_val, calc_names)
    data, calc_names = v_rsent(data, curryr, currmon, sector_val, calc_names, r_threshold)
    data, calc_names = v_level(data, curryr, currmon, sector_val, calc_names)   
    data, calc_names = g_rol(data, curryr, currmon, sector_val, calc_names)
    if sector_val == "apt":
        data, calc_names = g_sqlev(data, curryr, currmon, sector_val, calc_names)
    data, calc_names = g_consp(data, curryr, currmon, sector_val, calc_names)
    data, calc_names = g_consn(data, curryr, currmon, sector_val, calc_names)
    data, calc_names = g_large(data, curryr, currmon, sector_val, calc_names, r_threshold)
    data, calc_names = g_sqdir(data, curryr, currmon, sector_val, calc_names)
    data, calc_names = g_sqdiff(data, curryr, currmon, sector_val, calc_names)
    data, calc_names = g_surdiff(data, curryr, currmon, sector_val, calc_names, r_threshold)
    data, calc_names = e_low(data, curryr, currmon, sector_val, calc_names)
    data, calc_names = e_high(data, curryr, currmon, sector_val, calc_names)
    data, calc_names = e_perc(data, curryr, currmon, sector_val, calc_names)
    data, calc_names = e_mdir(data, curryr, currmon, sector_val, calc_names)

    flag_names = get_issue("list", sector_val)
    flag_names = list(flag_names.keys())

    if sector_val == "apt":
        flag_names.remove("c_flag_sqdiff")

    # Calculate flag rankings for all flag cols
    data[flag_names] = data[calc_names].rank(ascending=False, method='first')
    data = data.drop(calc_names, axis=1)
    data[flag_names] = data[flag_names].fillna(0)

    return data