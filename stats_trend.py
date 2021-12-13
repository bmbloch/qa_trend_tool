import pandas as pd
import numpy as np
import itertools
from statsmodels.tsa.holtwinters import SimpleExpSmoothing
import time
from scipy.stats import zscore
from IPython.core.display import display, HTML
from pathlib import Path
import math

#from timer_trend import Timer

# Function that drops columns of the dataframe that will be recalculated each time a sub is examined for flags
def drop_cols(dataframe):
    cols = list(dataframe.columns)
    index = dataframe.columns.get_loc("Recalc Insert")
    drop_list = cols[index :]
    dataframe = dataframe.drop(columns = drop_list, axis = 1)
        
    return dataframe

# This function will calculate the key metrics needed to assess viability of oob forecast
def calc_stats(data, curryr, currmon, first, sector_val):

    if not first:    
        data = drop_cols(data)

    # Calculate the 5 and 95 gap percentiles based on the initial levels prior to shims for use in flags (remember that a large gap is bad, so label bottom 5 with the 95 percentile value, and top 5 with the 5 percentile value)
    if first:
        data_filt = data.copy()
        if currmon == 1:
            data_filt = data_filt[(data_filt['yr'] == curryr - 1) & (data_filt['currmon'] == 12)]
        else:
            data_filt = data_filt[(data_filt['yr'] == curryr) & (data_filt['currmon'] == currmon - 1)]
        data_5 = data_filt.copy()
        data_5 = pd.DataFrame(data_5.groupby('subsector')['gap'].quantile(0.95))
        data_5.columns = ['gap_perc_5']
        data = data.join(data_5, on='subsector')
        data_95 = data_filt.copy()
        data_95 = pd.DataFrame(data_95.groupby('subsector')['gap'].quantile(0.05))
        data_95.columns = ['gap_perc_95']
        data = data.join(data_95, on='subsector')

        # Calculate the typical market rent growth for positive chg and for negative chg, for use in flag benchmarks
        pos_avg = data.copy()
        pos_avg = pos_avg[pos_avg['cons'] == 0]
        pos_avg = pos_avg[(pos_avg['currmon'] != currmon) | (pos_avg['yr'] != curryr)]
        pos_avg = pos_avg[pos_avg['yr'] >= curryr - 5]
        pos_avg = pos_avg[round(pos_avg['G_mrent'],3) > 0]
        pos_avg['g_avg_pos'] = pos_avg.groupby('subsector')['G_mrent'].transform('mean')
        pos_avg = pos_avg.drop_duplicates('subsector')
        data = data.join(pos_avg.set_index('subsector')[['g_avg_pos']], on='subsector')
        data['g_avg_pos'] = np.where((data['g_avg_pos'] < 0.015), 0.015, data['g_avg_pos'])

        neg_avg = data.copy()
        neg_avg = neg_avg[neg_avg['cons'] == 0]
        neg_avg = neg_avg[(neg_avg['currmon'] != currmon) | (neg_avg['yr'] != curryr)]
        neg_avg = neg_avg[neg_avg['yr'] >= curryr - 5]
        neg_avg = neg_avg[round(neg_avg['G_mrent'],3) < 0]
        neg_avg['g_avg_neg'] = neg_avg.groupby('subsector')['G_mrent'].transform('mean')
        neg_avg = neg_avg.drop_duplicates('subsector')
        data = data.join(neg_avg.set_index('subsector')[['g_avg_neg']], on='subsector')
        data['g_avg_neg'] = np.where((data['g_avg_neg'] > -0.007), -0.007, data['g_avg_neg'])
        

    # Create a column called Recalc Insert, so that we can drop everything created after it from the dataframe and the stats can be recalculated after a fix is entered
    data['Recalc Insert'] = 0
    
    # Calculate the percentile ranking for the relevant rent growth variables that will be used in a time series graph
    # Do this for absolute value, as trying to assess who has a high magnitude of change, regardles of direction
    cols = ['G_mrent', 'dqren10d', 'sub_g_renx_mo_wgt', 'met_g_renx_mo_wgt']
    abs_cols = ['abs_G_mrent', 'abs_dqren10d', 'abs_sub_g_renx_mo_wgt', 'abs_met_g_renx_mo_wgt']
    names = ['G_mrent_perc', 'dqren10d_perc', 'sub_g_renx_mo_wgt_perc', 'met_g_renx_mo_wgt_perc']
    data[abs_cols] = abs(data[cols])
    data[names] = data[(data['curr_tag'] == 1)][abs_cols].rank(pct=True)
    data.drop(abs_cols, axis=1, inplace=True)

    # Calculate the 12 month vac change and 12 month rent change for both published and square
    data['vac_chg_12'] = np.where((data['identity'] == data['identity'].shift(11)) & (data['curr_tag'] == 1), data['vac'] - data['vac'].shift(11), np.nan)
    data['sqvac_chg_12'] = np.where((data['identity'] == data['identity'].shift(11)) & (data['curr_tag'] == 1), data['sqvac'] - data['sqvac'].shift(11), np.nan)
    data['G_mrent_12'] = np.where((data['identity'] == data['identity'].shift(11)) & (data['curr_tag'] == 1), (data['mrent'] - data['mrent'].shift(11)) / data['mrent'].shift(11), np.nan)
    data['sq_Gmrent_12'] = np.where((data['identity'] == data['identity'].shift(11)) & (data['curr_tag'] == 1), (data['sqsren'] - data['sqsren'].shift(11)) / data['sqsren'].shift(11), np.nan)

    # Calculate the published YTD vac chg, market rent chg, and gap chg
    
    data['vac_chg_ytd'] = np.where(data['curr_tag'] == 1, data['vac'] - data['vac'].shift(periods=currmon), np.nan)
    data['G_mrent_ytd'] = np.where(data['curr_tag'] == 1, (data['mrent'] - data['mrent'].shift(periods=currmon)) / data['mrent'].shift(periods=currmon), np.nan)
    data['gap_chg_ytd'] = np.where(data['curr_tag'] == 1, data['gap'] - data['gap'].shift(periods=currmon), np.nan)

    return data

    