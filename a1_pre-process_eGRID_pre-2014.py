# -*- coding: utf-8 -*-
"""
Created on Thu Feb 23 16:04:43 2023

Pre-processes eGRID2005, X, X, and X data to be consistent with eGRID2014+ inputs for Simple Dispatch

Prior to 2014, UNT sheet used to be BLR - also, the headings have somewhat changed over the years

@author: emei3
"""

import pandas as pd
import os
import numpy as np

#%% 2005 eGRID data
abspath = os.path.abspath(__file__)
base_dname = os.path.dirname(abspath)
os.chdir(base_dname) # change to code directory
os.chdir("../Data/Simple Dipatch Inputs/Raw")

egrid_fname = 'eGRID2005_plant.xls' # 2005-2006 data
egrid_year_str = '05'

## read in and process unit (boiler) data
egrid_unt = pd.read_excel(egrid_fname, 'BLR'+egrid_year_str, skiprows=5) # BLR sheet is proxy for UNT sheet
# retrieve needed columns
# deviations from 2016: UNITID = BLRID, no PRMVR (prime mover), FUELU1 = FUELB1, HTIAN = HTIBAN, NOXAN = NOXBAN, SO2AN = SO2BAN, CO2 = CO2BAN, HRSOP = LOADHRS
df_unt = egrid_unt[['PNAME', 'ORISPL', 'BLRID', 'FUELB1', 'HTIBAN', 'NOXBAN', 'SO2BAN', 'CO2BAN', 'LOADHRS', 'BLRYRONL']]
df_unt.columns = ['PNAME', 'ORISPL', 'UNITID', 'FUELU1', 'HTIAN', 'NOXAN', 'SO2AN', 'CO2AN', 'HRSOP', 'year_online_unt']

# fill in 0 in heat throughput, emissions, and year online columns with NA
cols = ['HTIAN', 'NOXAN', 'SO2AN', 'CO2AN', 'year_online_unt']
for col in cols:
    mask = (df_unt[col].eq(0))
    df_unt.loc[mask, col] = np.nan


## read in and process generator data
egrid_gen = pd.read_excel(egrid_fname, 'GEN'+egrid_year_str, skiprows=5)
# retrieve needed columns
# no deviations from 2016
egrid_gen['orispl_unit'] = egrid_gen['ORISPL'].map(str) + '_' + egrid_gen['GENID'].map(str)
df_gen = egrid_gen[['ORISPL', 'NAMEPCAP', 'GENNTAN', 'GENYRONL', 'orispl_unit', 'PRMVR', 'FUELG1']]


#%% add in a PRMVR column to unit data using generator data

## match prime mover data to ORISPL unit name
temp_df_unt = df_unt
temp_df_unt['orispl_unit'] = temp_df_unt['ORISPL'].astype(str)+'_'+temp_df_unt['UNITID'] # construct orispl_unit column
temp_df_unt = temp_df_unt.merge(df_gen[['orispl_unit', 'PRMVR', 'GENYRONL']], how='left', on=['orispl_unit'])

## match the rest based on majority rules within ORISPL and fuel type
# obtain majority prime mover data
temp_majority_prmvr = df_gen # remove non-generating generators; NOTE: play with this; see if removing these are better
temp_majority_prmvr = temp_majority_prmvr.groupby(['ORISPL', 'FUELG1'])[['PRMVR', 'GENYRONL']].agg(pd.Series.mode) # find most common PRMVR by generator + fuel
temp_majority_prmvr = temp_majority_prmvr.reset_index(level=['ORISPL', 'FUELG1']) # make indices columns
temp_majority_prmvr.columns = ['ORISPL', 'FUELU1', 'PRMVR', 'GENYRONL'] # rename columns to be consistent with unit (boiler) columns

# add prime mover data to unit data based on ORISPL and fuel type
temp_df_unt_leftover = temp_df_unt[temp_df_unt['PRMVR'].isna()] # get units with nan prime mover data
temp_df_unt_leftover = temp_df_unt_leftover.drop(['PRMVR', 'GENYRONL'], axis=1) # remove old PRMVR and GENYRONL 
temp_df_unt_leftover = temp_df_unt_leftover.merge(temp_majority_prmvr, how='left', on=['ORISPL', 'FUELU1']) # merge majority PRMVR data with remaining data
temp_df_unt_leftover['PRMVR'] = temp_df_unt_leftover['PRMVR'].map(lambda x: np.nan if isinstance(x, np.ndarray) else x) # replace multiple PRMVR with nan
# replace nan PRMVR from prior iteration with new PRMVR data
temp = temp_df_unt.drop(['PRMVR', 'GENYRONL'], axis=1) # temporarily drop these columns to avoid redundancy
temp_df_unt.loc[temp_df_unt['PRMVR'] # add only PRMVR and GENYRONL if original dataframe doesn't have them
                .isna(), ['PRMVR', 'GENYRONL']] = temp.merge(temp_df_unt_leftover, how='left', on=['ORISPL', 'UNITID'])[['PRMVR', 'GENYRONL']]

#%% match remaining to globally most common prime mover for the fuel type
temp_majority_prmvr = df_gen # remove non-generating generators; NOTE: play with this; see if removing these are better
temp_majority_prmvr = temp_majority_prmvr.groupby(['FUELG1'])[['PRMVR']].agg(pd.Series.mode) # find most common PRMVR by fuel
temp_majority_prmvr['PRMVR'] = temp_majority_prmvr['PRMVR'].map(lambda x: x[0] if isinstance(x, np.ndarray) else x) # replace multiple PRMVR with first instance
temp_majority_prmvr = temp_majority_prmvr.reset_index(level=['FUELG1']) # make indices columns
temp_majority_prmvr.columns = ['FUELU1', 'PRMVR'] # rename columns to be consistent with unit (boiler) columns

# add prime mover data to unit data based on fuel type
temp_df_unt_leftover = temp_df_unt[temp_df_unt['PRMVR'].isna()] # get units with nan prime mover data
temp_df_unt_leftover = temp_df_unt_leftover.drop(['PRMVR'], axis=1) # remove old PRMVR and GENYRONL 
temp_df_unt_leftover = temp_df_unt_leftover.merge(temp_majority_prmvr, how='left', on=['FUELU1']) # merge majority PRMVR data with remaining data
# replace nan PRMVR from prior iteration with new PRMVR data
temp = temp_df_unt.drop(['PRMVR'], axis=1) # temporarily drop these columns to avoid redundancy
temp_df_unt.loc[temp_df_unt['PRMVR'] # add only PRMVR and GENYRONL if original dataframe doesn't have them
                .isna(), ['PRMVR']] = temp.merge(temp_df_unt_leftover, how='left', on=['ORISPL', 'UNITID'])[['PRMVR']]

#%%

## read in and process plant data
egrid_plnt = pd.read_excel(egrid_fname, 'PLNT'+egrid_year_str, skiprows=4)
# retrieve needed columns
# deviations from 2016: no BACODE (balancing authority code)
df_plnt = egrid_plnt[['ORISPL', 'PSTATABB', 'NERC', 'SUBRGN', 'PLPRMFL', 'PLFUELCT']]

# fill in blank column for balancing authority
df_plnt[['BACODE']] = None

#%% column names to rename to
df_unt = df_unt[['PNAME', 'ORISPL', 'UNITID', 'PRMVR', 'FUELU1', 'HTIAN', 'NOXAN', 'SO2AN', 'CO2AN', 'HRSOP']]

df_gen = egrid_gen[['ORISPL', 'NAMEPCAP', 'GENNTAN', 'GENYRONL', 'orispl_unit', 'PRMVR', 'FUELG1']]