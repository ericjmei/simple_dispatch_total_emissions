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
df_unt = egrid_unt[['PNAME', 'ORISPL', 'BLRID', 'FUELB1', 'HTIBAN', 'NOXBAN', 'SO2BAN', 'CO2BAN', 'LOADHRS']]
df_unt.columns = ['PNAME', 'ORISPL', 'UNITID', 'FUELU1', 'HTIAN', 'NOXAN', 'SO2AN', 'CO2AN', 'HRSOP']

# fill in 0 in heat throughput and emissions columns with NA
cols = ['HTIAN', 'NOXAN', 'SO2AN', 'CO2AN']
mask = (df_unt[cols].eq(0).all(axis=1))
df_unt.loc[mask, cols] = np.nan


## read in and process generator data
egrid_gen = pd.read_excel(egrid_fname, 'GEN'+egrid_year_str, skiprows=5)
# retrieve needed columns
# no deviations from 2016
egrid_gen['orispl_unit'] = egrid_gen['ORISPL'].map(str) + '_' + egrid_gen['GENID'].map(str)
df_gen = egrid_gen[['ORISPL', 'NAMEPCAP', 'GENNTAN', 'GENYRONL', 'orispl_unit', 'PRMVR', 'FUELG1']]


#%% add in a PRMVR column to unit data using generator data
temp = df_gen.groupby(['ORISPL', 'FUELG1'])['PRMVR'].agg(pd.Series.mode)

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