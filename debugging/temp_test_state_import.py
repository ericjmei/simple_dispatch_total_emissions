# -*- coding: utf-8 -*-
"""
Created on Mon Mar 13 17:15:05 2023

@author: emei3
"""

import os
import pandas
import datetime

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)
os.chdir("../../Data/CAMD/PUDL retrieved hourly") # relative folder path from input

# self inputs
s = "GA"
year = 2008

# begin for loop
state = s.upper() # for data reading purposes
print("processing CEMS data from " + state + " for " + str(year))

# obtain hourly CEMS data for state and year
os.chdir("./"+state) # change to state's directory
df_cems_add = pandas.read_parquet('CEMS_hourly_'+state+'_'+str(year)+'.parquet')
os.chdir("..") # change to upstream directory

# split date time columns
date = df_cems_add["operating_datetime_utc"].dt.strftime("%m/%d/%Y") # retrieve mm/dd/yy date string
hour = df_cems_add["operating_datetime_utc"].dt.strftime("%H") # retrieve hour string
timeData_toAdd = pandas.DataFrame({'date': date,
                               'hour': hour}) # create new parallel dataframe with date and hour
df_cems_add = pandas.concat([df_cems_add, timeData_toAdd], axis=1) # concat to dataframe

# to_split = df_cems_add["operating_datetime_utc"] # datetime IN UTC - check that simple dispatch operates this way?
# to_split = to_split.tolist()
# date = [x.strftime("%m/%d/%Y") for x in to_split] # retrieve mm/dd/yy date string
# hour = [int(x.strftime("%H")) for x in to_split] # retrieve hour string
# timeData_toAdd = pandas.DataFrame({'date': date,
#                                'hour': hour}) # create new parallel dataframe with date and hour
# df_cems_add = pandas.concat([df_cems_add, timeData_toAdd], axis=1) # concat to dataframe

# grab necessary data and rename
# because data was pre-cleaned by PUDL, made choice to use EIA id - should test with EPA id as well
df_cems_add = df_cems_add[['plant_id_eia', 'emissions_unit_id_epa', 'date','hour', 'gross_load_mw', 
                           'so2_mass_lbs', 'nox_mass_lbs', 'co2_mass_tons', 'heat_content_mmbtu']].dropna()
df_cems_add.columns=['orispl', 'unit', 'date','hour','mwh', 'so2_tot', 'nox_tot', 'co2_tot', 'mmbtu']
#df_cems = pandas.concat([df_cems, df_cems_add])

#%% fix bug on end time slice that can't account for leap year
year = 2008
t = 52
start = (datetime.datetime.strptime(str(year) + '-01-01', '%Y-%m-%d') + datetime.timedelta(days=7.05*(t-1)-1)).strftime('%Y-%m-%d')
end = (datetime.datetime.strptime(str(year) + '-01-01', '%Y-%m-%d') + datetime.timedelta(days=7.05*(t)-1)).strftime('%Y-%m-%d') 
if (year % 4 == 0) & (t == 52):
    end = (datetime.datetime.strptime(str(year) + '-01-01', '%Y-%m-%d') + datetime.timedelta(days=7.05*(t))).strftime('%Y-%m-%d') 