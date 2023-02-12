# -*- coding: utf-8 -*-
"""
Created on Sun Feb 12 17:42:22 2023

to debug simple dispatch generator

@author: emei3
"""

import pandas
import numpy
import datetime
import os

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)
df_cems = pandas.read_parquet('df_cems_to_test.parquet')

year = 2017
nerc = 'SERC'

#%% start line 294
##calculate emissions rates and heat rate for each week and each generator
#rather than parsing the dates (which takes forever because this is such a big dataframe) we can create month and day columns for slicing the data based on time of year
df_orispl_unit = df_cems.copy(deep=True)
df_orispl_unit.date = df_orispl_unit.date.str.replace('/','-')
temp = pandas.DataFrame(df_orispl_unit.date.str.split('-').tolist(), columns=['month', 'day', 'year'], index=df_orispl_unit.index).astype(float)
df_orispl_unit['monthday'] = temp.year*10000 + temp.month*100 + temp.day


###
#loop through the weeks, slice the data, and find the average heat rates and emissions rates
## first, add a column 't' that says which week of the simulation we are in
df_orispl_unit['t'] = 52
for t in numpy.arange(52)+1: # add column to relevant rows
    start = (datetime.datetime.strptime(str(year) + '-01-01', '%Y-%m-%d') + datetime.timedelta(days=7.05*(t-1)-1)).strftime('%Y-%m-%d') 
    end = (datetime.datetime.strptime(str(year) + '-01-01', '%Y-%m-%d') + datetime.timedelta(days=7.05*(t)-1)).strftime('%Y-%m-%d') 
    start_monthday = float(start[0:4])*10000 + float(start[5:7])*100 + float(start[8:])
    end_monthday = float(end[0:4])*10000 + float(end[5:7])*100 + float(end[8:])
    #slice the data for the days corresponding to the time series period, t
    df_orispl_unit.loc[(df_orispl_unit.monthday >= start_monthday) & (df_orispl_unit.monthday < end_monthday), 't'] = t          

## process data
#remove outlier emissions and heat rates. These happen at hours where a generator's output is very low (e.g. less than 10 MWh). To remove these, we will remove any datapoints where mwh < 10.0 and heat_rate < 30.0 (0.5% percentiles of the 2014 TRE data).
# NOTE maybe should put print statement here for debugging purposes
percRemoved = ((df_orispl_unit.shape[0] - sum((df_orispl_unit.mwh >= 10.0) & (df_orispl_unit.heat_rate <= 30.0)))
               *100/df_orispl_unit.shape[0]) # percent of data that are outliers and will be removed
print(str(percRemoved)+"% data removed from "+nerc+" because <10 Mwh or <30 mmbtu")
df_orispl_unit = df_orispl_unit[(df_orispl_unit.mwh >= 10.0) & (df_orispl_unit.heat_rate <= 30.0)]
#aggregate by orispl_unit and t to get the heat rate, emissions rates, and capacity for each unit at each t
# NOTE need to add numeric_only spec, make sure same output
temp_2 = df_orispl_unit.groupby(['orispl_unit', 't'], as_index=False).agg('median')[['orispl_unit', 't', 'heat_rate', 'co2', 'so2', 'nox']].copy(deep=True)
temp_2['mw'] = df_orispl_unit.groupby(['orispl_unit', 't'], as_index=False).agg('max')['mwh'].copy(deep=True) # capacity is max mwh of week
#condense df_orispl_unit down to where we just have 1 row for each unique orispl_unit; NOTE: 
df_orispl_unit = df_orispl_unit.groupby('orispl_unit', as_index=False).agg('max')[['orispl_unit', 'orispl', 'state', 'ba', 'nerc', 'egrid', 'mwh']]
df_orispl_unit.rename(columns={'mwh':'mw'}, inplace=True)