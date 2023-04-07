# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 22:22:11 2023

log actual average fuel prices for the counterfactual scenario

@author: emei3
"""

import pandas
import scipy
import scipy.interpolate
import numpy
import os
import pickle


##inputs
input_folder_rel_path = "../Data/Simple Dispatch Inputs" # where to access input data relative to code folder
output_folder_actual_gd_rel_path = "../Data/Simple Dispatch Outputs/2023-03-23 Actual Scenario"
output_folder_rel_path = "../Data/Simple Dispatch Outputs/Fuel Price Metrics/Actual"
CPI_path = 'CPI-U_for_inflation.csv' # consumer price index data
nerc_region_all = ['SERC', 'RFC', 'NPCC', 'WECC']
years = range(2006, 2020)

## import consumer price index
# obtain code directory name for future folder changing
abspath = os.path.abspath(__file__)
base_dname = os.path.dirname(abspath)
os.chdir(base_dname)  # change to code directory
os.chdir(input_folder_rel_path)
CPI = pandas.read_csv(CPI_path) # 2005 to 2020 CPI-U

## add year and month columns
CPI['DATE'] = pandas.to_datetime(CPI['DATE']) # cast date column into datetime
CPI['year'] = CPI['DATE'].dt.year
CPI['month'] = CPI['DATE'].dt.month 

# function adjusts nominal dollars of a certain month and year to real 2006/1 dolalrs
def adjust_to_2006_1_real_dollars(nominal_dollars, year, month):
    CPI_2006 = CPI.loc[((CPI['year'] == 2006) & (CPI['month'] == 1)), 'CPIAUCSL'].values[0]
    CPI_current = CPI.loc[((CPI['year'] == year) & (CPI['month'] == month)), 'CPIAUCSL'].values[0]
    real_dollars = nominal_dollars * CPI_current/CPI_2006
    return real_dollars

# run for all years and NERC regions
for year in years:
    
    for nerc_region in nerc_region_all:
        os.chdir(base_dname)  # change to code directory
        os.chdir(input_folder_rel_path)
        
        # EIA 923
        eia923_fname = 'EIA923_Schedules_2_3_4_5_M_12_'+str(year)+'_Final_Revision.xlsx' 
        eia923 = pandas.read_excel(eia923_fname, 'Page 5 Fuel Receipts and Costs', skiprows=[0,1,2,3]) 
        eia923 = eia923.rename(columns={'Plant Id': 'orispl'})
        
        # df of fuel prices
        os.chdir(base_dname)  # change to code directory
        os.chdir(output_folder_actual_gd_rel_path)
        gd_short = pickle.load(open('generator_data_short_%s_%s.obj'%(nerc_region, str(year)), 'rb')) # load generatordata object
        
        ### taken pretty much exactly from the calcFuelPrices function in generatorData
        #we use eia923, where generators report their fuel purchases 
        df = eia923 # fuel purchase receipt form # NOTE: substituted from function
        df = df[['YEAR','MONTH','orispl','ENERGY_SOURCE','FUEL_GROUP','QUANTITY','FUEL_COST', 'Purchase Type']]
        df.columns = ['year', 'month', 'orispl' , 'fuel', 'fuel_type', 'quantity', 'fuel_price', 'purchase_type'] # rename columns
        df.fuel = df.fuel.str.lower()       
        # clean up prices
        df.loc[df.fuel_price=='.', 'fuel_price'] = scipy.nan # nan fuel price gets actual nan
        df.fuel_price = df.fuel_price.astype('float')/100.
        df = df.reset_index() # adds index as column    
        ## find unique monthly prices per orispl and fuel type
        #create empty dataframe to hold the results
        df2 = gd_short['df'].copy(deep=True)[['fuel','orispl','orispl_unit']] # NOTE: substituted from function
        orispl_prices = pandas.DataFrame(columns=['orispl_unit', 'orispl', 'fuel', 1,2,3,4,5,6,7,8,9,10,11,12, 'quantity']) # empty dataframe
        orispl_prices[['orispl_unit','orispl','fuel']] = df2[['orispl_unit', 'orispl', 'fuel']] # copies over cleanGeneratorData dataframe
        #populate the results by looping through the orispl_units to see if they have EIA923 fuel price data
        for o_u in orispl_prices.orispl_unit.unique(): # o_u is ORISPL_unit
            #grab 'fuel' and 'orispl'
            f = orispl_prices.loc[orispl_prices.orispl_unit==o_u].iloc[0]['fuel'] # fuel of unit
            o = orispl_prices.loc[orispl_prices.orispl_unit==o_u].iloc[0]['orispl'] # ORISPL of unit
            #find the weighted average monthly fuel price matching 'f' and 'o'
            temp = df[(df.orispl==o) & (df.fuel==f)][['month', 'quantity', 'fuel_price']] # finds fuel prices and quantities over all months
            if len(temp) != 0: # do this if dataframe is not empty; NOTE: does not mean that the fuel price is non-nan
                temp['weighted'] = numpy.multiply(temp.quantity, temp.fuel_price) # multiplies heat throughput by fuel price
                temp = temp.groupby(['month'], as_index=False).sum()[['month', 'quantity', 'weighted']] # calcs if multiple fuels are used
                temp['fuel_price'] = numpy.divide(temp.weighted, temp.quantity) # weighted average price of all fuels used per each month
                temp_prices = pandas.DataFrame({'month': numpy.arange(12)+1})
                temp_prices = temp_prices.merge(temp[['month', 'fuel_price']], on='month', how='left') # add fuel prices to dataframe
                temp_prices.loc[temp_prices.fuel_price.isna(), 'fuel_price'] = temp_prices.fuel_price.median() # populates nan months by median fuel price
                #add the monthly fuel prices into orispl_prices
                orispl_prices.loc[orispl_prices.orispl_unit==o_u, orispl_prices.columns.difference(['orispl_unit', 'orispl', 'fuel'])] = numpy.append(numpy.array(temp_prices.fuel_price),temp.quantity.sum())
        
        #add in additional purchasing information for slicing that we can remove later on; adds purchase type to the orispl prices dataframe
        orispl_prices = orispl_prices.merge(df[['orispl' , 'fuel', 'purchase_type']].drop_duplicates(subset=['orispl', 'fuel'], keep='first'), on=['orispl', 'fuel'], how='left')           
                
        #for any fuels that we have non-zero region level EIA923 data, apply those monthly fuel price profiles to other generators with the same fuel type but that do not have EIA923 fuel price data
        f_iter = list(orispl_prices[orispl_prices[1] != 0].dropna().fuel.unique()) # all types of unique fuels during this period
        if 'rc' in orispl_prices.fuel.unique(): # rc is refined coal
            f_iter.append('rc')
        for f in f_iter:
            orispl_prices_filled = orispl_prices[(orispl_prices.fuel==f) & (orispl_prices[1] != 0.0)].dropna().drop_duplicates(
                subset='orispl', keep='first').sort_values('quantity', ascending=0) # retrieves non-zero and non-nan units of particular fuel type
            #orispl_prices_empty = orispl_prices[(orispl_prices.fuel==f) & (orispl_prices[1].isna())]
            orispl_prices_empty = orispl_prices[(orispl_prices.fuel==f) & (orispl_prices[1]==0)].dropna(subset=['quantity']) #plants with some EIA923 data but no prices
            orispl_prices_nan = orispl_prices[(orispl_prices.fuel==f) & (orispl_prices['quantity'].isna())] #plants with no EIA923 data
            multiplier = 1.00
            
            #if lignite, use the national fuel-quantity-weighted median # NOTE: make print statement for how much this is of total generation
            if f == 'lig':
                #grab the 5th - 95th percentile prices
                temp = df[(df.fuel==f) & (df.fuel_price.notna())][['month', 'quantity', 'fuel_price', 'purchase_type']]
                temp = temp[(temp.fuel_price >= temp.fuel_price.quantile(0.05)) & (temp.fuel_price <= temp.fuel_price.quantile(0.95))]
                #weight the remaining prices according to quantity purchased
                temp['weighted'] = numpy.multiply(temp.quantity, temp.fuel_price)
                temp = temp.groupby(['month'], as_index=False).sum()[['month', 'quantity', 'weighted']]
                temp['fuel_price'] = numpy.divide(temp.weighted, temp.quantity)
                #build a dataframe that we can insert into orispl_prices
                temp_prices = pandas.DataFrame({'month': numpy.arange(12)+1})
                temp_prices = temp_prices.merge(temp[['month', 'fuel_price']], on='month', how='left')
                temp_prices.loc[temp_prices.fuel_price.isna(), 'fuel_price'] = temp_prices.fuel_price.median()
                #update orispl_prices for any units in orispl_prices_empty or orispl_prices_nan
                orispl_prices.loc[(orispl_prices.fuel==f) & 
                                  ((orispl_prices.orispl.isin(orispl_prices_empty.orispl)) | 
                                   (orispl_prices.orispl.isin(orispl_prices_nan.orispl))), 
                                  orispl_prices.columns.difference(['orispl_unit', 'orispl', 'fuel', 'purchase_type'])] = numpy.append(
                                      numpy.array(temp_prices.fuel_price),temp.quantity.sum())
        
            #if natural gas, sort by supplier type (contract, tolling, spot, or other)
            elif f =='ng': 
                orispl_prices_filled_0 = orispl_prices_filled.copy()
                orispl_prices_empty_0 = orispl_prices_empty.copy()
                #loop through the different purchase types and update any empties
                for pt in ['T', 'S', 'C']:  
                    orispl_prices_filled = orispl_prices_filled_0[orispl_prices_filled_0.purchase_type==pt]
                    orispl_prices_empty = orispl_prices_empty_0[orispl_prices_empty_0.purchase_type==pt]
                    multiplier = 1.00
                    #if pt == tolling prices, use a cheaper form of spot prices
                    if pt == 'T':
                        orispl_prices_filled = orispl_prices_filled_0[orispl_prices_filled_0.purchase_type=='S']
                        multiplier = 0.90
                    #of the plants with EIA923 data that we are assigning to plants without eia923 data, 
                    #we will use the plant with the highest energy production first, assigning its fuel price profile to one 
                    #of the generators that does not have EIA923 data. We will move on to plant with the next highest energy production and so on, 
                    #uniformly distributing the available EIA923 fuel price profiles to generators without fuel price data
                    loop = 0
                    loop_len = len(orispl_prices_filled) - 1 # ensure looping forwards from generating units with EIA923 price data
                    for o in orispl_prices_empty.orispl.unique(): # loop through ORISPL units with some EIA data but no prices
                        orispl_prices.loc[(orispl_prices.orispl==o) 
                                          & (orispl_prices.fuel==f), 
                                          orispl_prices.columns
                                          .difference(['orispl_unit', 'orispl', 'fuel', 'quantity', 'purchase_type'])] = numpy.array(
                                              orispl_prices_filled[orispl_prices.columns.difference(['orispl_unit', 'orispl', 'fuel', 'quantity', 'purchase_type'])]
                                              .iloc[loop]) * multiplier # populate monthly fuel price columns of generator with 0 data with the first generator with actual non-zero and non-nan data
                        #keep looping through the generators with eia923 price data until we have used all of their fuel price profiles, 
                        #then start again from the beginning of the loop with the plant with the highest energy production
                        if loop < loop_len:
                            loop += 1
                        else:
                            loop = 0                
                #for nan prices (those without any EIA923 information) use Spot, Contract, and Tolling Prices (i.e. all of the non-nan prices) 
                #update orispl_prices_filled to include the updated generators with previously 0 fuel price data
                orispl_prices_filled_new = orispl_prices[(orispl_prices.fuel==f) & (orispl_prices[1] != 0.0)].dropna().drop_duplicates(subset='orispl', keep='first').sort_values('quantity', ascending=0)
                #loop through the filled prices and use them for nan prices
                loop = 0
                loop_len = len(orispl_prices_filled_new) - 1
                for o in orispl_prices_nan.orispl.unique():
                    orispl_prices.loc[(orispl_prices.orispl==o) & 
                                      (orispl_prices.fuel==f), 
                                      orispl_prices.columns.difference(['orispl_unit', 'orispl', 'fuel', 'quantity', 'purchase_type'])] = numpy.array(
                                          orispl_prices_filled_new[orispl_prices.columns.difference(['orispl_unit', 'orispl', 'fuel', 'quantity', 'purchase_type'])]
                                          .iloc[loop]) # populate monthly fuel price columns of generator with 0 data with the first generator with actual non-zero and non-nan data
                    #keep looping through the generators with eia923 price data until we have used all of their fuel price profiles, then start again from the beginning of the loop with the plant with the highest energy production
                    if loop < loop_len:
                        loop += 1
                    else:
                        loop = 0     
            #otherwise            
            else:
                multiplier = 1.00
                #if refined coal, use subbitaneous prices * 1.15
                if f =='rc':
                    orispl_prices_filled = (orispl_prices[(orispl_prices.fuel=='sub') & (orispl_prices[1] != 0.0)].dropna()
                                            .drop_duplicates(subset='orispl', keep='first').sort_values('quantity', ascending=0))
                    multiplier = 1.1
                loop = 0
                loop_len = len(orispl_prices_filled) - 1
                #of the plants with EIA923 data that we are assigning to plants without eia923 data, 
                #we will use the plant with the highest energy production first, assigning its fuel price profile 
                #to one of the generators that does not have EIA923 data. We will move on to plant with the next highest energy production
                #and so on, uniformly distributing the available EIA923 fuel price profiles to generators without fuel price data
                for o in numpy.concatenate((orispl_prices_empty.orispl.unique(),orispl_prices_nan.orispl.unique())):
                    orispl_prices.loc[(orispl_prices.orispl==o) & (orispl_prices.fuel==f), 
                                      orispl_prices.columns.difference(['orispl_unit', 'orispl', 'fuel', 'quantity', 'purchase_type'])] = numpy.array(
                                          orispl_prices_filled[orispl_prices.columns.difference(['orispl_unit', 'orispl', 'fuel', 'quantity', 'purchase_type'])]
                                          .iloc[loop]) * multiplier
                    #keep looping through the generators with eia923 price data until we have used all of their fuel price profiles, 
                    # then start again from the beginning of the loop with the plant with the highest energy production
                    if loop < loop_len:
                        loop += 1
                    else:
                        loop = 0
        
        #and now we may have some nan values for fuel types that had no nerc_region eia923 data. We'll start with the national median for the EIA923 data.
        f_array = numpy.intersect1d(orispl_prices[orispl_prices[1].isna()].fuel.unique(), df.fuel[~df.fuel.isna()].unique())
        for f in f_array: 
            temp = df[df.fuel==f][['month', 'quantity', 'fuel_price']]
            temp['weighted'] = numpy.multiply(temp.quantity, temp.fuel_price)
            temp = temp.groupby(['month'], as_index=False).sum()[['month', 'quantity', 'weighted']]
            temp['fuel_price'] = numpy.divide(temp.weighted, temp.quantity)
            temp_prices = pandas.DataFrame({'month': numpy.arange(12)+1})
            temp_prices = temp_prices.merge(temp[['month', 'fuel_price']], on='month', how='left')
            temp_prices.loc[temp_prices.fuel_price.isna(), 'fuel_price'] = temp_prices.fuel_price.median()
            orispl_prices.loc[orispl_prices.fuel==f, orispl_prices.columns.difference(['orispl_unit', 'orispl', 'fuel', 'purchase_type'])] = numpy.append(
                numpy.array(temp_prices.fuel_price),temp.quantity.sum())
        
        ### calculate fuel averages
        ## adjust for inflation
        temp_orispl_prices = orispl_prices.copy(deep=True)
        temp_orispl_prices[[i for i in range(1, 13)]] = orispl_prices[range(1, 13)].apply(
            lambda x: adjust_to_2006_1_real_dollars(x, year, x.name)) # adjusts based on year and month
        
        ## create a dataframe to hold fuel_price_metrics: number of units, average, min, max, standard deviation
        unique_fuel_types = list(orispl_prices.fuel.unique()) # all types of unique fuels during this period
        fuel_price_metrics = pandas.DataFrame({'fuel': unique_fuel_types})
        
        # Repeat the 'average', 'standard deviation', 'min', and 'max' columns with suffixes from 1 to 12
        suffixes = [str(i) for i in range(1, 13)]
        cols_to_repeat = ['average', 'standard_deviation', 'min', 'max']
        new_cols = [f"{col}{suffix}" for col in cols_to_repeat for suffix in suffixes]
        new_cols = ['purchase_type', 'number_of_units', 'total_average', 'total_standard_deviation'] + new_cols # also append 'number of units' to the list
        fuel_price_metrics = pandas.concat([fuel_price_metrics, pandas.DataFrame(columns=new_cols)]) # append new empty columns
        
        # add purchase type to natural gas
        fuel_price_metrics.drop(fuel_price_metrics[fuel_price_metrics['fuel'] == 'ng'].index, inplace=True) # remove natrual gas from dataframe
        purchase_types = ['T', 'S', 'C', 'other', 'all'] # types of ng contracts
        rows_to_add = pandas.DataFrame({'fuel': ['ng', 'ng', 'ng', 'ng', 'ng'], # make new rows
                                        'purchase_type': purchase_types})
        fuel_price_metrics = pandas.concat([fuel_price_metrics, rows_to_add], axis=0) # append new rows
        
        ## iterate over all unique generator fuel types, adjusting the fuel prices if they exist in the avg_price_fuel_type dictionary
        #  and populating the fuel_price_metrics dataframe
        for fuel_type in unique_fuel_types:
            if fuel_type == 'ng':
                for purchase_type in purchase_types: # iterate over all contract types, same method as in last 'else' statement (that one is more readable)
                    # mask for units that have matching fuel type
                    mask = (temp_orispl_prices["fuel"] == fuel_type) & (temp_orispl_prices["purchase_type"] == purchase_type)
                    if purchase_type == 'other': # if other, retrieve all ng that are not the three purchase types
                        mask = ((temp_orispl_prices["fuel"] == fuel_type) & (temp_orispl_prices["purchase_type"] != 'T') 
                                & (temp_orispl_prices["purchase_type"] != 'S')  & (temp_orispl_prices["purchase_type"] != 'C'))
                    if purchase_type == 'all': # if all, retrieve all ng
                        mask = temp_orispl_prices["fuel"] == fuel_type
                    
                    # mask for fuel price metrics index
                    fuel_price_metrics_row = (fuel_price_metrics['fuel'] == fuel_type) & (fuel_price_metrics['purchase_type'] == purchase_type)
                    
                    ## calculate metrics (number of units, average, min, max, standard deviation)
                    fuel_price_metrics.loc[fuel_price_metrics_row, 'number_of_units'] = mask.sum() # number of units
                    temp = temp_orispl_prices.loc[mask, range(1, 13)] # retrieve all price data to manipulate
                    # average each week
                    fuel_price_metrics.loc[fuel_price_metrics_row, [f"average{suffix}" for suffix in suffixes]] = temp.mean(axis=0, skipna=True).values
                    # total average
                    fuel_price_metrics.loc[fuel_price_metrics_row, 'total_average'] = temp.stack().mean(axis=0, skipna=True)
                    # min before
                    fuel_price_metrics.loc[fuel_price_metrics_row, [f"min{suffix}" for suffix in suffixes]] = temp.min(axis=0, skipna=True).values
                    # max before
                    fuel_price_metrics.loc[fuel_price_metrics_row, [f"max{suffix}" for suffix in suffixes]] = temp.max(axis=0, skipna=True).values
                    # standard deviation 
                    fuel_price_metrics.loc[fuel_price_metrics_row, [f"standard_deviation{suffix}" for suffix in suffixes]] = temp.std(axis=0, skipna=True).values 
                    # total standard devation
                    fuel_price_metrics.loc[fuel_price_metrics_row, 'total_standard_deviation'] = temp.stack().std(axis=0, skipna=True)
            else:
                # mask for units that have matching fuel type
                mask = temp_orispl_prices["fuel"] == fuel_type
                # mask for fuel price metrics index
                fuel_price_metrics_row = fuel_price_metrics['fuel'] == fuel_type
                
                ## calculate metrics (number of units, average, min, max, standard deviation)
                fuel_price_metrics.loc[fuel_price_metrics_row, 'number_of_units'] = mask.sum() # number of units
                temp = temp_orispl_prices.loc[mask, range(1, 13)] # retrieve all price data to manipulate
                # average each week
                fuel_price_metrics.loc[fuel_price_metrics_row, [f"average{suffix}" for suffix in suffixes]] = temp.mean(axis=0, skipna=True).values
                # total average
                fuel_price_metrics.loc[fuel_price_metrics_row, 'total_average'] = temp.stack().mean(axis=0, skipna=True)
                # min before
                fuel_price_metrics.loc[fuel_price_metrics_row, [f"min{suffix}" for suffix in suffixes]] = temp.min(axis=0, skipna=True).values
                # max before
                fuel_price_metrics.loc[fuel_price_metrics_row, [f"max{suffix}" for suffix in suffixes]] = temp.max(axis=0, skipna=True).values
                # standard deviation 
                fuel_price_metrics.loc[fuel_price_metrics_row, [f"standard_deviation{suffix}" for suffix in suffixes]] = temp.std(axis=0, skipna=True).values 
                # total standard devation
                fuel_price_metrics.loc[fuel_price_metrics_row, 'total_standard_deviation'] = temp.stack().std(axis=0, skipna=True)
                
        ## write fuel_price_metrics to file
        os.chdir(base_dname)  # change to code directory
        os.chdir(output_folder_rel_path)
        fuel_price_metrics.to_csv('actual_fuel_price_metrics_'+nerc_region+'_'+str(year)+'.csv', index=False)