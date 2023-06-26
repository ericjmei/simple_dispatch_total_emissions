# -*- coding: utf-8 -*-
"""
Created on Wed May  3 14:48:58 2023

script does 2 things:
1. edit all generator data objects such that they use maximum capacity over multiple years in a year instead of observed capacity in a given week
2. makes sure coal plants carry over to each year unless they are retired to capture plants that are off because of fuel prices

@author: emei3
"""

import os
import pickle
import pandas as pd
import numpy as np

# obtain code directory name for future folder changing
abspath = os.path.abspath(__file__)
base_dname = os.path.dirname(abspath)
os.chdir(base_dname)  # change to code directory
os.chdir("..") # change to simple dispatch

if __name__ == '__main__':
    
    
    ### inputs
    run_years = range(2006, 2020) # specify run years
    
    region = 'ISNE'
    is_counterfactual = True # NOTE: if only doing natural gas counterfactual, this needs to be set to false and fn_beginning_gd_short = 'counterfactual_'
    rel_path_input_generators_old = "../Data/Simple Dispatch Outputs/2023-06-23 cf/Generator Data Old"
    rel_path_input_generators_new = "../Data/Simple Dispatch Outputs/2023-06-23 cf/Generator Data"
    rel_path_fuel_prices = "../Data/Simple Dispatch Outputs/Fuel Price Metrics/7. ba regions edited" # fuel prices to replace gendata with if generator doesn't exist
    fn_beginning_fuel_prices = "actual_fuel_price_metrics_"
    rel_path_retirements = "../Data/Simple Dispatch Inputs" # retirement data
    rel_path_unit_to_gen = "../Data/Simple Dispatch Inputs" # keys for generators to units at each orispl
    rel_path_output = "../Data/Simple Dispatch Outputs/2023-06-23 cf/Generator Data"
    
    if is_counterfactual:
        fn_beginning_gd_short = 'counterfactual_'
    else:
        fn_beginning_gd_short = '' # 'counterfactual_'
    
    ## outputs to store all values
    inserted_units = dict() # for plants from prior years inserted into current year dataframe
    retired_units = dict() # for plants that are retired in the current year
    units_not_found = pd.DataFrame() # for units that are retired but can't be found in generator data
    
    ## load generator to unit mapping data
    os.chdir(base_dname)
    os.chdir(rel_path_unit_to_gen)
    unit_to_gen_raw = pd.read_excel("epa_eia_crosswalk.xlsx", sheet_name="epa_eia_crosswalk")
    # match EIA gen type to EPA if CAMD unmatched
    mask = unit_to_gen_raw['MATCH_TYPE_GEN'] == 'CAMD Unmatched'
    unit_to_gen_raw.loc[mask, 'EIA_GENERATOR_ID'] = unit_to_gen_raw.loc[mask, 'CAMD_GENERATOR_ID'].copy()
    # keep EPA orispl and unit ID and EIA gen ID
    unit_to_gen = unit_to_gen_raw.rename(columns={'CAMD_PLANT_ID':'orispl', 'CAMD_UNIT_ID':'unit', 'EIA_GENERATOR_ID':'generator'})
    # drop units with non-unique CAMD orispl and unit IDs and no matching EIA unit ID (boiler)
    mask = unit_to_gen.duplicated(subset=['orispl', 'unit'], keep=False)
    mask = mask & unit_to_gen['EIA_BOILER_ID'].isna()
    unit_to_gen.drop(index=unit_to_gen.loc[mask, :].index, inplace=True)
    
    ## read in retirements data
    os.chdir(base_dname)
    os.chdir(rel_path_retirements)
    generators_retired = pd.read_excel('retired_plants.xlsx', sheet_name='edited')
    generators_retired.rename(columns={'Plant ID':'orispl', 'Generator ID':'generator'}, inplace=True)
    # keep only 'sub' and 'bit'
    mask = generators_retired['Energy Source Code'].isin(['BIT', 'SUB'])
    generators_retired.drop(generators_retired.index[~mask], inplace=True)
    
    for run_year in run_years:
        ## read in fuel price data
        os.chdir(base_dname)
        os.chdir(rel_path_fuel_prices)
        if not is_counterfactual: # read that year's data
            fn = fn_beginning_fuel_prices+region+"_"+str(run_year)+".csv"
        else: # if counterfactual, just pull 2006 data
            fn = fn_beginning_fuel_prices+region+"_"+str(2006)+".csv"
        prices = pd.read_csv(fn)
        if is_counterfactual: # if counterfactual, set equal to 2006 average
            row_averages = prices[[f"average_no_outliers{suffix}" for suffix in range(1, 13)]].mean(axis=1)
            prices[[f"average_no_outliers{suffix}" for suffix in range(1, 13)]] = pd.concat([row_averages] * 12, axis=1)
        
        ## load generator data objects
        os.chdir(base_dname)
        os.chdir(rel_path_input_generators_old)
        gd_short = pickle.load(open(fn_beginning_gd_short+'generator_data_short_%s_%s.obj'%(region, str(run_year)), 'rb')) # load generatordata object
        generators_year_current = gd_short['df'].copy() # load generator data dataframe
        # prior generator dataframe should be in output folder unless it's the first year
        if run_year==2006: 
            run_year_prior = run_year # ensure no issues if starting at beginning
        else:
            os.chdir(base_dname)
            os.chdir(rel_path_input_generators_new)
            run_year_prior = run_year-1
            
        gd_short_prior = pickle.load(open(fn_beginning_gd_short+'generator_data_short_%s_%s.obj'%(region, str(run_year_prior)), 'rb')) # load prior year generatordata object
        generators_year_prior = gd_short_prior['df'].copy() # load prior year generator data dataframe
        # create unit id column
        generators_year_current['unit'] = generators_year_current['orispl_unit'].str.split('_', expand=True)[1]
        generators_year_prior['unit'] = generators_year_prior['orispl_unit'].str.split('_', expand=True)[1]
        
        ## map generators to units; not all units will have a matching generator (co-gen plants)
        generators_year_current = generators_year_current.merge(unit_to_gen[['orispl', 'unit', 'generator']],
                                                                on=['orispl', 'unit'],
                                                                how='left')
        generators_year_prior = generators_year_prior.merge(unit_to_gen[['orispl', 'unit', 'generator']],
                                                            on=['orispl', 'unit'],
                                                            how='left')
        # ensure no issues with expanding dataframes bc multiple generators to one unit
        generators_year_current.drop_duplicates(subset=['orispl_unit'], keep='first', inplace=True, ignore_index=True)
        generators_year_prior.drop_duplicates(subset=['orispl_unit'], keep='first', inplace=True, ignore_index=True)
            
        ### look for CFPP fired in prior year but not in current year (look through all generators in case of repowering)
        print('propagating coal units in ' + str(run_year))

        coal_generators_prior = generators_year_prior.loc[generators_year_prior['fuel_type'] == 'coal'][['orispl_unit', 'orispl', 'unit', 'generator']] 
        mask = coal_generators_prior['orispl_unit'].isin(generators_year_current['orispl_unit'])
        coal_generators_insert = coal_generators_prior.loc[~mask, :] # generators fired previously but not currently
        
        ## from these generators, remove ones that have been retired previously (catches generators retired in december, for instance)
        # retrieve prior retired generators
        mask = generators_retired['Retirement Year'] < run_year
        generators_retired_prior = generators_retired.loc[mask]
        # remove units that match orispl and generator
        matching_generators = coal_generators_insert.reset_index().merge(generators_retired_prior, on=['orispl', 'generator'], how='left').set_index('index')
        mask = ~matching_generators['Retirement Year'].isna()
        if len(mask) != 0:
            coal_generators_insert = coal_generators_insert.drop(index=coal_generators_insert.loc[mask, :].index)
        
        ## insert remaining generators
        # grab generators
        mask = generators_year_prior['orispl_unit'].isin(coal_generators_insert['orispl_unit'])
        generators_insert = generators_year_prior.loc[mask].copy()
        # take median for heat rate and ER (check that this is a reasonable assumption manually)
        suffixes = [str(i) for i in range(1, 53)] # weeks 1 to 52 to append to columns
        for prefix in ['heat_rate', 'co2', 'so2', 'nox']:
            column_names = [prefix+f"{suffix}" for suffix in suffixes]
            median = generators_insert[column_names].median(axis=1)
            for col in column_names:
                generators_insert.loc[:, col] = median
        # loop through each generator and populate fuel price
        for index, row in generators_insert.iterrows():
            # use fuel price of other generators of the same type at the plant, if available
            mask = ((generators_year_current['orispl_unit'] == generators_insert.loc[index, 'orispl_unit']) & 
                    (generators_year_current['fuel'] == generators_insert.loc[index, 'fuel']) &
                    (generators_year_current['prime_mover'] == generators_insert.loc[index, 'prime_mover']))
            generator_matching_type = generators_year_current[mask].copy()
            if not generator_matching_type.empty: # only proceed if generators appear
                suffixes = [str(i) for i in range(1, 53)] # weeks 1 to 52 to append to columns
                column_names = [f"fuel_price{suffix}" for suffix in suffixes]
                # set fuel price of generator to insert to median of fuel prices
                generators_insert.loc[index, column_names] = generator_matching_type.loc[:, column_names].median(axis=0)
                print('filling fuel prices of generator '+generators_insert.loc[index, 'orispl_unit']+' with similar generators.')
            
            # if no generators available, use average fuel price for that fuel type in that region
            else:
                suffixes = [str(i) for i in range(1, 53)] # weeks 1 to 52 to append to columns
                column_names = [f"fuel_price{suffix}" for suffix in suffixes]
                # retrieve average prices
                prices_to_fill = prices.loc[prices['fuel'] == generators_insert.loc[index, 'fuel'], 
                                            [f"average_no_outliers{suffix}" for suffix in range(1, 13)]]
                if (generators_insert.loc[index, 'fuel'] == 'rc') | (generators_insert.loc[index, 'fuel'] == 'wc'):
                    prices_to_fill = prices.loc[prices['fuel'] == 'bit', 
                                                [f"average_no_outliers{suffix}" for suffix in range(1, 13)]]    
                # turn average prices from monthly to weekly resolution; this is taken from Simple Dispatch
                month_weeks = [1, 5, 9, 14, 18, 22, 27, 31, 36, 40, 44, 48]
                prices_to_fill_week = pd.DataFrame()
                i = 0
                for week in np.arange(52)+1: # loop through all weeks
                    if week in month_weeks: # for weeks at first of month,
                        i += 1
                    prices_to_fill_week['fuel_price'+ str(week)] = prices_to_fill.loc[:, 'average_no_outliers'+str(i)] # update price for week column
                generators_insert.loc[index, column_names] = prices_to_fill_week.values.flatten()
                print('filling fuel prices of generator '+generators_insert.loc[index, 'orispl_unit']+' with average.')
                # check that average doesn't have nans
                if any(np.isnan(prices_to_fill_week.values.flatten())):
                    print('nan values in ' + generators_insert.loc[index, 'fuel'])
        
        # store inserted plant data in separate dataframe
        if not generators_insert.empty:
            inserted_units[run_year] = generators_insert
        # add generator to dataframe
        generators_year_current_new = pd.concat([generators_year_current, generators_insert], axis = 0, ignore_index=True)
        
        ### if maximum capacity for prior year is higher, replace mw capacity
        print('changing capacity in ' + str(run_year))
        # rename generators_year_prior column to merge (mw)
        generators_year_prior.rename(columns={'mw':'mw_old'}, inplace=True)
        # join prior year generator data to current year generator data
        # generator must have same fuel type and prime mover to make sure no fuel switching happened
        generators_year_current_new = generators_year_current_new.merge(generators_year_prior[['orispl_unit', 'fuel', 'prime_mover', 'mw_old']],
                                                on=['orispl_unit', 'fuel', 'prime_mover'],
                                                how='left')
        # replace mw with prior year's mw if it's higher
        generators_year_current_new['mw'] = generators_year_current_new['mw'].where(generators_year_current_new['mw'] >= generators_year_current_new['mw_old'].fillna(0), 
                                                            generators_year_current_new['mw_old'])
        generators_year_current_new.drop(['mw_old'], axis=1, inplace=True)
        # recalculate min_out
        generators_year_current_new['min_out'] = np.multiply(generators_year_current_new['mw'], generators_year_current_new['min_out_multiplier'])
        
        ## set all capacity each week to maximum observed capacity
        suffixes = [str(i) for i in range(1, 53)] # weeks 1 to 52 to append to 'mw'
        for col in [f"mw{suffix}" for suffix in suffixes]:
            generators_year_current_new[col] = generators_year_current_new['mw']
        
        ### use retirements data to remove plants that have retired mid-year
        print('retiring units in ' + str(run_year))
        def retire_unit(generator_input, retirement_month):
            """
            retires generator unit after retirement month by setting heat rate very high (moving it to the end of the dispatch)
            and setting all emissions rates to 0
    
            Parameters
            ----------
            generator_input : DataFrame
                generator data of unit to retire.
            retirement_month : int
                month of generator retirement.
    
            Returns
            -------
            generator_found : DataFrame
                processed generator data row.
    
            """
            
            generator_found = generator_input.copy()
            month_weeks = [1, 5, 9, 14, 18, 22, 27, 31, 36, 40, 44, 48] # first weeks that correspond to months in simple dispatch
            if retirement_month != 12: # retirement at 12th month treated as retirement in beginning of next year
                retirement_week = month_weeks[retirement_month] # retire units at beginning of the next month
                suffixes = [str(i) for i in range(retirement_week, 53)] # week numbers to append to column name
                # set heat rates to high value (50)
                for col in [f"heat_rate{suffix}" for suffix in suffixes]:
                    generator_found[col] = 50
                # set all emissions rates to 0
                for prefix in ['co2', 'so2', 'nox']:
                    for col in [prefix+suffix for suffix in suffixes]:
                        generator_found[col] = 0
            return generator_found
        
        ## check for plants that are matches
        mask = ((generators_retired['Retirement Year'] == run_year) # current year
                & (generators_retired['orispl'].isin(generators_year_current_new['orispl']))) # and orispls match
        generators_retired_current = generators_retired.loc[mask]
        
        ## retire units 
        generators_to_retire = pd.DataFrame() # to store processed dataframes
        for index, row in generators_retired_current.iterrows():
            # find units in plant with same fuel type and prime mover
            mask = ((generators_year_current_new['orispl'] == generators_retired_current.loc[index, 'orispl']) & 
                    (generators_year_current_new['generator'] == generators_retired_current.loc[index, 'generator']))
            generators_to_search = generators_year_current_new.loc[mask, :]
            
            # if 1+ generator found, retire unit. Otherwise, print error message
            if generators_to_search.shape[0] == 1:
                print('unit found with exact match: '+ generators_to_search['orispl_unit'].values[0])
                generators_to_retire = pd.concat([generators_to_retire, 
                                                  retire_unit(generators_to_search,  generators_retired_current.loc[index, 'Retirement Month'])],
                                                 axis=0)
            elif generators_to_search.shape[0] > 1:
                print('multiple units found with exact match: '+ ', '.join(generators_to_search['orispl_unit'].values))
                generators_to_retire = pd.concat([generators_to_retire, 
                                                  retire_unit(generators_to_search,  generators_retired_current.loc[index, 'Retirement Month'])],
                                                 axis=0)
            else:
                print('generator '+str(generators_retired_current.loc[index, 'orispl']) + ' ' + 
                      generators_retired_current.loc[index, 'generator'] + ' has found ' + str(generators_to_search.shape[0])
                      + ' units in gendata')
                units_not_found = pd.concat([units_not_found, generators_retired_current.loc[index].to_frame().T])
        
        # store generators in global output
        if not generators_to_retire.empty:
            retired_units[run_year] = generators_to_retire
        # insert generators back into dataframe
        generators_year_current_new.loc[generators_to_retire.index] = generators_to_retire
        
        ## write in new dataframe
        os.chdir(base_dname)
        os.chdir(rel_path_output)
        generators_year_current_new = generators_year_current_new.drop(columns='generator')
        gd_short['df'] = generators_year_current_new.copy()
        pickle.dump(gd_short, open(fn_beginning_gd_short+'generator_data_short_%s_%s.obj'%(region, str(run_year)), 'wb'))
    
    ### write metrics in new file
    os.chdir(base_dname)
    os.chdir(rel_path_output)
    writer = pd.ExcelWriter('generator_data_changes_'+region+'.xlsx')
    for year in inserted_units.keys():
        inserted_units[year].to_excel(writer, sheet_name='inserted '+str(year))
    
    for year in retired_units.keys():
        retired_units[year].to_excel(writer, sheet_name='retired '+str(year))
    writer.close()