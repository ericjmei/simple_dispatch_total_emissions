# -*- coding: utf-8 -*-
"""
Created on Mon Jun 26 17:45:59 2023

stitch generator data objects from different balancing authorities (e.g., SOCO, TVA, AEC) into one region to run in Simple Dispatch

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

def addDummies(df):
    """ 
    Adds dummy "coal_0" and "ngcc_0" generators to df
    copied straight from simple_dispatch
    ---
    """
    df_out = df.copy(deep=True)
    #coal_0
    df_out.loc[len(df_out)] = df_out.loc[0] # new row
    df_out.loc[len(df_out)-1, df.columns.drop(['ba', 'nerc', 'egrid'])] = df_out.loc[0, df_out.columns.drop(['ba', 'nerc', 'egrid'])] * 0 # dummy 0 values for coal
    df_out.loc[len(df_out)-1,['orispl', 'orispl_unit', 'fuel', 'fuel_type', 'prime_mover', 'min_out_multiplier', 'min_out', 'is_coal']] = ['coal_0', 'coal_0', 'sub', 'coal', 'st', 0.0, 0.0, 1]
    #ngcc_0
    df_out.loc[len(df_out)] = df_out.loc[0] # new row
    df_out.loc[len(df_out)-1, df.columns.drop(['ba', 'nerc', 'egrid'])] = df_out.loc[0, df_out.columns.drop(['ba', 'nerc', 'egrid'])] * 0 # dummy 0 values for nat gas
    df_out.loc[len(df_out)-1,['orispl', 'orispl_unit', 'fuel', 'fuel_type', 'prime_mover', 'min_out_multiplier', 'min_out', 'is_gas']] = ['ngcc_0', 'ngcc_0', 'ng', 'gas', 'ct', 0.0, 0.0, 1]
    
    return df_out

def calcMdtCoalEvents(demand_data, coal_min_downtime=12):
    """ 
    Creates a dataframe of the start, end, and demand_threshold for each event in the demand data where we would expect a coal plant's minimum downtime constraint to kick in
    copied from simple dispatch
    ---
    """                      
    mdt_coal_events = demand_data.copy()
    mdt_coal_events['indices'] = mdt_coal_events.index
    #find the integral from x to x+
    mdt_coal_events['integral_x_xt'] = mdt_coal_events.demand[::-1].rolling(window=coal_min_downtime+1).sum()[::-1] # sum of demand between current hour and next X hours determined by min downtime
    #find the integral of a flat horizontal line extending from x
    mdt_coal_events['integral_x'] = mdt_coal_events.demand * (coal_min_downtime+1) # flat integral from multiplying by min downtime
    #find the integral under the minimum of the flat horizontal line and the demand curve
    def d_forward_convex_integral(mdt_index):
        try:
            return np.minimum(np.repeat(mdt_coal_events.demand[mdt_index], (coal_min_downtime+1)), mdt_coal_events.demand[mdt_index:(mdt_index+coal_min_downtime+1)]).sum()
        except:
            return mdt_coal_events.demand[mdt_index]   
    mdt_coal_events['integral_x_xt_below_x'] = mdt_coal_events.indices.apply(d_forward_convex_integral)
    #find the integral of the convex portion below x_xt
    mdt_coal_events['integral_convex_portion_btwn_x_xt'] = mdt_coal_events['integral_x'] - mdt_coal_events['integral_x_xt_below_x']
    #keep the convex integral only if x < 1.05*x+
    def d_keep_convex(mdt_index):
        try:
            return int(mdt_coal_events.demand[mdt_index] <= 1.05*mdt_coal_events.demand[mdt_index + coal_min_downtime]) * mdt_coal_events.integral_convex_portion_btwn_x_xt[mdt_index]
        except:
            return mdt_coal_events.integral_convex_portion_btwn_x_xt[mdt_index]   
    mdt_coal_events['integral_convex_filtered'] = mdt_coal_events.indices.apply(d_keep_convex)
    #mdt_coal_events['integral_convex_filtered'] = mdt_coal_events['integral_convex_filtered'].replace(0, np.nan)
    #keep any local maximums of the filtered convex integral
    mdt_coal_events['local_maximum'] = ((mdt_coal_events.integral_convex_filtered== mdt_coal_events.integral_convex_filtered.rolling(window=int(coal_min_downtime/2+1), center=True).max()) & (mdt_coal_events.integral_convex_filtered != 0) & (mdt_coal_events.integral_x >= mdt_coal_events.integral_x_xt))
    #spread the maximum out over the min downtime window
    mdt_coal_events = mdt_coal_events[mdt_coal_events.local_maximum]
    mdt_coal_events['demand_threshold'] = mdt_coal_events.demand
    mdt_coal_events['start'] = mdt_coal_events.datetime
    mdt_coal_events['end'] = mdt_coal_events.start + pd.DateOffset(hours=coal_min_downtime)
    mdt_coal_events = mdt_coal_events[['start', 'end', 'demand_threshold']]
    return mdt_coal_events     

if __name__ == '__main__':
    
    
    ### inputs
    run_years = range(2006, 2020) # specify run years
    
    nerc = 'SERC'
    input_region_names = ['SOCO', 'TVA', 'AEC']
    output_region_name = 'SE' # SE for southeast
    is_counterfactual = True # NOTE: if only doing natural gas counterfactual, this needs to be set to false and fn_beginning_gd_short = 'counterfactual_'
    rel_path_input_generators = "../Data/Simple Dispatch Outputs/2023-06-23 cf/Generator Data Old"
    # put in same folder as input so e2_generator_data_max_capacity_propagate_coal can work on entire region
    rel_path_output = "../Data/Simple Dispatch Outputs/2023-06-23 cf/Generator Data Old"
    
    if is_counterfactual:
        fn_beginning_gd_short = 'counterfactual_'
    else:
        fn_beginning_gd_short = '' # 'counterfactual_'
    
    for run_year in run_years:
        # output dataframe for combined generator objects
        generator_data_combined = pd.DataFrame()
        hist_dispatch_combined = pd.DataFrame()
        for region in input_region_names:
            ## import generator data short from simple dispatch
            os.chdir(base_dname)
            os.chdir(rel_path_input_generators)
            gd_short = pickle.load(open(fn_beginning_gd_short+'generator_data_short_%s_%s.obj'%(region, str(run_year)), 'rb')) # load generatordata object
            
            ## combine generator data
            generator_data_raw = gd_short['df'].copy() # load generator data dataframe
            # remove coal_0 and ngcc_0 and add to the dataframe
            mask = generator_data_raw['orispl_unit'].isin(['coal_0', 'ngcc_0'])
            generator_data = generator_data_raw.drop(index=generator_data_raw.loc[mask].index)
            generator_data_combined = pd.concat([generator_data_combined, generator_data], axis=0)
            
            ## combine historical dispatch
            hist_dispatch = gd_short['hist_dispatch'].copy()
            hist_dispatch_combined = pd.concat([hist_dispatch_combined, hist_dispatch], axis=0)
            hist_dispatch_combined = hist_dispatch_combined.resample('1H', on='datetime').sum()
            hist_dispatch_combined.reset_index(inplace=True)
        
        # add dummies back into dataframe and write back into object
        generator_data_combined = generator_data_combined.reset_index()
        generator_data_combined = addDummies(generator_data_combined)
        
        # create minimum downtime again for the re-constituted historical dispatch
        demand_data_combined = hist_dispatch_combined.copy()
        demand_data_combined.datetime = pd.to_datetime(demand_data_combined.datetime)
        demand_data_combined = demand_data_combined[['datetime', 'demand']]
        mdt_coal_events_combined = calcMdtCoalEvents(demand_data_combined)
        
        ## file back into gd_short and dump
        gd_short = {'year': run_year, 'nerc': nerc, 'hist_dispatch': hist_dispatch_combined, 'demand_data': demand_data_combined, 
                    'mdt_coal_events': mdt_coal_events_combined, 'df': generator_data_combined, 'ba_code':output_region_name+':'+', '.join(input_region_names)}
        os.chdir(base_dname)
        os.chdir(rel_path_output)
        pickle.dump(gd_short, open(fn_beginning_gd_short+'generator_data_short_%s_%s.obj'%(output_region_name, str(run_year)), 'wb'))
        