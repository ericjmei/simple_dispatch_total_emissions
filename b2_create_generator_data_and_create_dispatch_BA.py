# -*- coding: utf-8 -*-
"""
Created on Tue Mar 21 16:58:34 2023

Runs Simple Dispatch to simulate actual balancing authority emissions subset to specific states
Inputs: FERC 714, eGRID, EIA 923, EPA CEMS
Outputs: actual observed emissions and demand, modeled actual emissions and demand, generatorData object

@author: emei3
"""

import os
import pickle
import numpy as np

# obtain code directory name for future folder changing
abspath = os.path.abspath(__file__)
base_dname = os.path.dirname(abspath)
os.chdir(base_dname)  # change to code directory

from simple_dispatch import generatorData
from simple_dispatch import bidStack
from simple_dispatch import dispatch

if __name__ == '__main__':
    
    ## simple dispatch setup, define path and file names
    
    # ferc 714 data from here: https://www.ferc.gov/docs-filing/forms/form-714/data.asp
    # ferc 714 ids available on the simple_dispatch github repository
    # egrid data from here: https://www.epa.gov/energy/emissions-generation-resource-integrated-database-egrid
    # eia 923 data from here: https://www.eia.gov/electricity/data/eia923/
    # cems data from here: ftp://newftp.epa.gov/DmDnLoad/emissions/hourly/monthly/
    # easiur data from here: https://barney.ce.cmu.edu/~jinhyok/easiur/online/
    # fuel_default_prices.xlsx compiled from data from https://www.eia.gov/
    input_folder_rel_path = "../Data/Simple Dispatch Inputs" # where to access input data relative to code folder
    output_rel_path = "../Data/Simple Dispatch Outputs/2023-05-10 act ba coal propagated/" # where to save output data
    ferc714_part2_schedule6_csv = 'Part 2 Schedule 6 - Balancing Authority Hourly System Lambda.csv'
    ferc714IDs_csv= 'Respondent IDs.csv'
    cems_folder_path ='../Data/CAMD/PUDL retrieved hourly' # relative path for all CEMS outputs
    easiur_csv_path ='egrid_2016_plant_easiur.csv'
    fuel_commodity_prices_xlsx = 'fuel_default_prices.xlsx'
    
    ## specify run year
    run_year = 2015
    # ## define states to subset
    # states_to_subset_all = [['GA', 'AL'], # SOCO
    #                         ['PA', 'NJ', 'DE', 'WV', 'OH', 'IL', 'NC', 'IN'], # PJM
    #                         ['CT'], # ISNE
    #                         ['NY']] # NYIS
    # ## define balancing authority regions to be run
    # ba_region_all = ['SOCO', 'PJM', 'ISNE', 'NYIS']
    # # define NERC regions that are parallel to balancing authority regions to be run
    # nerc_region_all = ['SERC', 'RFC', 'NPCC', 'NPCC']
    # # define NERC states for rename convention
    # ba_to_state_names = [['GA','AL','FL','MS'],
    #                       ['PA', 'NJ', 'DE', 'MD', 'VA', 'WV', 'OH', 'KY', 'MI', 'IL', 'NC', 'IN'],
    #                       ['ME', 'NH', 'VT', 'MA', 'CT', 'RI'],
    #                       ['NY']] 
    states_to_subset_all = [[]]
    ba_region_all = ['PJM']
    nerc_region_all = ['RFC']
    ba_to_state_names=[['PA', 'NJ', 'DE', 'MD', 'VA', 'WV', 'OH', 'KY', 'MI', 'IL', 'NC', 'IN']]
    
    ## these file paths will change with every year (automatically when run_year is set)
    eia923_schedule5_xlsx = 'EIA923_Schedules_2_3_4_5_M_12_'+str(run_year)+'_Final_Revision.xlsx' # EIA 923
    # different run years will have different eGRIDs
    if run_year == 2006:
        egrid_data_xlsx = 'egrid2005_data.xlsx'
    elif run_year == 2007 or run_year == 2008:
        egrid_data_xlsx = 'egrid2007_data.xlsx'
    elif run_year == 2009:
        egrid_data_xlsx = 'egrid2009_data.xlsx'
    elif run_year == 2010 or run_year == 2011:
        egrid_data_xlsx = 'egrid2010_data.xlsx'
    elif run_year == 2012 or run_year == 2013:
        egrid_data_xlsx = 'egrid2012_data.xlsx'
    elif run_year == 2014 or run_year == 2015:
        egrid_data_xlsx = 'egrid2014_data.xlsx'
    elif run_year == 2016 or run_year == 2017:
        egrid_data_xlsx = 'egrid2016_data.xlsx'
    elif run_year == 2018:
        egrid_data_xlsx = 'egrid2018_data.xlsx'
    elif run_year == 2019:
        egrid_data_xlsx = 'egrid2019_data.xlsx'
    
    
    for i, ba_region in enumerate(ba_region_all):
        
        ## create/retrieve simple generator dispatch object
        try: # get shortened pickeled dictionary if generatorData has already been run for the particular year and region
            # change path to simple dispatch output data folder
            os.chdir(base_dname) 
            os.chdir(output_rel_path) # where to access output data relative to code folder
            os.chdir('./Generator Data')
            gd_short = pickle.load(open('generator_data_short_%s_%s.obj'%(ba_region, str(run_year)), 'rb')) # load generatordata object
        except:
            # run the generator data object
            gd = generatorData(nerc_region_all[i], 
                               input_folder_rel_path=input_folder_rel_path,
                               egrid_fname=egrid_data_xlsx, 
                               eia923_fname=eia923_schedule5_xlsx, 
                               ferc714IDs_fname=ferc714IDs_csv, 
                               ferc714_fname=ferc714_part2_schedule6_csv, 
                               cems_folder=cems_folder_path, 
                               easiur_fname=easiur_csv_path, 
                               include_easiur_damages=False, # NOTE: we don't use any easiur damages. Variables relating to this have been commented out of the functions
                               year=run_year, 
                               fuel_commodity_prices_excel_dir=fuel_commodity_prices_xlsx, 
                               hist_downtime=True, 
                               coal_min_downtime = 12, 
                               cems_validation_run=True, # makes sure only CEMS boilers are included in eGRID. We only need CEMS plants
                               ba_code=ba_region) 
            
            # pickle the trimmed version of the generator data object
            gd_short = {'year': gd.year, 'nerc': gd.nerc, 'hist_dispatch': gd.hist_dispatch, 'demand_data': gd.demand_data, 
                        'mdt_coal_events': gd.mdt_coal_events, 'df': gd.df, 'ba_code':gd.ba_code}
            # change path to simple dispatch output data folder
            os.chdir(base_dname)
            os.chdir(output_rel_path) # where to access output data relative to code folder
            os.chdir('./Generator Data')
            pickle.dump(gd_short, open('generator_data_short_%s_%s.obj'%(ba_region, str(run_year)), 'wb'))
        
            # save historical actual dispatch
            os.chdir(base_dname) 
            os.chdir(output_rel_path)
            os.chdir("./Actual CEMS")
            fn = 'actual_CEMS_'+ba_region+'_'+'_'.join(ba_to_state_names[i])+'_'+str(run_year)+'.csv' # unique file name for particular NERC region
            gd_short["hist_dispatch"].to_csv(fn, index=False)
        
        states_to_subset = states_to_subset_all[i]
        ## create bidStack object and save merit order figures
        #run the bidStack object - use information about the generators (from gd_short) to create a merit order (bid stack) of the nerc region's generators
        week = 30 # week that bid stack is calculated
        bs = bidStack(gd_short, time=week, dropNucHydroGeo=True, include_min_output=True, 
                      states_to_subset=states_to_subset, mdt_weight=0.5) 
        
        ## run and save the dispatch object - use the nerc region's merit order (bs), a demand timeseries (gd.demand_data), 
        #  and a time array (default is array([ 1,  2, ... , 51, 52]) for 52 weeks to run a whole year)
        dp = dispatch(bs, gd_short["demand_data"], states_to_subset = states_to_subset, 
                      time_array=np.arange(52)+1) #set up the dispatch object         
        dp.calcDispatchAll() #function that solves the dispatch for each time period in time_array (default for each week of the year)
        
        #save dispatch results 
        # change path to simple dispatch output data folder
        os.chdir(base_dname)
        os.chdir(output_rel_path)
        fn = 'simple_dispatch_'+ba_region+'_'+'_'.join(ba_to_state_names[i])+'_'+str(run_year)+'.csv' # unique file name for particular NERC region
        dp.df.to_csv(fn, index=False) # save larger dispatch results
        # save subset results
        if states_to_subset != []:
            dp.df_subset.to_csv('simple_dispatch_'+ba_region+'_' + '_'.join(states_to_subset)+'_'+str(run_year)+'.csv', index=False)