# -*- coding: utf-8 -*-
"""
Created on Wed Feb 15 09:05:10 2023

Runs Simple Dispatch to simulate actual NERC emissions
Inputs: FERC 714, eGRID, EIA 923, EPA CEMS
Outputs: actual observed emissions and demand, modeled actual emissions and demand, generatorData object

@author: emei3
"""

import os
import pickle
import numpy as np
from simple_dispatch import generatorData
from simple_dispatch import bidStack
from simple_dispatch import dispatch

if __name__ == '__main__':
    
    ## simple dispatch setup, define path and file names
    # NOTE: working on getting the FERC and EIA data from PUDL
    
    # ferc 714 data from here: https://www.ferc.gov/docs-filing/forms/form-714/data.asp
    # ferc 714 ids available on the simple_dispatch github repository
    # egrid data from here: https://www.epa.gov/energy/emissions-generation-resource-integrated-database-egrid
    # eia 923 data from here: https://www.eia.gov/electricity/data/eia923/
    # cems data from here: ftp://newftp.epa.gov/DmDnLoad/emissions/hourly/monthly/
    # easiur data from here: https://barney.ce.cmu.edu/~jinhyok/easiur/online/
    # fuel_default_prices.xlsx compiled from data from https://www.eia.gov/
    input_folder_rel_path = "../Data/Simple Dipatch Inputs" # where to access input data relative to code folder
    ferc714_part2_schedule6_csv = 'Part 2 Schedule 6 - Balancing Authority Hourly System Lambda.csv'
    ferc714IDs_csv= 'Respondent IDs.csv'
    cems_folder_path ='../Data/CAMD/PUDL retrieved hourly' # relative path for all CEMS outputs
    easiur_csv_path ='egrid_2016_plant_easiur.csv'
    fuel_commodity_prices_xlsx = 'fuel_default_prices.xlsx'
    
    # these will change with every year
    run_year = 2018 ## specify run year
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
    
    
    # obtain code folder name for future folder changing
    abspath = os.path.abspath(__file__)
    base_dname = os.path.dirname(abspath)
    
    #for nerc_region in ['TRE', 'MRO', 'WECC', 'SPP', 'SERC', 'RFC', 'FRCC', 'NPCC']:
    for nerc_region in ['SERC', 'WECC', 'NPCC', 'RFC']:
        
        ## create/retrieve simple generator dispatch object
        try: # get shortened pickeled dictionary if generatorData has already been run for the particular year and region
            # change path to simple dispatch output data folder
            os.chdir(base_dname) 
            os.chdir("../Data/Simple Dispatch Outputs") # where to access output data relative to code folder
            gd_short = pickle.load(open('generator_data_short_%s_%s.obj'%(nerc_region, str(run_year)), 'rb')) # load generatordata object
        except:
            # run the generator data object
            gd = generatorData(nerc_region, 
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
                               hist_downtime=True, # should always be true
                               coal_min_downtime = 12, 
                               cems_validation_run=True) # makes sure only CEMS boilers are included in eGRID. We only need CEMS plants
            
            # pickle the trimmed version of the generator data object
            gd_short = {'year': gd.year, 'nerc': gd.nerc, 'hist_dispatch': gd.hist_dispatch, 'demand_data': gd.demand_data, 
                        'mdt_coal_events': gd.mdt_coal_events, 'df': gd.df}
            # change path to simple dispatch output data folder
            os.chdir(base_dname)
            os.chdir("../Data/Simple Dispatch Outputs") # where to access output data relative to code folder
            pickle.dump(gd_short, open('generator_data_short_%s_%s.obj'%(nerc_region, str(run_year)), 'wb'))
        
        ## save historical actual dispatch
        gd_short["hist_dispatch"].to_csv('actual_dispatch_'+nerc_region+'_'+str(run_year)+'.csv', index=False)
        
        ## create bidStack object and save merit order figures
        #run the bidStack object - use information about the generators (from gd_short) to create a merit order (bid stack) of the nerc region's generators
        week = 30 # week that bid stack is calculated
        bs = bidStack(gd_short, time=week, dropNucHydroGeo=True, include_min_output=True, 
                      mdt_weight=0.5) #NOTE: set dropNucHydroGeo to True if working with data that only looks at fossil fuels (e.g. CEMS)
        
        # produce bid stack plots
        bid_stack_cost = bs.plotBidStackMultiColor('gen_cost', plot_type='bar', fig_dim = (4,4), production_cost_only=True) #plot the merit order
        bid_stack_co2 = bs.plotBidStackMultiColor('co2', plot_type='bar') #plot CO2 emissions
        bid_stack_so2 = bs.plotBidStackMultiColor('so2', plot_type='bar') #plot SO2 emissions
        bid_stack_nox = bs.plotBidStackMultiColor('nox', plot_type='bar') #plot NOx emissions           
        
        # save plots
        # change path to simple dispatch merit order figures folder
        os.chdir(base_dname)
        os.chdir("../Figures/Simple Dispatch Merit Orders and Emissions") # where to save figures relative to code folder
        fn = nerc_region + '_' + str(run_year) + '_week_' + str(week) # base name for saving file
        bid_stack_cost.savefig(fn+"_merit_order_"+".png", dpi=bid_stack_cost.dpi*10) # save at plot dpi*10
        bid_stack_co2.savefig(fn+"_unit_emissions_co2"+".png", dpi=bid_stack_co2.dpi*10) # save at plot dpi*10
        bid_stack_so2.savefig(fn+"_unit_emissions_so2"+".png", dpi=bid_stack_so2.dpi*10) # save at plot dpi*10
        bid_stack_nox.savefig(fn+"_unit_emissions_nox"+".png", dpi=bid_stack_nox.dpi*10) # save at plot dpi*10
        
        
        ## run and save the dispatch object - use the nerc region's merit order (bs), a demand timeseries (gd.demand_data), 
        #  and a time array (default is array([ 1,  2, ... , 51, 52]) for 52 weeks to run a whole year)
        # change path to simple dispatch output data folder
        os.chdir(base_dname)
        os.chdir("../Data/Simple Dispatch Outputs") # where to access output data relative to code folder
        if not os.path.exists('simple_dispatch_'+nerc_region+'_'+str(run_year)+'.csv'): #if you've already run and saved the dispatch, skip this step
            #run the dispatch object
            dp = dispatch(bs, gd_short["demand_data"], time_array=np.arange(52)+1) #set up the object         
            dp.calcDispatchAll() #function that solves the dispatch for each time period in time_array (default for each week of the year)
            #save dispatch results 
            dp.df.to_csv('simple_dispatch_'+nerc_region+'_'+str(run_year)+'.csv', index=False)