# -*- coding: utf-8 -*-
"""
Created on Wed Feb 15 09:05:10 2023

New script to run Simple Dispatch from

@author: emei3
"""

import os
import pickle
from simple_dispatch import generatorData

if __name__ == '__main__':
    
    ## simple dispatch setup, define path names
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
    egrid_data_xlsx = 'egrid2016_data.xlsx'
    eia923_schedule5_xlsx = 'EIA923_Schedules_2_3_4_5_M_12_2017_Final_Revision.xlsx'
    run_year = 2017
    
    #for nerc_region in ['TRE', 'MRO', 'WECC', 'SPP', 'SERC', 'RFC', 'FRCC', 'NPCC']:
    for nerc_region in ['SERC']:
        
        try: # get shortened pickeled dictionary if generatorData has already been run for the particular year and region
            # change path to simple dispatch output data folder
            abspath = os.path.abspath(__file__)
            dname = os.path.dirname(abspath)
            os.chdir(dname)
            os.chdir("../Data/Simple Dispatch Outputs") # where to access output data relative to code folder
            gd_short = pickle.load(open('generator_data_short_%s_%s.obj'%(nerc_region, str(run_year)), 'rb')) # load generatordata object
        except:
            #run the generator data object
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
            #pickle the trimmed version of the generator data object
            gd_short = {'year': gd.year, 'nerc': gd.nerc, 'hist_dispatch': gd.hist_dispatch, 'demand_data': gd.demand_data, 
                        'mdt_coal_events': gd.mdt_coal_events, 'df': gd.df}
            # change path to simple dispatch output data folder
            abspath = os.path.abspath(__file__)
            dname = os.path.dirname(abspath)
            os.chdir(dname)
            os.chdir("../Data/Simple Dispatch Outputs") # where to access output data relative to code folder
            pickle.dump(gd_short, open('generator_data_short_%s_%s.obj'%(nerc_region, str(run_year)), 'wb'))