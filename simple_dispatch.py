# simple_dispatch
# Thomas Deetjen
# edits v28 and later by Eric Mei
# last edited: 2023-03-22
# v_21
# class "generatorData" turns CEMS, eGrid, FERC, and EIA data into a cleaned up dataframe for feeding into a "bidStack" object
# class "bidStack" creates a merit order curve from the generator fleet information created by the "generatorData" class
# class "dispatch" uses the "bidStack" object to choose which power plants should be operating during each time period to meet a demand time-series input
# ---
# v21:
# set up to simulate the 2017 historical dispatch for the NERC regional level
# v22:
# added some better functionality to allow co2, so2, and nox prices to be included in the dispatch. They were only being included in non-default calls of the bidStack.calcGenCost() function in v21. 
# fixed an error where bidStack was basing 'demand', 'f', 's', and 'a' off of 'mw' instead of ('mw' + str(self.time))
# fixed an error in bidStack.returnTotalFuelCost where [ind+1] was looking for a row in df that wasn't there. Changed this to [ind]. I'm not really sure how the model was solving before this without throwing errors about the index value not being there?
# updated the calculation of total production cost to reflect the FullMeritOrder
# fixed the calculation of full_"fuel"_mix_marg which was going above 1 and summing to more than 1 or less than 1 often
# added 'og' fuel to the 'gas' fuel_type in the generatorData class
# updated dispatch object to use returnFullTotalValue(demand, 'gen_cost_tot') for its 'gen_cost_tot' calculation
# v23:
# fixed an issue where putting in negative CO2 price could lead to a negative gen_cost which messed up the merit order (added a scipy.maximum(0.01, script here for calculating gen_cost) to the calcGenCost function).
# added an initialization variable to the bidStack object. It prevents the bidStack object from continually adding on new 0.0 dummy generators, and only does this once instead.
# added a new very large generator to the end of the merit order so that demand cannot exceed supply
# v24:
# added easiur environmental damages for each generator. These damages will be tracked through the dispatch object into the results. These damages are not included in the generation cost of the power plants, so they do not influence the power plant dispatch solution.
# fixed a minor bug that caused leap years to throw an error (changed "0" to "52" in df_orispl_unit['t'] = 52)
# now tracking gas and coal mmBtu consumption in the results
# now tracking production cost in the historic data. Production cost = mmtbu * fuel_price + mwh * vom where fuel_price and vom have the same assumptions as used in the dispatch model
# added the hist_downtime boolean to generatorData. If hist_downtime = True, 
# this allows you to use historical data to define the weekly capacity, heat rate, etc., which lets generators 
# be unavailable if they didn't produce any power during the historical week. This setting is useful for validation against historical data. 
# If hist_downtime = False, then for weeks when a generator is turned off, or its capacity, heat rate, or emissions rates are outside of the 
# 99th or 1st percentile of the historical data, then we assign the previously observed weekly value to that generator. 
# In this way we ignore maintenance but also allow for generators that did not show up in the dispatch historically to be dispatchable in scenario analysis 
# (e.g. if demand/prices are low, you might not see many GTs online - not because of maintenance but because of economics - but their capacity is still available for dispatch even though they didn't produce anything historically.)
# note that the dispatch goes a lot slower now. maybe 2x slower. Not exactly sure why, it could be from the addition of easiur damages and gas,coal,oil mmbtu in the output, 
# but I wouldn't expect these to take twice as long. Future versions of this code might benefit from some rewrites to try and improve the code's efficiency and transparency.
# updated returnMarginalGenerator to be 8x faster for floats. 
# updated returnTotalCost (and other returnTotal...) to be ~90x faster (yes, 90).
# the last 2 updates combined let us generate a TRE bidStack in 6.5 seconds instead of 50 seconds = ~8x faster. It reduced TRE dispatch from ~60 minutes to ~7 minutes.
# v25:
# updated bs.returnFullTotalValue to be 15x faster using similar methods as v24. This has a tiny amount of error associated with it 
#(less than 0.5% annually for most output variables). If we use the old returnFullTotalValue function with the new returnFullMarginalValue, the error goes to zero, but the solve time increases from 2.5 minutes to 4 minutes. 
# The error is small enough that I will leave as is, but we can always remove it later if something in the results seems strange. 
#Perhaps the linear interpolation is not quite accurate and there is something more nuanced going on. Or maybe I wasn't previously calculating it correctly. 
# updated bs.returnFullMarginalValue to be 5x faster using similar methods as v24. This reduced dispatch solve time by another 60%, brining the TRE dispatch down to 2.5 minutes. 
# v26:
# added an approximation of the minimum downtime constraint
# fixed an issue where year_online was being left off of many generators because the unit portion of the orispl_unit string isn't always consistent between egrid_unt and egrid_gen
# realized that WECC states had MN instead of MT and excluded NV
# fixed a problem where plants with nan fuel price data (i.e. plants in CEMS but not in EIA923) where not having their fuel prices correctly filled
# changed VOM to be a range based on generator age according to the data and discusison in the NREL Western Interconnection Study Phase 2
# made a variety of fuel price updates:
#   'rc' fuel price is now calculated as the price for 'sub' X 1.15. Refined coal has some additional processing that makes it more expensive.
#   'lig' fuel missing price data is now calculated as the national EIA923 average (there are not many LIG plants, so most regions don't have enough data to be helpful)
#   'ng' fuel missing price data is populated according to purchase type (contract, spot, or tolling) where tolling is assumed to be a bit cheaper. In this program tolling ng prices are popualated using 0.90 X spot prices
# v27:
# fixed an issue where coal that was being turned down in response to the min_downtime constraint was not contributing to the marginal generation
# updated some fuel and vom assumptions:
#   'rc' fuel price is now calculated as the price for 'sub' X 1.11. Refined coal has some additional processing that makes it more expensive.
#   'ng' fuel missing price data is populated according to purchase type (contract, spot, or tolling) where tolling prices (which EIA923 has no information on) are calculated as contract prices. Any nan prices (plants in CEMS that aren't in EIA923) are populated with spot, contract, and tolling prices
#   cleanGeneratorData now derates CHP plants according to their electricity : gross ratio
# v28:
# added a few more comments
# added capability to subset total emissions from groups of states within the NERC regions simulated
# added capability to create counterfactual generatorData object that is adjusted to specified average annual fuel price
# model can now run balancing authority regions as well as NERC regions; model regions will need to be added (only has SOCO, PJM, NYIS, and ISNE currently)
# fixued bug for hist_downtime = False where 'ffill' was populating in axis 0 instead of 1, which was giving units attributes (mw, heat_rate, etc.) from unrelated units



import pandas
import matplotlib.pylab
import scipy
import scipy.interpolate
import numpy
import datetime
import math
import copy
import os
import warnings
from bisect import bisect_left



class generatorData(object):
    def __init__(self, nerc, egrid_fname, input_folder_rel_path, eia923_fname, ferc714_fname='', ferc714IDs_fname='', cems_folder='', easiur_fname='', 
                 include_easiur_damages=False, year=2017, fuel_commodity_prices_excel_dir='', hist_downtime = True, coal_min_downtime = 12, cems_validation_run=True,
                 avg_price_fuel_type={}, CPI='', ba_code=''):
        """ 
        Translates the CEMS, eGrid, FERC, and EIA data into a dataframe for feeding into the bidStack class
        ---
        nerc : nerc region of interest (e.g. 'TRE', 'MRO', etc.)
        input_folder_rel_path : relative path of all input data (except for CEMS) compared to simple_dispatch
        egrid_fname : a .xlsx file name for the eGrid generator data
        eia923_fname : filename of eia form 923
        ferc714_fname : filename of nerc form 714 hourly system lambda 
        ferc714IDs_fname : filename that matches nerc 714 respondent IDs with nerc regions
        easiur_fname : filename containing easiur damages ($/tonne) for each power plant orispl
        include_easiur_damages : if True, then easiur damages will be added to the generator data. If False, we will skip that step.
        year : year that we're looking at (e.g. 2017)
        fuel_commodity_prices_excel_dir : filename of national EIA fuel prices (in case EIA923 data is empty)
        hist_downtime : if True, will use each generator's maximum weekly mw for weekly capacity. if False, will use maximum annual mw.
        coal_min_downtime : hours that coal plants must remain off if they shut down
        cems_validation_run : if True, then we are trying to validate the output against CEMS data and will only look at power plants included in the CEMS data\
        avg_price_fuel_type : dictionary of fuel types (and purchase type if natural gas) and average price to set monthly fuel prices equal to for
            counterfactual analysis of short-term price changes
        CPI : consumer price index file name that spans the time period of interest from https://fred.stlouisfed.org/series/CPIAUCSL
            NOTE: you don't actually need this unless you're interested in the real dollar adjustment in fuel prices
                the relative positions of generators will shift proportionally to the fuel prices set in avg_price_fuel_type
        ba_code: balancing authority code to run in lieu of NERC regions. NERC region still needs to be inputted for addElecPriceToDemandData(), but it won't
            have an overall impact on the emissions generated. Only has SOCO, ISNE, PJM, and NYIS so far, but more can be added easily
        """
        ## read in the data
        
        # change to input folder
        abspath = os.path.abspath(__file__)
        dname = os.path.dirname(abspath)
        os.chdir(dname)
        os.chdir(input_folder_rel_path)
        
        # edited to parquet files upon first read - this makes the entire process much faster on subsequent runs
        self.nerc = nerc
        self.ba_code = ba_code
        egrid_year_str = egrid_fname[7:9] #grab last two digits of egrid year; NOTE: file name must be XXXXXXX14XX..., etc.
        print('Reading in unit level data from eGRID...')
        try:
            self.egrid_unt = pandas.read_parquet(egrid_fname.split('.')[0]+'_UNT.parquet')
        except:
            self.egrid_unt = pandas.read_excel(egrid_fname, 'UNT'+egrid_year_str, skiprows=[0]) 
            self.egrid_unt.to_parquet(egrid_fname.split('.')[0]+'_UNT.parquet', index=False)
        print('Reading in generator level data from eGRID...')
        try:
            self.egrid_gen = pandas.read_parquet(egrid_fname.split('.')[0]+'_GEN.parquet')
        except:
            self.egrid_gen = pandas.read_excel(egrid_fname, 'GEN'+egrid_year_str, skiprows=[0])
            self.egrid_gen.to_parquet(egrid_fname.split('.')[0]+'_GEN.parquet', index=False)
        print('Reading in plant level data from eGRID...')
        try:
            self.egrid_plnt = pandas.read_parquet(egrid_fname.split('.')[0]+'_PLNT.parquet')
        except:
            self.egrid_plnt = pandas.read_excel(egrid_fname, 'PLNT'+egrid_year_str, skiprows=[0])
            self.egrid_plnt.to_parquet(egrid_fname.split('.')[0]+'_PLNT.parquet', index=False)
        
        print('Reading in data from EIA Form 923...')
        eia923 = pandas.read_excel(eia923_fname, 'Page 5 Fuel Receipts and Costs', skiprows=[0,1,2,3]) 
        eia923 = eia923.rename(columns={'Plant Id': 'orispl'})
        self.eia923 = eia923
        eia923_1 = pandas.read_excel(eia923_fname, 'Page 1 Generation and Fuel Data', skiprows=[0,1,2,3,4]) 
        eia923_1 = eia923_1.rename(columns={'Plant Id': 'orispl'})
        self.eia923_1 = eia923_1
              
        print('Reading in data from FERC Form 714...')
        try:
            self.ferc714 = pandas.read_parquet(ferc714_fname.split('.')[0]+'.parquet')
        except:
            self.ferc714 = pandas.read_csv(ferc714_fname) 
            self.ferc714.to_parquet(ferc714_fname.split('.')[0]+'.parquet', index=False)
        
        try:
            self.ferc714_ids = pandas.read_parquet(ferc714IDs_fname.split('.')[0]+'.parquet')
        except:
            self.ferc714_ids = pandas.read_csv(ferc714IDs_fname) 
            self.ferc714_ids.to_parquet(ferc714IDs_fname.split('.')[0]+'.parquet', index=False)
        
        # other data
        self.cems_folder = cems_folder # we only want data from CEMS anyway
        self.easiur_per_plant = pandas.read_csv(easiur_fname) 
        self.fuel_commodity_prices = pandas.read_excel(fuel_commodity_prices_excel_dir, str(year)) # needs custom updating
        self.cems_validation_run = cems_validation_run 
        self.hist_downtime = hist_downtime
        self.coal_min_downtime = coal_min_downtime
        self.year = year
        self.avg_price_fuel_type = avg_price_fuel_type
        self.CPI = CPI
        # if shifting average fuel prices, read in that dictionary as well
        if bool(avg_price_fuel_type): # execute only if dictionary is not empty
            
            print("Reading in CPI data...")
            CPI = pandas.read_csv(CPI) # consumer price index data
            # add year and month columns
            CPI['DATE'] = pandas.to_datetime(CPI['DATE']) # cast date column into datetime
            CPI['year'] = CPI['DATE'].dt.year
            CPI['month'] = CPI['DATE'].dt.month
            self.CPI = CPI
        
        ## data cleaning
        self.cleanGeneratorData() # converts eGRID and CEMS data to df of generator units and df of all CEMS data in NERC region
        self.addGenVom() # calculates VOM prices based on unit age
        self.calcFuelPrices() # calculates fuel prices using EIA 923 form 5 data
        if include_easiur_damages:
            self.easiurDamages()
        self.addGenMinOut() # calculates generator minimum MW capacity
        self.addDummies() # adds dummy coal and nat gas plants
        self.calcDemandData() # calculates historical dispatch-  demand, emissions, and generation mix - using CEMS data and prior assembled dfs
        self.addElecPriceToDemandData() # calculates historical electrical prices from FERC data
        self.demandTimeSeries() # slices just the datetime and demand columns of historical dispatch
        self.calcMdtCoalEvents() # returns minimum downtime events relevant for coal plants
        

    def cleanGeneratorData(self):
        """ 
        Converts the eGrid and CEMS data into a dataframe usable by the bidStack class.
        ---
        Creates
        self.df : has 1 row per generator unit or plant. columns describe emissions, heat rate, capacity, fuel, grid region, etc. This dataframe will be used to describe the generator fleet and merit order.
        self.df_cems : has 1 row per hour of the year per generator unit or plant. columns describe energy generated, emissions, and grid region. This dataframe will be used to describe the historical hourly demand, dispatch, and emissions
        """
        #copy in the egrid data and merge it together. In the next few lines we use the eGRID excel file to bring in unit level data 
        # for fuel consumption and emissions, generator level data for capacity and generation, and plant level data for fuel type and grid region. Then we compile it together to get an initial selection of data that defines each generator.
        print('Cleaning eGRID Data...')
        
        ##unit-level data: prime mover type, fuel type, heat input, NOx, SO2, CO2, and hours online
        df = self.egrid_unt.copy(deep=True)
        #rename columns
        # NOTE: 2005, UNITID = BLRID, no PRMVR, FUELU1 = FUELB1, HTIAN = [HTIEAN, HTIFAN]?, NOXAN = [NOXEAN NOXFAN]?, SO2AN = [SO2EAN,SO2FAN]?, CO2 = [CO2EAN,CO2FAN]?, HRSOP = LOADHRS
        df = df[['PNAME', 'ORISPL', 'UNITID', 'PRMVR', 'FUELU1', 'HTIAN', 'NOXAN', 'SO2AN', 'CO2AN', 'HRSOP']]
        df.columns = ['gen', 'orispl', 'unit', 'prime_mover', 'fuel', 'mmbtu_ann', 'nox_ann', 'so2_ann', 'co2_ann', 'hours_on']
        df['orispl_unit'] = df.orispl.map(str) + '_' + df.unit.map(str) #orispl_unit is a unique tag for each generator unit
        #drop nan fuel
        df = df[~df.fuel.isna()]
        
        ##gen-level data: contains MW capacity and MWh annual generation data, generator fuel, generator online year
        df_gen = self.egrid_gen.copy(deep=True) 
        df_gen['orispl_unit'] = df_gen['ORISPL'].map(str) + '_' + df_gen['GENID'].map(str) #orispl_unit is a unique tag for each generator unit
        # create two different dataframes for generator: one short and one long
        df_gen_long = df_gen[['ORISPL', 'NAMEPCAP', 'GENNTAN', 'GENYRONL', 'orispl_unit', 'PRMVR', 'FUELG1']].copy()
        df_gen_long.columns = ['orispl', 'mw', 'mwh_ann', 'year_online', 'orispl_unit', 'prime_mover', 'fuel']
        df_gen = df_gen[['NAMEPCAP', 'GENNTAN', 'GENYRONL', 'orispl_unit']] # short
        df_gen.columns = ['mw', 'mwh_ann', 'year_online', 'orispl_unit']
        
        ##plant-level data: contains fuel, fuel_type, balancing authority, nerc region, and egrid subregion data
        df_plnt = self.egrid_plnt.copy(deep=True) 
        # grab unique fuel types
        df_plnt_fuel = df_plnt[['PLPRMFL', 'PLFUELCT']] # plant primary fuel and plant primary fuel category
        df_plnt_fuel = df_plnt_fuel.drop_duplicates('PLPRMFL')
        df_plnt_fuel.PLFUELCT = df_plnt_fuel.PLFUELCT.str.lower()
        df_plnt_fuel.columns = ['fuel', 'fuel_type']
        # grab all geography
        # NOTE: 2005, no BACODE (balancing authority code)
        df_plnt = df_plnt[['ORISPL', 'PSTATABB', 'BACODE', 'NERC', 'SUBRGN']]
        df_plnt.columns = ['orispl', 'state', 'ba', 'nerc', 'egrid']
       
        ## merge these egrid data together at the unit-level
        df = df.merge(df_gen, how='left', on='orispl_unit')
        df = df.merge(df_plnt, how='left', on='orispl')  
        df = df.merge(df_plnt_fuel, how='left', on='fuel')  
        # keep only the units in the nerc/balancing authority region we're analyzing
        if self.ba_code == '':
            df = df[df.nerc == self.nerc]
        else:
            df = df[df.ba == self.ba_code]
        
        ## calculate the emissions rates # NOTE: double check that eGRID units in tons
        with numpy.errstate(divide='ignore'):
            df['co2'] = numpy.divide(df.co2_ann,df.mwh_ann) * 907.185 #tons to kg # replaced scipy with numpy divide
            df['so2'] = numpy.divide(df.so2_ann,df.mwh_ann) * 907.185 #tons to kg
            df['nox'] = numpy.divide(df.nox_ann,df.mwh_ann) * 907.185 #tons to kg
        
        ## analyze empty years
        #for empty year online, look at orispl in egrid_gen instead of egrid_unit # NOTE: all year_online is from eGRID_gen?? May need to re-run with eGRID_unt
        df.loc[df.year_online.isna(), 'year_online'] = (df[df.year_online.isna()][['orispl', 'prime_mover', 'fuel']] # retrieve nan year online
                                                        .merge(df_gen_long[['orispl', 'prime_mover', 'fuel', 'year_online']]
                                                               .groupby(['orispl', 'prime_mover', 'fuel'], as_index=False)
                                                               .agg('mean'), on=['orispl', 'prime_mover', 'fuel'])['year_online']) # average year online for egrid_gen
        #for any remaining empty year onlines, assume self.year (i.e. that they are brand new)
        df.loc[df.year_online.isna(), 'year_online'] = scipy.zeros_like(df.loc[df.year_online.isna(), 'year_online']) + self.year
        
        
        ###
        #now sort through and compile CEMS data. The goal is to use CEMS data to characterize each generator unit. 
        #So if CEMS has enough information to describe a generator unit we will over-write the eGRID data. 
        #If not, we will use the eGRID data instead. (CEMS data is expected to be more accurate because it has 
        #actual hourly performance of the generator units that we can use to calculate their operational characteristics. 
        #eGRID is reported on an annual basis and might be averaged out in different ways than we would prefer.)
        print('Compiling CEMS data...')
        #dictionary of which states are in which nerc/balancing authority region (b/c CEMS file downloads have the state in the filename)
        states = {'FRCC': ['fl'], 
                  'WECC': ['ca','or','wa', 'nv','mt','id','wy','ut','co','az','nm','tx'],
                  'SPP' : ['nm','ks','tx','ok','la','ar','mo'],
                  'RFC' : ['mi','in','oh','wv','md','pa','nj', 'il', 'ky', 'wi', 'va'],
                  'NPCC' : ['ny','ct','de','ri','ma','vt','nh','me'],
                  'SERC' : ['mo','ar','la','ms','tn','ky','il','va','al','ga','sc','nc', 'tx', 'fl'],
                  'MRO': ['ia','il','mi','mn','mo','mt','nd','ne','sd','wi','wy'], 
                  'TRE': ['ok','tx'],
                  # balancing authorities
                  'SOCO': ['GA','AL','FL','MS'],
                  'PJM': ['PA', 'NJ', 'DE', 'MD', 'VA', 'WV', 'OH', 'KY', 'MI', 'IL', 'NC', 'IN'],
                  'ISNE': ['ME', 'NH', 'VT', 'MA', 'RI', 'CT'],
                  'NYIS': ['NY']}
        #compile the different months of CEMS files into one dataframe, df_cems. 
        df_cems = pandas.DataFrame()
        # change to correct data path
        abspath = os.path.abspath(__file__)
        dname = os.path.dirname(abspath)
        os.chdir(dname)
        os.chdir(self.cems_folder) # relative folder path from input
        if self.ba_code == '':
            states_to_retrieve = states[self.nerc]
        else:
            states_to_retrieve = states[self.ba_code]
        
        for s in states_to_retrieve:
            state = s.upper() # for data reading purposes
            print("processing CEMS data from " + state + " for " + str(self.year))
            
            # obtain hourly CEMS data for state and year
            os.chdir("./"+state) # change to state's directory
            df_cems_add = pandas.read_parquet('CEMS_hourly_local_'+state+'_'+str(self.year)+'.parquet')
            os.chdir("..") # change to upstream directory
            
            # split date time columns
            date = df_cems_add["operating_datetime"].dt.strftime("%m/%d/%Y") # retrieve mm/dd/yy date string
            hour = df_cems_add["operating_datetime"].dt.strftime("%H") # retrieve hour string
            timeData_toAdd = pandas.DataFrame({'date': date,
                                           'hour': hour}) # create new parallel dataframe with date and hour
            df_cems_add = pandas.concat([df_cems_add, timeData_toAdd], axis=1) # concat to dataframe

            # grab necessary data and rename
            # because data was pre-cleaned by PUDL, made choice to use EIA id - should test with EPA id as well
            df_cems_add = df_cems_add[['plant_id_eia', 'emissions_unit_id_epa', 'date','hour', 'gross_load_mw', 
                                       'so2_mass_lbs', 'nox_mass_lbs', 'co2_mass_tons', 'heat_content_mmbtu']].dropna()
            df_cems_add.columns=['orispl', 'unit', 'date','hour','mwh', 'so2_tot', 'nox_tot', 'co2_tot', 'mmbtu']
            df_cems = pandas.concat([df_cems, df_cems_add])
            
        #create the 'orispl_unit' column, which combines orispl and unit into a unique tag for each generation unit
        df_cems['orispl_unit'] = df_cems['orispl'].map(str) + '_' + df_cems['unit'].map(str)
        #bring in geography data and only keep generators within NERC/balancing authority region
        df_cems = df_cems.merge(df_plnt, how='left', on='orispl') # add data such as egrid subregion and balancing authority
        if self.ba_code == '':
            df_cems = df_cems[df_cems['nerc']==self.nerc] 
        else:
            df_cems = df_cems[df_cems['ba']==self.ba_code] 
        #convert emissions to kg; NOTE: double check units!
        df_cems.co2_tot = df_cems.co2_tot * 907.185 #tons to kg
        df_cems.so2_tot = df_cems.so2_tot * 0.454 #lbs to kg
        df_cems.nox_tot = df_cems.nox_tot * 0.454 #lbs to kg
        
        ## calculate the hourly heat and emissions rates. Later we will take the medians over each week to define the generators weekly heat and emissions rates.
        df_cems['heat_rate'] = df_cems.mmbtu / df_cems.mwh
        df_cems['co2'] = df_cems.co2_tot / df_cems.mwh
        df_cems['so2'] = df_cems.so2_tot / df_cems.mwh
        df_cems['nox'] = df_cems.nox_tot / df_cems.mwh
        df_cems.replace([scipy.inf, -scipy.inf], scipy.nan, inplace=True) #don't want inf messing up median calculations
        #drop any bogus data. For example, the smallest mmbtu we would expect to see is 
        #25MW(smallest unit) * 0.4(smallest minimum output) * 6.0 (smallest heat rate) = 60 mmbtu. 
        #Any entries with less than 60 mmbtu fuel or less than 6.0 heat rate, let's get rid of that row of data.
        df_cems = df_cems[(df_cems.heat_rate >= 6.0) & (df_cems.mmbtu >= 60)] # NOTE: double check assumption above
        
        ##calculate emissions rates and heat rate for each week and each generator
        #rather than parsing the dates (which takes forever because this is such a big dataframe) we can create month and day columns for slicing the 
        #data based on time of year
        df_orispl_unit = df_cems.copy(deep=True)
        df_orispl_unit.date = df_orispl_unit.date.str.replace('/','-')
        temp = pandas.DataFrame(df_orispl_unit.date.str.split('-').tolist(), columns=['month', 'day', 'year'], index=df_orispl_unit.index).astype(float)
        df_orispl_unit['monthday'] = temp.year*10000 + temp.month*100 + temp.day
        
        
        ###
        #loop through the weeks, slice the data, and find the average heat rates and emissions rates
        ## first, add a column 't' that says which week of the simulation we are in
        df_orispl_unit['t'] = 52
        for t in numpy.arange(52)+1: # add column to relevant rows
            start = (datetime.datetime.strptime(str(self.year) + '-01-01', '%Y-%m-%d') + datetime.timedelta(days=7.05*(t-1)-1)).strftime('%Y-%m-%d') 
            end = (datetime.datetime.strptime(str(self.year) + '-01-01', '%Y-%m-%d') + datetime.timedelta(days=7.05*(t)-1)).strftime('%Y-%m-%d') 
            if (self.year % 4 == 0) & (t == 52): # account for leap years, add in extra day in last week
                end = (datetime.datetime.strptime(str(self.year) + '-01-01', '%Y-%m-%d') + datetime.timedelta(days=7.05*(t))).strftime('%Y-%m-%d') 
            start_monthday = float(start[0:4])*10000 + float(start[5:7])*100 + float(start[8:])
            end_monthday = float(end[0:4])*10000 + float(end[5:7])*100 + float(end[8:])
            #slice the data for the days corresponding to the time series period, t
            df_orispl_unit.loc[(df_orispl_unit.monthday >= start_monthday) & (df_orispl_unit.monthday < end_monthday), 't'] = t          
        
        ## make columns for every t week and each variable
        #remove outlier emissions and heat rates. These happen at hours where a generator's output is very low (e.g. less than 10 MWh). 
        #To remove these, we will remove any datapoints where mwh < 10.0 and heat_rate < 30.0 (0.5% percentiles of the 2014 TRE data).
        percRemoved = ((df_orispl_unit.shape[0] - sum((df_orispl_unit.mwh >= 10.0) & (df_orispl_unit.heat_rate <= 30.0)))
                       *100/df_orispl_unit.shape[0]) # percent of data that are outliers and will be removed
        if self.ba_code == '':
            print(str(percRemoved)+"% data removed from "+self.nerc+" because <10 Mwh or <30 mmbtu")
        else:
            print(str(percRemoved)+"% data removed from "+self.ba_code+" because <10 Mwh or <30 mmbtu")
        df_orispl_unit = df_orispl_unit[(df_orispl_unit.mwh >= 10.0) & (df_orispl_unit.heat_rate <= 30.0)]
        #aggregate by orispl_unit and t to get the heat rate, emissions rates, and capacity for each unit at each t
        # NOTE need to add numeric_only spec, make sure same output
        temp_2 = df_orispl_unit.groupby(['orispl_unit', 't'], as_index=False).agg('median')[['orispl_unit', 't', 'heat_rate', 'co2', 'so2', 'nox']].copy(deep=True)
        temp_2['mw'] = df_orispl_unit.groupby(['orispl_unit', 't'], as_index=False).agg('max')['mwh'].copy(deep=True) # capacity is max mwh of week
        #condense df_orispl_unit down to where we just have 1 row for each unique orispl_unit 
        # finds max capacity for each unit
        df_orispl_unit = df_orispl_unit.groupby('orispl_unit', as_index=False).agg('max')[['orispl_unit', 'orispl', 'state', 'ba', 'nerc', 'egrid', 'mwh']]
        df_orispl_unit.rename(columns={'mwh':'mw'}, inplace=True)
        
        for c in ['heat_rate', 'co2', 'so2', 'nox', 'mw']: # loop through each variable
            # sets index to ORISPL_unit and t (week number), then lists each variable+week number in its own column 
            temp_3 = temp_2.set_index(['orispl_unit', 't'])[c].unstack().reset_index() # makes matrix; rows are orispl_unit and columns are X variable for each week
            temp_3.columns = list(['orispl_unit']) + ([c + str(a) for a in numpy.arange(52)+1]) # make sure naming convention correct
            if not self.hist_downtime: ## if we want to use the max MW for unit capacity instead of total
                #remove any outlier values in the 1st or 99th percentiles
                max_array = temp_3.copy().drop(columns='orispl_unit').quantile(0.99, axis=1) # max in any row
                min_array = temp_3.copy().drop(columns='orispl_unit').quantile(0.01, axis=1) # min in any row
                median_array = temp_3.copy().drop(columns='orispl_unit').median(axis=1) # median in any row
                for i in temp_3.index: # loops through each unit in dataframe
                    test = temp_3.drop(columns='orispl_unit').iloc[i]
                    test[test > max_array[i]] = scipy.NaN # removes any value above 99 percentile
                    test[test < min_array[i]] = scipy.NaN # removes any value below 1 percentile
                    test = list(test) #had a hard time putting the results back into temp_3 without using a list
                    #if the first entry in test is nan, we want to fill that with the median value so that we can use ffill later
                    if math.isnan(test[0]):
                        test[0] = median_array[i]
                    test.insert(0, temp_3.iloc[i].orispl_unit) # adds unit name to list
                    temp_3.iloc[i] = test 
                    
            #for any nan values (assuming these are offline generators without any output data), 
            #fill nans with a large heat_rate that will move the generator towards the end of the merit order and large-ish emissions rate, so if the generator is dispatched in the model
            #it will jack up prices but emissions won't be heavily affected (note, previously I just replaced all nans with 99999, 
            #but I was concerned that this might lead to a few hours of the year with extremely high emissions numbers that threw off the data)
            #M here defines the heat rate and emissions data we will give to generators that were not online in the historical data
            M = float(scipy.where(c=='heat_rate', 50.0, # NOTE, emissions need to be edited to match high-ish emissions that make sense for each year
                                  scipy.where(c=='co2', 1500.0, scipy.where(c=='so2', 4.0, scipy.where(c=='nox', 3.0, scipy.where(c=='mw', 0.0, 99.0)))))) 
            #if we are using hist_downtime, then replace scipy.NaN with M. That way offline generators can still be dispatched, but they will have high cost and high emissions.
            if self.hist_downtime:
                temp_3 = temp_3.fillna(M)
            #if we are not using hist_downtime, then use ffill to populate the scipy.NaN values. 
            #This allows us to use the last observed value for the generator to populate data that we don't have for it. 
            #For example, if generator G had a heat rate of 8.5 during time t-1, but we don't have data for time t, 
            #then we assume that generator G has a heat rate of 8.5 for t. 
            #When we do this, we can begin to include generators that might be available for dispatch but were not turned on because prices were too low. 
            #However, we also remove any chance of capturing legitimate maintenance downtime that would impact the historical data. 
            #So, for validation purposes, we probably want to have hist_downtime = True. 
            #For future scenario analysis, we probably want to have hist_downtime = False.
            if not self.hist_downtime:
                temp_3 = temp_3.fillna(method='ffill', axis=1)
                # re-cast objects into floats so that later methods don't freak out
                suffixes = [str(i) for i in range(1, 53)]
                for col in [c + f"{suffix}" for suffix in suffixes]:
                    temp_3[col] = pandas.to_numeric(temp_3[col])
            #merge temp_3 with df_orispl_unit. Now we have weekly heat rates, emissions rates, and capacities for each generator. 
            #These values depend on whether we are including hist_downtime
            df_orispl_unit = df_orispl_unit.merge(temp_3, on='orispl_unit', how='left')
        
        ## merge df_orispl_unit into df. Now we have a dataframe with weekly heat rate and emissions rates for any plants in CEMS with that data. 
        #There will be some nan values in df for those weekly columns (e.g. 'heat_rate1', 'co223', etc.) that we will want to fill with annual averages from eGrid for now
        # NOTE: IGNORE THIS BECAUSE WE ARE ONLY USING CEMS
        orispl_units_egrid = df.orispl_unit.unique()
        orispl_units_cems = df_orispl_unit.orispl_unit.unique()
        df_leftovers = df[df.orispl_unit.isin(scipy.setdiff1d(orispl_units_egrid, orispl_units_cems))] # finds units in egrid but not cems
        #if we're doing a cems validation run, we only want to include generators that are in the CEMS data
        if self.cems_validation_run: # removes all df_leftovers, so makes next few steps moot
            df_leftovers = df_leftovers[df_leftovers.orispl_unit.isin(orispl_units_cems)]
        #remove any outliers - fuel is solar, wind, waste-heat, purchased steam, or other, less than 25MW capacity, less than 88 operating hours (1% CF), mw = nan, mmbtu = nan
        if self.year >= 2014: # hours are weird for eGRID pre-2014, so only remove 88 operating hours for years 2014 or after
            df_leftovers = df_leftovers[(df_leftovers.fuel!='SUN') & (df_leftovers.fuel!='WND') & (df_leftovers.fuel!='WH') & (df_leftovers.fuel!='OTH') & (df_leftovers.fuel!='PUR') &
                                        (df_leftovers.mw >=25) & (df_leftovers.hours_on >=88) & (~df_leftovers.mw.isna()) & (~df_leftovers.mmbtu_ann.isna())]
        else: # outlier removal without considering hours_on
            df_leftovers = df_leftovers[(df_leftovers.fuel!='SUN') & (df_leftovers.fuel!='WND') & (df_leftovers.fuel!='WH') & (df_leftovers.fuel!='OTH') & 
                                        (df_leftovers.fuel!='PUR') & (df_leftovers.mw >=25) & (~df_leftovers.mw.isna()) & (~df_leftovers.mmbtu_ann.isna())]
        
            
        #remove any outliers that have 0 emissions (except for nuclear)
        df_leftovers = df_leftovers[~((df_leftovers.fuel!='NUC') & (df_leftovers.nox_ann.isna()))]
        df_leftovers['cf'] = df_leftovers.mwh_ann / (df_leftovers.mw *8760.)
        #drop anything with capacity factor less than 1%
        df_leftovers = df_leftovers[df_leftovers.cf >= 0.01]
        df_leftovers.fillna(0.0, inplace=True)
        df_leftovers['heat_rate'] = df_leftovers.mmbtu_ann / df_leftovers.mwh_ann
        #add in the weekly time columns for heat rate and emissions rates. 
        #In this case we will just apply the annual average to each column, but we still need those columns to be able to 
        #concatenate back with df_orispl_unit and have our complete set of generator data
        for e in ['heat_rate', 'co2', 'so2', 'nox', 'mw']:
            for t in numpy.arange(52)+1:
                if e == 'mw':
                    if self.hist_downtime:
                        df_leftovers[e + str(t)] = df_leftovers[e]
                    if not self.hist_downtime:
                        df_leftovers[e + str(t)] = df_leftovers[e].quantile(0.99) # NOTE: trying to get this to stop giving warning
                else: 
                    df_leftovers[e + str(t)] = df_leftovers[e]
        df_leftovers.drop(columns = ['gen', 'unit', 'prime_mover', 'fuel', 'mmbtu_ann', 'nox_ann', 'so2_ann', 'co2_ann', 
                                     'mwh_ann', 'fuel_type', 'co2', 'so2', 'nox', 'cf', 'heat_rate', 'hours_on', 'year_online'], inplace=True)   
        #concat df_leftovers and df_orispl_unit
        df_orispl_unit = pandas.concat([df_orispl_unit, df_leftovers])
                
        #use df to get prime_mover, fuel, and fuel_type for each orispl_unit
        df_fuel = df[df.orispl_unit.isin(df_orispl_unit.orispl_unit.unique())][['orispl_unit', 'fuel', 'fuel_type', 'prime_mover', 'year_online']]
        df_fuel.fuel = df_fuel.fuel.str.lower()
        df_fuel.fuel_type = df_fuel.fuel_type.str.lower()
        df_fuel.prime_mover = df_fuel.prime_mover.str.lower()
        df_orispl_unit = df_orispl_unit.merge(df_fuel, on='orispl_unit', how='left')
        #if we are using, for example, 2017 CEMS and 2016 eGrid, there may be some powerplants without fuel, fuel_type, prime_mover, and year_online data. 
        #Let's assume 'ng', 'gas', 'ct', and 2017 for these units based on trends on what was built in 2017; 
        # NOTE: NEED TO CHANGE THESE FOR EACH YEAR
        df_orispl_unit.loc[df_orispl_unit.fuel.isna(), ['fuel', 'fuel_type']] = ['ng', 'gas']
        df_orispl_unit.loc[df_orispl_unit.prime_mover.isna(), 'prime_mover'] = 'ct'
        df_orispl_unit.loc[df_orispl_unit.year_online.isna(), 'year_online'] = self.year
        #also change 'og' to fuel_type 'gas' instead of 'ofsl' (other fossil fuel)
        df_orispl_unit.loc[df_orispl_unit.fuel=='og', ['fuel', 'fuel_type']] = ['og', 'gas']
        df_orispl_unit.fillna(0.0, inplace=True)
        #add in some columns to aid in calculating the fuel mix
        for f_type in ['gas', 'coal', 'oil', 'nuclear', 'hydro', 'geothermal', 'biomass']:
            df_orispl_unit['is_'+f_type.lower()] = (df_orispl_unit.fuel_type==f_type).astype(int)
            
        ###
        #derate any CHP - (combined heat and power) units according to their ratio of electric fuel consumption : total fuel consumption
        #now use EIA Form 923 to flag any CHP units and calculate their ratio of total fuel : fuel used for electricity. We will use those ratios to de-rate the mw and emissions of any generators that have a CHP-flagged orispl
        #calculate the elec_ratio that is used for CHP derating
        chp_derate_df = self.eia923_1.copy(deep=True) # copy entire EIA generator dataframe
        chp_derate_df = chp_derate_df[(chp_derate_df.orispl.isin(df_orispl_unit.orispl)) & (chp_derate_df['Combined Heat And\nPower Plant']=='Y')].replace('.', 0.0) # grabs plants classified as CHP that are also in CEMS
        chp_derate_df = (chp_derate_df[['orispl', 'Reported\nFuel Type Code', 'Elec_Quantity\nJanuary', 'Elec_Quantity\nFebruary', 'Elec_Quantity\nMarch', 
                                       'Elec_Quantity\nApril', 'Elec_Quantity\nMay', 'Elec_Quantity\nJune', 'Elec_Quantity\nJuly', 'Elec_Quantity\nAugust', 
                                       'Elec_Quantity\nSeptember', 'Elec_Quantity\nOctober', 'Elec_Quantity\nNovember', 'Elec_Quantity\nDecember', 
                                       'Quantity\nJanuary', 'Quantity\nFebruary', 'Quantity\nMarch', 'Quantity\nApril', 'Quantity\nMay', 'Quantity\nJune', 
                                       'Quantity\nJuly', 'Quantity\nAugust', 'Quantity\nSeptember', 'Quantity\nOctober', 'Quantity\nNovember', 'Quantity\nDecember']]
                         .groupby(['orispl', 'Reported\nFuel Type Code'], as_index=False).agg('sum')) # sums electricity consumption (elec quantity) and electricity and heat consumption (quantity) for each fuel type of each ORISPL
        chp_derate_df['elec_ratio'] = (chp_derate_df[['Elec_Quantity\nJanuary', 'Elec_Quantity\nFebruary', 'Elec_Quantity\nMarch', 'Elec_Quantity\nApril', 
                                                      'Elec_Quantity\nMay', 'Elec_Quantity\nJune', 'Elec_Quantity\nJuly', 'Elec_Quantity\nAugust', 
                                                      'Elec_Quantity\nSeptember', 'Elec_Quantity\nOctober', 'Elec_Quantity\nNovember', 'Elec_Quantity\nDecember']]
                                       .sum(axis=1) / chp_derate_df[['Quantity\nJanuary', 'Quantity\nFebruary', 'Quantity\nMarch', 'Quantity\nApril', 'Quantity\nMay',
                                                                     'Quantity\nJune', 'Quantity\nJuly', 'Quantity\nAugust', 'Quantity\nSeptember', 'Quantity\nOctober',
                                                                     'Quantity\nNovember', 'Quantity\nDecember']].sum(axis=1)) # elec ratio is electricity generated/(electricity and heat generated)
        chp_derate_df = chp_derate_df[['orispl', 'Reported\nFuel Type Code', 'elec_ratio']].dropna() # removes all columns but fuel type and ratio for each ORISPL
        chp_derate_df.columns = ['orispl', 'fuel', 'elec_ratio']    
        chp_derate_df.fuel = chp_derate_df.fuel.str.lower()
        mw_cols = ['mw','mw1','mw2','mw3','mw4','mw5','mw6','mw7','mw8','mw9','mw10','mw11','mw12','mw13','mw14','mw15','mw16','mw17','mw18','mw19','mw20','mw21',
                   'mw22','mw23','mw24','mw25','mw26','mw27','mw28','mw29','mw30','mw31','mw32','mw33','mw34','mw35','mw36','mw37','mw38','mw39','mw40','mw41','mw42',
                   'mw43','mw44','mw45','mw46','mw47','mw48','mw49','mw50', 'mw51', 'mw52']
        chp_derate_df = df_orispl_unit.merge(chp_derate_df, how='right', on=['orispl', 'fuel'])[mw_cols + ['orispl', 'fuel', 'elec_ratio', 'orispl_unit']] # links each relevant ORISPL unit, along with its max capaciy for each week, to the derated CHP columns just created
        chp_derate_df[mw_cols] = chp_derate_df[mw_cols].multiply(chp_derate_df.elec_ratio, axis='index') # multiplies each unit's electricity max generation capacity for each week with the calculated electricity ratio
        chp_derate_df.dropna(inplace=True) # remove nans
        #merge updated mw columns back into df_orispl_unit
        #update the chp_derate_df index to match df_orispl_unit
        chp_derate_df.index = df_orispl_unit[df_orispl_unit.orispl_unit.isin(chp_derate_df.orispl_unit)].index       
        df_orispl_unit.update(chp_derate_df[mw_cols]) # update relevant MW columns to reflect heat generation
        #replace the global dataframes
        self.df_cems = df_cems # saves large CEMS dataset for entire region
        self.df = df_orispl_unit # emissions, max capacity, and heat rate of unique units over each week of the year (along with their fuel types, etc.)


    def calcFuelPrices(self):
        """ 
        let RC be a high-ish price (1.1 * SUB)
        let LIG be based on national averages
        let NG be based on purchase type, where T takes C purchase type values and nan takes C, S, & T purchase type values
        ---
        Adds one column for each week of the year to self.df that contain fuel prices for each generation unit
        """   
        #we use eia923, where generators report their fuel purchases 
        df = self.eia923.copy(deep=True) # fuel purchase receipt form
        df = df[['YEAR','MONTH','orispl','ENERGY_SOURCE','FUEL_GROUP','QUANTITY','FUEL_COST', 'Purchase Type']]
        df.columns = ['year', 'month', 'orispl' , 'fuel', 'fuel_type', 'quantity', 'fuel_price', 'purchase_type'] # rename columns
        df.fuel = df.fuel.str.lower()       
        # clean up prices
        df.loc[df.fuel_price=='.', 'fuel_price'] = scipy.nan # nan fuel price gets actual nan
        df.fuel_price = df.fuel_price.astype('float')/100.
        df = df.reset_index() # adds index as column    
        ## find unique monthly prices per orispl and fuel type
        #create empty dataframe to hold the results
        df2 = self.df.copy(deep=True)[['fuel','orispl','orispl_unit']] # created in cleanGeneratorData; fuel type, ORISPL, and ORISPL+unit label for each label
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
                    if loop_len != -1: # stops prematurely if there are no filled prices of that specific contract type
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
                    #keep looping through the generators with eia923 price data until we have used all of their fuel price profiles, 
                    # then start again from the beginning of the loop with the plant with the highest energy production
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
                if loop_len != -1: # stops prematurely if there are no filled prices of that specific fuel type
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
        
        ## if we are shifting average fuel prices to some counterfactual value, we will do that here
        if bool(self.avg_price_fuel_type): # execute only if dictionary is not empty
            
            ## adjust nominal prices to real 2006/01 prices
            # function adjusts nominal dollars of a certain month and year to real 2006/1 dolalrs
            def adjust_to_2006_1_real_dollars(nominal_dollars, year, month):
                CPI_2006 = self.CPI.loc[((self.CPI['year'] == 2006) & (self.CPI['month'] == 1)), 'CPIAUCSL'].values[0]
                CPI_current = self.CPI.loc[((self.CPI['year'] == year) & (self.CPI['month'] == month)), 'CPIAUCSL'].values[0]
                real_dollars = nominal_dollars * CPI_current/CPI_2006
                return real_dollars

            temp_orispl_prices = orispl_prices.copy(deep=True)
            temp_orispl_prices[[i for i in range(1, 13)]] = orispl_prices[range(1, 13)].apply(
                lambda x: adjust_to_2006_1_real_dollars(x, self.year, x.name)) # adjusts to real 2006 dollars based on current year and month
            
            ## functions to filter data
            def mask_outliers(data):
                """
                Returns a boolean mask indicating which elements of the input array
                are outliers according to the modified Z-score method only applied to the upper bound.
                Note: the minimum value for the upper threshold is 30 $/MWh
                
                Parameters
                ----------
                data : numpy.ndarray or pandas.Series
                    The input array of data to test for outliers.
                threshold : float, optional
                    The number of scaled MADs above the median at which to define an
                    outlier. Default is 3.
                
                Returns
                -------
                mask : numpy.ndarray or pandas.Series
                    A boolean mask indicating which elements of the input array are
                    outliers.
                upper_threshold : float
                    The upper threshold used to define outliers.
                """
                
                # Calculate median and MAD
                median = numpy.nanmedian(data)
                mad = numpy.nanmedian(numpy.abs(data - median))

                # Calculate the threshold for outliers (3 scaled MAD)
                threshold = 3 * 1.4826 * mad

                # Mask the non-outlier elements
                # lower_threshold = median - threshold
                upper_threshold = max(30, median + threshold)
                mask = (data < 0) | (data > upper_threshold)

                return mask

            def outlier_threshold(data):
                """
                Returns upper threshold for data
                Note: the minimum value for the upper threshold is 10 $/MWh
                """

                # Calculate median and MAD
                median = numpy.nanmedian(data)
                mad = numpy.nanmedian(numpy.abs(data - median))

                # Calculate the threshold for outliers (3 scaled MAD)
                threshold = 3 * 1.4826 * mad
                
                # calculate the upper threshold
                upper_threshold = max(30, median + threshold)
                return upper_threshold

            
            
            ## create a dataframe to hold fuel_price_metrics: number of units, average before, average after, 
            #  average % change, min before, max before, standard deviation
            tuple_list = [] # assemble dataframe columns
            for key, value in self.avg_price_fuel_type.items():
                    if isinstance(value, dict):
                        for subkey, subvalue in value.items():
                            if isinstance(subvalue, dict):
                                subvalue = numpy.nan
                            tuple_list.append((key, subkey, subvalue))
                    else:
                        tuple_list.append((key, numpy.nan, value))
            fuel_price_metrics = pandas.DataFrame(tuple_list, columns=["fuel", "purchase_type", "average_counterfactual"])
            
            # Repeat the 'fuel_price_average', 'average_percent_change', 'standard_deviation', 
            # 'min', and 'max' 'upper_threshold_outliers', 'excluded_units', 'excluded_units_fraction', 'average_percent_change_no_outliers'
            # 'average_no_outliers', 'standard_deviation_no_outliers'columns with suffixes from 1 to 12
            suffixes = [str(i) for i in range(1, 13)]
            cols_to_repeat = ['average', 'average_percent_change', 'standard_deviation', 'min', 'max', 'upper_threshold_outliers', 
                              'excluded_units', 'excluded_units_fraction', 'average_no_outliers', 'average_percent_change_no_outliers',
                              'standard_deviation_no_outliers']
            new_cols = [f"{col}{suffix}" for col in cols_to_repeat for suffix in suffixes]
            new_cols = ['number_of_units'] + new_cols # also append 'number of units' to the list
            fuel_price_metrics = pandas.concat([fuel_price_metrics, pandas.DataFrame(columns=new_cols)]) # append new empty columns
            
            
            ## iterate over all unique generator fuel types, adjusting the fuel prices if they exist in the avg_price_fuel_type dictionary
            #  and populating the fuel_price_metrics dataframe
            f_iter = list(orispl_prices.fuel.unique()) # all types of unique fuels during this period
            for fuel_type in f_iter:
                if fuel_type not in self.avg_price_fuel_type:
                    units_not_in_dict = (orispl_prices["fuel"] == fuel_type).sum() # number of units with this fuel type
                    print(fuel_type + " is not in the dictionary of average fuel prices (" 
                          + str(units_not_in_dict) + " units out of " + str(orispl_prices.shape[0])
                          + " total units)")
                    # add row to metrics dataframe
                    new_row = pandas.DataFrame({'fuel': [fuel_type], 'number_of_units': [units_not_in_dict]})
                    fuel_price_metrics = pandas.concat([fuel_price_metrics, new_row])
                elif fuel_type == 'ng':
                    for purchase_type in self.avg_price_fuel_type['ng']: # iterate over all contract types, same method as in last 'else' statement (that is more readable)
                        # mask for units that have matching fuel type and contract type
                        mask = (temp_orispl_prices["fuel"] == fuel_type) & (temp_orispl_prices["purchase_type"] == purchase_type)
                        if purchase_type == 'other': # if other, retrieve all ng that are not the three purchase types
                            mask = ((temp_orispl_prices["fuel"] == fuel_type) & (temp_orispl_prices["purchase_type"] != 'T') 
                                    & (temp_orispl_prices["purchase_type"] != 'S')  & (temp_orispl_prices["purchase_type"] != 'C'))
                        if purchase_type == 'all': # if all (should be the only one in the list), apply to all ng; aka treat like 'else' statement
                            mask = temp_orispl_prices["fuel"] == fuel_type
                        # mask for fuel price metrics index
                        fuel_price_metrics_row = (fuel_price_metrics['fuel'] == fuel_type) & (fuel_price_metrics['purchase_type'] == purchase_type)
                        
                        ## calculate 'before' metrics (number of units, average, min, max, standard deviation) with all units
                        fuel_price_metrics.loc[fuel_price_metrics_row, 'number_of_units'] = mask.sum() # number of units
                        temp = temp_orispl_prices.loc[mask, range(1, 13)] # retrieve all price data to manipulate
                        # average before
                        fuel_price_metrics.loc[fuel_price_metrics_row, [f"average{suffix}" for suffix in suffixes]] = temp.mean(axis=0, skipna=True).values
                        # min before
                        fuel_price_metrics.loc[fuel_price_metrics_row, [f"min{suffix}" for suffix in suffixes]] = temp.min(axis=0, skipna=True).values
                        # max before
                        fuel_price_metrics.loc[fuel_price_metrics_row, [f"max{suffix}" for suffix in suffixes]] = temp.max(axis=0, skipna=True).values
                        # standard deviation 
                        fuel_price_metrics.loc[fuel_price_metrics_row, [f"standard_deviation{suffix}" for suffix in suffixes]] = temp.std(axis=0, skipna=True).values 
                        # calculate average percent change from actual to counterfactual average
                        avg_shift = numpy.divide((self.avg_price_fuel_type[fuel_type][purchase_type]
                                                  - temp.mean(axis=0, skipna=True))*100, temp.mean(axis=0, skipna=True)) # negative if shifting down
                        avg_shift.replace([numpy.inf, -numpy.inf], numpy.nan, inplace=True) # replace inf if dividing by 0 with nan
                        fuel_price_metrics.loc[fuel_price_metrics_row, [f"average_percent_change{suffix}" for suffix in suffixes]] = avg_shift.values
                        
                        ## remove outliers, then re-do calculations
                        with warnings.catch_warnings():
                            warnings.filterwarnings(action='ignore', category=RuntimeWarning)
                            # calculate upper threshold for outliers
                            fuel_price_metrics.loc[fuel_price_metrics_row, [f"upper_threshold_outliers{suffix}" for suffix in suffixes]] = temp.apply(outlier_threshold,
                                                                                                                                                      axis=0).values
                            # mask for outliers
                            outlier_mask = temp.apply(mask_outliers)
                            temp = temp.where(~outlier_mask, numpy.nan) # change outlier values to nan
                            # calculate number of units excluded from average
                            fuel_price_metrics.loc[fuel_price_metrics_row, [f"excluded_units{suffix}" for suffix in suffixes]] = outlier_mask.sum().values
                            # calculate fraction of units excluded from average
                            fuel_price_metrics.loc[fuel_price_metrics_row, [f"excluded_units_fraction{suffix}" 
                                                                            for suffix in suffixes]] = outlier_mask.sum().values/temp.shape[0] if temp.shape[0] else numpy.nan
                            # average each month
                            fuel_price_metrics.loc[fuel_price_metrics_row, [f"average_no_outliers{suffix}" for suffix in suffixes]] = temp.mean(
                                axis=0, skipna=True).values
                            # standard deviation 
                            fuel_price_metrics.loc[fuel_price_metrics_row, [f"standard_deviation_no_outliers{suffix}" for suffix in suffixes]] = temp.std(
                                axis=0, skipna=True).values
                            # calculate average percent change from actual to counterfactual average
                            avg_shift = numpy.divide((self.avg_price_fuel_type[fuel_type][purchase_type]
                                                      - temp.mean(axis=0, skipna=True))*100, temp.mean(axis=0, skipna=True)) # negative if shifting down
                            avg_shift.replace([numpy.inf, -numpy.inf], numpy.nan, inplace=True) # replace inf if dividing by 0 with nan
                            fuel_price_metrics.loc[fuel_price_metrics_row, 
                                                   [f"average_percent_change_no_outliers{suffix}" for suffix in suffixes]] = avg_shift.values
                        
                        ## perform shift
                        avg_ratio = self.avg_price_fuel_type[fuel_type][purchase_type]/temp.mean(axis=0, skipna=True) # new average/old average with outliers removed
                        temp_orispl_prices.loc[mask, range(1, 13)] = temp_orispl_prices.loc[mask, range(1, 13)].multiply(avg_ratio, axis=1)
                else:
                    # mask for units that have matching fuel type
                    mask = temp_orispl_prices["fuel"] == fuel_type
                    # mask for fuel price metrics index
                    fuel_price_metrics_row = fuel_price_metrics['fuel'] == fuel_type
                    
                    ## calculate 'before' metrics (number of units, average, min, max, standard deviation)
                    fuel_price_metrics.loc[fuel_price_metrics_row, 'number_of_units'] = mask.sum() # number of units
                    temp = temp_orispl_prices.loc[mask, range(1, 13)] # retrieve all price data to manipulate
                    # average before
                    fuel_price_metrics.loc[fuel_price_metrics_row, [f"average{suffix}" for suffix in suffixes]] = temp.mean(axis=0, skipna=True).values
                    # min before
                    fuel_price_metrics.loc[fuel_price_metrics_row, [f"min{suffix}" for suffix in suffixes]] = temp.min(axis=0, skipna=True).values
                    # max before
                    fuel_price_metrics.loc[fuel_price_metrics_row, [f"max{suffix}" for suffix in suffixes]] = temp.max(axis=0, skipna=True).values
                    # standard deviation 
                    fuel_price_metrics.loc[fuel_price_metrics_row, [f"standard_deviation{suffix}" for suffix in suffixes]] = temp.std(axis=0, skipna=True).values 
                    # calculate average percent change from actual to counterfactual average
                    avg_shift = numpy.divide((self.avg_price_fuel_type[fuel_type] - temp.mean(axis=0, skipna=True))*100, temp.mean(axis=0, skipna=True)) # negative if shifting down
                    avg_shift.replace([numpy.inf, -numpy.inf], numpy.nan, inplace=True) # replace inf if dividing by 0 with nan
                    fuel_price_metrics.loc[fuel_price_metrics_row, [f"average_percent_change{suffix}" for suffix in suffixes]] = avg_shift.values
                    
                    ## remove outliers, then re-do calculations
                    with warnings.catch_warnings():
                        warnings.filterwarnings(action='ignore', category=RuntimeWarning)
                        # calculate upper threshold for outliers
                        fuel_price_metrics.loc[fuel_price_metrics_row, [f"upper_threshold_outliers{suffix}" for suffix in suffixes]] = temp.apply(outlier_threshold,
                                                                                                                                                  axis=0).values
                        # mask for outliers
                        outlier_mask = temp.apply(mask_outliers)
                        temp = temp.where(~outlier_mask, numpy.nan) # change outlier values to nan
                        # calculate number of units excluded from average
                        fuel_price_metrics.loc[fuel_price_metrics_row, [f"excluded_units{suffix}" for suffix in suffixes]] = outlier_mask.sum().values
                        # calculate fraction of units excluded from average
                        fuel_price_metrics.loc[fuel_price_metrics_row, [f"excluded_units_fraction{suffix}" 
                                                                        for suffix in suffixes]] = outlier_mask.sum().values/temp.shape[0] if temp.shape[0] else numpy.nan
                        # average each month
                        fuel_price_metrics.loc[fuel_price_metrics_row, [f"average_no_outliers{suffix}" for suffix in suffixes]] = temp.mean(
                            axis=0, skipna=True).values
                        # standard deviation 
                        fuel_price_metrics.loc[fuel_price_metrics_row, [f"standard_deviation_no_outliers{suffix}" for suffix in suffixes]] = temp.std(
                            axis=0, skipna=True).values
                        # calculate average percent change from actual to counterfactual average
                        avg_shift = numpy.divide((self.avg_price_fuel_type[fuel_type]
                                                  - temp.mean(axis=0, skipna=True))*100, temp.mean(axis=0, skipna=True)) # negative if shifting down
                        avg_shift.replace([numpy.inf, -numpy.inf], numpy.nan, inplace=True) # replace inf if dividing by 0 with nan
                        fuel_price_metrics.loc[fuel_price_metrics_row, 
                                               [f"average_percent_change_no_outliers{suffix}" for suffix in suffixes]] = avg_shift.values
                    
                    ## perform shift
                    avg_ratio = self.avg_price_fuel_type[fuel_type]/temp.mean(axis=0, skipna=True) # new average/old average with outliers removed
                    temp_orispl_prices.loc[mask, range(1, 13)] = temp_orispl_prices.loc[mask, range(1, 13)].multiply(avg_ratio, axis=1)
            
            self.fuel_price_metrics = fuel_price_metrics # save metrics
            orispl_prices = temp_orispl_prices.copy(deep=True) # copy over the new fuel prices
        
        #for any fuels that don't have EIA923 data at all (for all regions) we will use commodity price approximations from an excel file
        #first we need to change orispl_prices from months to weeks
        orispl_prices.columns = ['orispl_unit', 'orispl', 'fuel', 1, 5, 9, 14, 18, 22, 27, 31, 36, 40, 44, 48, 'quantity', 'purchase_type'] # weeks corresponding to months of year
        #numpy.array(orispl_prices.columns.difference(['orispl_unit', 'orispl', 'fuel', 'quantity', 'purchase_type']))
        test = orispl_prices.copy(deep=True)[['orispl_unit', 'orispl', 'fuel']] # remove weekly price columns
        month_weeks = numpy.array(orispl_prices.columns.difference(['orispl_unit', 'orispl', 'fuel', 'quantity', 'purchase_type'])) # weeks corresponding to months of year
        for c in numpy.arange(52)+1: # loop through all weeks
            if c in month_weeks: # for weeks at first of month,
                test['fuel_price'+ str(c)] = orispl_prices[c] # update price for week column
            else:
                test['fuel_price'+ str(c)] = test['fuel_price'+str(c-1)] # otherwise, keep price the same for week column
        orispl_prices = test.copy(deep=True)
        
        #now we add in the weekly fuel commodity prices ## NOTE: We don't update this data source but it affects <1% of all generators at most
        prices_fuel_commodity = self.fuel_commodity_prices 
        f_array = orispl_prices[orispl_prices['fuel_price1'].isna()].fuel.unique() # identify any remaining fuels with nan prices
        percNan = ((orispl_prices[orispl_prices['fuel_price1'].isna()].shape[0])
                       *100/orispl_prices.shape[0]) # percent of data that are outliers and will be removed
        if self.ba_code == '':
            print(str(percNan)+"% generators from "+self.nerc+" have no EIA fuel price data")
        else:
            print(str(percNan)+"% generators from "+self.ba_code+" have no EIA fuel price data")
        print("the fuels that need to be manually filled are "+', '.join(f_array))
        for f in f_array:
            l = len(orispl_prices.loc[orispl_prices.fuel==f, orispl_prices.columns.difference(['orispl_unit', 'orispl', 'fuel'])])
            orispl_prices.loc[orispl_prices.fuel==f, orispl_prices.columns.difference(['orispl_unit', 'orispl', 'fuel'])] = numpy.tile(prices_fuel_commodity[f], (l,1)) # fill with commodity prices
        
        #now we have orispl_prices, which has a weekly fuel price for each orispl_unit based mostly on EIA923 data with some commodity, national-level data from EIA to supplement
        #now merge the fuel price columns into self.df
        orispl_prices.drop(['orispl', 'fuel'], axis=1, inplace=True) 
               
        #save
        self.df = self.df.merge(orispl_prices, on='orispl_unit', how='left') # we are left with weekly fuel prices for each generating unit in the df


    def easiurDamages(self):
        """ 
        Adds EASIUR environmental damages for SO2 and NOx emissions for each power plant.
        ---
        Adds one column for each week of the year to self.df that contains environmental damages in $/MWh for each generation unit calculated using the EASIURE method
        """   
        print('Adding environmental damages...')
        #clean the easiur data
        df = self.easiur_per_plant.copy(deep=True)
        df = df[['ORISPL','SO2 Winter 150m','SO2 Spring 150m','SO2 Summer 150m','SO2 Fall 150m','NOX Winter 150m','NOX Spring 150m','NOX Summer 150m','NOX Fall 150m']]
        df.columns = ['orispl', 'so2_dmg_win', 'so2_dmg_spr' , 'so2_dmg_sum', 'so2_dmg_fal', 'nox_dmg_win', 'nox_dmg_spr' , 'nox_dmg_sum', 'nox_dmg_fal']        
        #create empty dataframe to hold the results
        df2 = self.df.copy(deep=True)
        #for each week, calculate the $/MWh damages for each generator based on its emissions rate (kg/MWh) and easiur damages ($/tonne)
        for c in numpy.arange(52)+1:
            season = scipy.where(((c>49) | (c<=10)), 'win', scipy.where(((c>10) & (c<=23)), 'spr', scipy.where(((c>23) & (c<=36)), 'sum', scipy.where(((c>36) & (c<=49)), 'fal', 'na')))) #define the season string
            df2['dmg' + str(c)] = (df2['so2' + str(c)] * df['so2' + '_dmg_' + str(season)] + df2['nox' + str(c)] * df['nox' + '_dmg_' + str(season)]) / 1e3
        #use the results to redefine the main generator DataFrame
        self.df = df2


    def addGenMinOut(self):
        """ 
        Adds weekly minimum output capacity to the generator dataframe 'self.df' 
        ---
        """
        df = self.df.copy(deep=True)
        #define min_out, based on the NREL Black & Veatch report (2012)
        min_out_coal = 0.4
        min_out_ngcc = 0.5
        min_out_ngst = min_out_coal #assume the same as coal boiler
        min_out_nggt = 0.5 
        min_out_oilst = min_out_coal #assume the same as coal boiler
        min_out_oilgt = min_out_nggt #assume the same as gas turbine
        min_out_nuc = 0.5
        min_out_bio = 0.4
        df['min_out_multiplier'] = scipy.where(df.fuel_type=='oil', 
                                               scipy.where(df.prime_mover=='st', min_out_oilst, min_out_oilgt), 
                                               scipy.where(df.fuel_type=='biomass',min_out_bio, 
                                                           scipy.where(df.fuel_type=='coal',min_out_coal, 
                                                                       scipy.where(df.fuel_type=='nuclear',min_out_nuc, 
                                                                                   scipy.where(df.fuel_type=='gas', 
                                                                                               scipy.where(df.prime_mover=='gt', min_out_nggt, 
                                                                                                           scipy.where(df.prime_mover=='st', min_out_ngst, min_out_ngcc)), 0.10))))) # assigns above min out multiplier to specific fuels types and prime movers. 0.1 for oil
        df['min_out'] = df.mw * df.min_out_multiplier # minimum capacity is max capacity in year multiplied by multiplier
        self.df = df          
        
        
    def addGenVom(self):
        """ 
        Adds vom costs to the generator dataframe 'self.df' NOTE: weird how they don't use FERC for price changes; therefore, it needs to to be updated each year?
        ---
        """
        df = self.df.copy(deep=True)
        #define vom, based on the ranges of VOM values from pg.12, 
        # fig 5 of NREL The Western Wind and Solar Integration Study Phase 2" report. We assume, based on that study, 
        # that older units have higher values and newer units have lower values according to a linear relationship between the following coordinates:
        vom_range_coal_bit = [1.5, 5]
        vom_range_coal_sub = [1.5, 5]
        vom_range_coal = [1.5, 5]
        age_range_coal = [1955, 2013]
        vom_range_ngcc = [0.5, 1.5]
        age_range_ngcc = [1990, 2013]
        vom_range_nggt = [0.5, 2.0]
        age_range_nggt = [1970, 2013]
        vom_range_ngst = [0.5, 6.0]
        age_range_ngst = [1950, 2013]

        def vom_calculator(fuelType, fuel, primeMover, yearOnline): # calculates VOM based on linear relationship outlined above. dX is difference between current simulation year and the year that the generator came online
            if fuelType=='coal':
                if fuel == 'bit':
                    return vom_range_coal_bit[0] + (vom_range_coal_bit[1]-vom_range_coal_bit[0])/(age_range_coal[1]-age_range_coal[0]) * (self.year - yearOnline)
                elif fuel == 'sub':
                    return vom_range_coal_sub[0] + (vom_range_coal_sub[1]-vom_range_coal_sub[0])/(age_range_coal[1]-age_range_coal[0]) * (self.year - yearOnline)
                else:
                    return vom_range_coal[0] + (vom_range_coal[1]-vom_range_coal[0])/(age_range_coal[1]-age_range_coal[0]) * (self.year - yearOnline)
            if fuelType!='coal':
                if (primeMover=='ct') | (primeMover=='cc'):
                    return vom_range_ngcc[0] + (vom_range_ngcc[1]-vom_range_ngcc[0])/(age_range_ngcc[1]-age_range_ngcc[0]) * (self.year - yearOnline)
                if primeMover=='gt':
                    return vom_range_nggt[0] + (vom_range_nggt[1]-vom_range_nggt[0])/(age_range_nggt[1]-age_range_nggt[0]) * (self.year - yearOnline)
                if primeMover=='st':
                    return vom_range_ngst[0] + (vom_range_ngst[1]-vom_range_ngst[0])/(age_range_ngst[1]-age_range_ngst[0]) * (self.year - yearOnline)
        
        df['vom'] = df.apply(lambda x: vom_calculator(x['fuel_type'], x['fuel'], x['prime_mover'], x['year_online']), axis=1) # adds new VOM cost column to dataframe
        self.df = df


    def addDummies(self):
        """ 
        Adds dummy "coal_0" and "ngcc_0" generators to df
        ---
        """
        df = self.df.copy(deep=True)
        #coal_0
        df.loc[len(df)] = df.loc[0] # new row
        df.loc[len(df)-1, self.df.columns.drop(['ba', 'nerc', 'egrid'])] = df.loc[0, df.columns.drop(['ba', 'nerc', 'egrid'])] * 0 # dummy 0 values for coal
        df.loc[len(df)-1,['orispl', 'orispl_unit', 'fuel', 'fuel_type', 'prime_mover', 'min_out_multiplier', 'min_out', 'is_coal']] = ['coal_0', 'coal_0', 'sub', 'coal', 'st', 0.0, 0.0, 1]
        #ngcc_0
        df.loc[len(df)] = df.loc[0] # new row
        df.loc[len(df)-1, self.df.columns.drop(['ba', 'nerc', 'egrid'])] = df.loc[0, df.columns.drop(['ba', 'nerc', 'egrid'])] * 0 # dummy 0 values for nat gas
        df.loc[len(df)-1,['orispl', 'orispl_unit', 'fuel', 'fuel_type', 'prime_mover', 'min_out_multiplier', 'min_out', 'is_gas']] = ['ngcc_0', 'ngcc_0', 'ng', 'gas', 'ct', 0.0, 0.0, 1]
        self.df = df
            
     
    def calcDemandData(self):
        """ 
        Uses CEMS data to calculate net demand (i.e. total fossil generation), total emissions, and each generator type's contribution to the actual historical generation mix
        ---
        Creates
        self.hist_dispatch : one row per hour of the year, columns for net demand, total emissions, operating cost of the marginal generator, and the contribution of different fuels to the total energy production
        """
        print('Calculating demand data from CEMS...')
        #re-compile the cems data adding in fuel and fuel type
        df = self.df_cems.copy(deep=True) # copy CEMS data
        merge_orispl_unit = self.df.copy(deep=True)[['orispl_unit', 'fuel', 'fuel_type']] # copy unit data
        merge_orispl = self.df.copy(deep=True)[['orispl', 'fuel', 'fuel_type']].drop_duplicates('orispl') # get unique CEMS plants
        df = df.merge(merge_orispl_unit, how='left', on=['orispl_unit']) # merge the fuel and fuel type data to CEMS
        df.loc[df.fuel.isna(), 'fuel'] = scipy.array(df[df.fuel.isna()].merge(merge_orispl, how='left', on=['orispl']).fuel_y) # fill in missing fuels for units with overall plant fuel
        df.loc[df.fuel_type.isna(), 'fuel_type'] = scipy.array(df[df.fuel_type.isna()].merge(merge_orispl, how='left', on=['orispl']).fuel_type_y) # do same for fuel types
        #build the hist_dispatch dataframe
        #start with the datetime column # NOTE: can probably replace this column by just doing hourly increments between first and last times. The last week will just go on an extra few hours? (repeat midnight hours?)
        start_date_str = (self.df_cems.date.min()[-4:] + '-' + self.df_cems.date.min()[:5] + ' 00:00') # NOTE: change this to get the actual first datetime (because we operate on UTC - should we just translate to local time?)
        date_hour_count = len(self.df_cems.date.unique())*24#+1 # amount of hours in year
        hist_dispatch = pandas.DataFrame(scipy.array([pandas.Timestamp(start_date_str) + datetime.timedelta(hours=i) for i in range(date_hour_count)]), columns=['datetime']) # builds time data for each hour of year 
        #add columns by aggregating df by date + hour
        hist_dispatch['demand'] = df.groupby(['date','hour'], as_index=False).sum().mwh
        hist_dispatch['co2_tot'] = df.groupby(['date','hour'], as_index=False).sum().co2_tot # * 2000
        hist_dispatch['so2_tot'] = df.groupby(['date','hour'], as_index=False).sum().so2_tot
        hist_dispatch['nox_tot'] = df.groupby(['date','hour'], as_index=False).sum().nox_tot
        hist_dispatch['coal_mix'] = df[(df.fuel_type=='coal') | (df.fuel=='SGC')].groupby(['date','hour'], as_index=False).sum().mwh
        hist_dispatch['gas_mix'] = df[df.fuel_type=='gas'].groupby(['date','hour'], as_index=False).sum().mwh
        hist_dispatch['oil_mix'] = df[df.fuel_type=='oil'].groupby(['date','hour'], as_index=False).sum().mwh
        hist_dispatch['biomass_mix'] = df[(df.fuel_type=='biomass') | (df.fuel=='obs') | (df.fuel=='wds') | (df.fuel=='blq') | (df.fuel=='msw') | (df.fuel=='lfg') | (df.fuel=='ab') | (df.fuel=='obg') | (df.fuel=='obl') | (df.fuel=='slw')].groupby(['date','hour'], as_index=False).sum().mwh
        hist_dispatch['geothermal_mix'] = df[(df.fuel_type=='geothermal') | (df.fuel=='geo')].groupby(['date','hour'], as_index=False).sum().mwh
        hist_dispatch['hydro_mix'] = df[(df.fuel_type=='hydro') | (df.fuel=='wat')].groupby(['date','hour'], as_index=False).sum().mwh
        hist_dispatch['nuclear_mix'] = df[df.fuel=='nuc'].groupby(['date','hour'], as_index=False).sum().mwh
        #hist_dispatch['production_cost'] = df[['date', 'hour', 'production_cost']].groupby(['date','hour'], as_index=False).sum().production_cost
        hist_dispatch.fillna(0, inplace=True) # if nan, is 0
        #fill in last line to equal the previous line
        #hist_dispatch.loc[(len(hist_dispatch)-1)] = hist_dispatch.loc[(len(hist_dispatch)-2)]
        hist_dispatch = hist_dispatch.fillna(0)
        self.hist_dispatch = hist_dispatch 


    def addElecPriceToDemandData(self):
        """ 
        Calculates the historical electricity price for the nerc region, adding it as a new column to the demand data
        ---
        """
        print('Calculating historical electricity prices...')
        #We will use FERC 714 data, where balancing authorities and similar entities report their locational marginal prices. 
        # This script pulls in those price for every reporting entity in the nerc region and takes the max price across the BAs/entities for each hour.
        df = self.ferc714.copy(deep=True)
        df_ids = self.ferc714_ids.copy(deep=True)
        nerc_region = self.nerc
        year = self.year
        df_ids_bas = list(df_ids[df_ids.nerc == nerc_region].respondent_id.values) # balancing authorities in NERC region
        #aggregate the price data by mean price per hour for any balancing authorities within the nerc region
        df_bas = df[df.respondent_id.isin(df_ids_bas) & (df.report_yr==year)][
            ['lambda_date', 'respondent_id', 'hour01', 'hour02', 'hour03', 'hour04', 'hour05', 'hour06', 'hour07', 
             'hour08', 'hour09', 'hour10', 'hour11', 'hour12', 'hour13', 'hour14', 'hour15', 'hour16', 'hour17', 'hour18', 
             'hour19', 'hour20', 'hour21', 'hour22', 'hour23', 'hour24']] # retrieves historical 24-hour prices for each day for the respondent authorities # NOTE: need to change time frame to make this align
        df_bas.drop(['respondent_id'], axis=1, inplace=True)
        df_bas.columns = ['date',1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24]
        df_bas_temp = pandas.melt(df_bas, id_vars=['date']) # makes each row-column price data point its own row; date and hour are reported alongside the price data in the row
        df_bas_temp.date = df_bas_temp.date.str[0:-7] + (df_bas_temp.variable - 1).astype(str) + ':00' # assigns hours to the date variable
        df_bas_temp['time'] = df_bas_temp.variable.astype(str) + ':00' # assigns hours using the hourly data column to the time column
        df_bas_temp['datetime'] = pandas.to_datetime(df_bas_temp.date) # changes datetime back to date from a string
        df_bas_temp.drop(columns=['date', 'variable', 'time'], inplace=True)
        #aggregate by datetime
        df_bas_temp = df_bas_temp.groupby('datetime', as_index=False).max() # takes max price of all of these
        df_bas_temp.columns = ['datetime', 'price']        
        #add the price column to self.hist_dispatch
        self.hist_dispatch['gen_cost_marg'] = df_bas_temp.price 


    def demandTimeSeries(self):
        """ 
        Re-formats and slices self.hist_dispatch to produce a demand time series to be used by the dispatch object
        ---
        Creates
        self.demand_data : row for each hour. columns for datetime and demand
        """
        print('Creating "demand_data" time series...')
        #demand using CEMS data
        demand_data = self.hist_dispatch.copy(deep=True)
        demand_data.datetime = pandas.to_datetime(demand_data.datetime)
        self.demand_data = demand_data[['datetime', 'demand']]
    
    
    def cemsBoxPlot(self, plot_col):
        """ 
        Creates a box plot of the hourly CEMS data for each unique orispl_unit for the given column
        ---
        plot_col: 'co2', 'heat_rate', etc.
        """
        print('Creating "demand_data" time series...')
        #copy the CEMS data
        cems_copy = self.df_cems.copy(deep=True)
        #each uniqe unit tag
        ounique = cems_copy.orispl_unit.unique()
       #empty data frame for results
        result = pandas.DataFrame({'orispl_unit': ounique, plot_col+'_5': scipy.zeros_like(ounique), plot_col+'_25': scipy.zeros_like(ounique), plot_col+'_50': scipy.zeros_like(ounique), plot_col+'_75': scipy.zeros_like(ounique), plot_col+'_95': scipy.zeros_like(ounique), 'data_points': scipy.zeros_like(ounique)})
        #for each unique unit calculate the 5th, 25th, median, 75th, and 95th percentile data
        print('Calculating quantiles...')
        for o in ounique:
            cems_e_test = cems_copy.loc[cems_copy.orispl_unit==o, plot_col]
            if len(cems_e_test) != 0:
                result.loc[result.orispl_unit==o, plot_col+'_5'] = cems_e_test.quantile(0.05)
                result.loc[result.orispl_unit==o, plot_col+'_25'] = cems_e_test.quantile(0.25)
                result.loc[result.orispl_unit==o, plot_col+'_50'] = cems_e_test.median()
                result.loc[result.orispl_unit==o, plot_col+'_75'] = cems_e_test.quantile(0.75)
                result.loc[result.orispl_unit==o, plot_col+'_95'] = cems_e_test.quantile(0.95)
                result.loc[result.orispl_unit==o, 'data_points'] = len(cems_e_test)
        #sort the results for plotting
        result = result.sort_values(by=[plot_col+'_50']).reset_index()
        #plot the results to be like a box plot
        x = numpy.arange(len(ounique))
        f, ax = matplotlib.pylab.subplots(1, figsize = (7,35))
        ax.scatter(result[plot_col+'_5'], x, marker='.', color='k', s=5)
        ax.scatter(result[plot_col+'_25'], x, marker='|', color='k', s=40)
        ax.scatter(result[plot_col+'_50'], x, marker='s', color='r', s=15)
        ax.scatter(result[plot_col+'_75'], x, marker='|', color='k', s=40)
        ax.scatter(result[plot_col+'_95'], x, marker='.', color='k', s=5)
        xmax = scipy.where(plot_col=='co2', 2000, scipy.where(plot_col=='so2', 5, scipy.where(plot_col=='nox', 3, 20)))
        for a in x:
            ax.plot([result.loc[a,plot_col+'_5'],result.loc[a,plot_col+'_95']], [a,a], lw=1, c='black', alpha=0.25)
            matplotlib.pylab.text(-xmax*0.2, a, result.loc[a, 'orispl_unit'])
        for v in [xmax*0.25, xmax*0.5, xmax*0.75]:
            ax.axvline(v, c='black', lw=1, alpha=0.5, ls=':')
        ax.set_xlim(0,xmax)
        ax.set_xticks([xmax*0.25, xmax*0.5, xmax*0.75, xmax])
        ax.get_yaxis().set_ticks([])
        if plot_col == 'heat_rate':
            ax.set_xlabel('Heat Rate [mmBtu/MWh]')
        else:
            ax.set_xlabel(plot_col.title() + ' Emissions Rate [kg/MWh]')
        return f


    def calcMdtCoalEvents(self):
        """ 
        Creates a dataframe of the start, end, and demand_threshold for each event in the demand data where we would expect a coal plant's minimum downtime constraint to kick in
        ---
        """                      
        mdt_coal_events = self.demand_data.copy()
        mdt_coal_events['indices'] = mdt_coal_events.index
        #find the integral from x to x+
        mdt_coal_events['integral_x_xt'] = mdt_coal_events.demand[::-1].rolling(window=self.coal_min_downtime+1).sum()[::-1] # sum of demand between current hour and next X hours determined by min downtime
        #find the integral of a flat horizontal line extending from x
        mdt_coal_events['integral_x'] = mdt_coal_events.demand * (self.coal_min_downtime+1) # flat integral from multiplying by min downtime
        #find the integral under the minimum of the flat horizontal line and the demand curve
        def d_forward_convex_integral(mdt_index):
            try:
                return scipy.minimum(scipy.repeat(mdt_coal_events.demand[mdt_index], (self.coal_min_downtime+1)), mdt_coal_events.demand[mdt_index:(mdt_index+self.coal_min_downtime+1)]).sum()
            except:
                return mdt_coal_events.demand[mdt_index]   
        mdt_coal_events['integral_x_xt_below_x'] = mdt_coal_events.indices.apply(d_forward_convex_integral)
        #find the integral of the convex portion below x_xt
        mdt_coal_events['integral_convex_portion_btwn_x_xt'] = mdt_coal_events['integral_x'] - mdt_coal_events['integral_x_xt_below_x']
        #keep the convex integral only if x < 1.05*x+
        def d_keep_convex(mdt_index):
            try:
                return int(mdt_coal_events.demand[mdt_index] <= 1.05*mdt_coal_events.demand[mdt_index + self.coal_min_downtime]) * mdt_coal_events.integral_convex_portion_btwn_x_xt[mdt_index]
            except:
                return mdt_coal_events.integral_convex_portion_btwn_x_xt[mdt_index]   
        mdt_coal_events['integral_convex_filtered'] = mdt_coal_events.indices.apply(d_keep_convex)
        #mdt_coal_events['integral_convex_filtered'] = mdt_coal_events['integral_convex_filtered'].replace(0, scipy.nan)
        #keep any local maximums of the filtered convex integral
        mdt_coal_events['local_maximum'] = ((mdt_coal_events.integral_convex_filtered== mdt_coal_events.integral_convex_filtered.rolling(window=int(self.coal_min_downtime/2+1), center=True).max()) & (mdt_coal_events.integral_convex_filtered != 0) & (mdt_coal_events.integral_x >= mdt_coal_events.integral_x_xt))
        #spread the maximum out over the min downtime window
        mdt_coal_events = mdt_coal_events[mdt_coal_events.local_maximum]
        mdt_coal_events['demand_threshold'] = mdt_coal_events.demand
        mdt_coal_events['start'] = mdt_coal_events.datetime
        mdt_coal_events['end'] = mdt_coal_events.start + pandas.DateOffset(hours=self.coal_min_downtime)
        mdt_coal_events = mdt_coal_events[['start', 'end', 'demand_threshold']]
        self.mdt_coal_events = mdt_coal_events     

    
    


class bidStack(object):
    def __init__(self, gen_data_short, states_to_subset = [], co2_dol_per_kg=0.0, so2_dol_per_kg=0.0, nox_dol_per_kg=0.0, 
                 coal_dol_per_mmbtu=0.0, coal_capacity_derate = 0.0, time=1, dropNucHydroGeo=False, 
                 include_min_output=True, initialization=True, coal_mdt_demand_threshold = 0.0, mdt_weight=0.50):
        """ 
        1) Bring in the generator data created by the "generatorData" class.
        2) Calculate the generation cost for each generator and sort the generators by generation cost. Default emissions prices [$/kg] are 0.00 for all emissions.
        ---
        gen_data_short : a generatorData object
        states_to_subset : list of 2-letter capital abbreviations of all states in which emissions will be subset
        co2 / so2 / nox_dol_per_kg : a tax on each amount of emissions produced by each generator. Impacts each generator's generation cost
        coal_dol_per_mmbtu : a tax (+) or subsidy (-) on coal fuel prices in $/mmbtu. Impacts each generator's generation cost
        coal_capacity_derate : fraction that we want to derate all coal capacity (e.g. 0.20 mulutiplies each coal plant's capacity by (1-0.20))
        time : number denoting which time period we are interested in. Default is weeks, so time=15 would look at the 15th week of heat rate, emissions rates, and fuel prices
        dropNucHydroGeo : if True, nuclear, hydro, and geothermal plants will be removed from the bidstack (e.g. to match CEMS data)
        include_min_output : if True, will include a representation of generators' minimum output constraints that impacts the marginal generators in the dispatch. So, a "True" value here is closer to the real world.
        initialization : if True, the bs object is being defined for the first time. This will trigger the generation of a dummy 0.0 demand generator to bookend the bottom of the merit order (in calcGenCost function) after which initialization will be set to False
        """
        self.year = gen_data_short["year"] # year of run
        self.nerc = gen_data_short["nerc"] # NERC region
        self.hist_dispatch = gen_data_short["hist_dispatch"] # historical demand, emissions, and marginal generator price and fuel
        self.mdt_coal_events = gen_data_short["mdt_coal_events"]  # minimum downtime events
        self.coal_mdt_demand_threshold = coal_mdt_demand_threshold
        self.mdt_weight = mdt_weight
        self.df_0 = gen_data_short["df"] # all generators, their attributes, and their weekly heat throughputs, emission rates, capacity, and fuel prices
        self.df = self.df_0.copy(deep=True)
        self.states_to_subset = states_to_subset # states to subset from overall run
        self.co2_dol_per_kg = co2_dol_per_kg
        self.so2_dol_per_kg = so2_dol_per_kg
        self.nox_dol_per_kg = nox_dol_per_kg
        self.coal_dol_per_mmbtu = coal_dol_per_mmbtu
        self.coal_capacity_derate = coal_capacity_derate
        self.time = time # week to run
        self.include_min_output = include_min_output # whether to include minimum downtime constraint
        self.initialization = initialization
        if dropNucHydroGeo:
            self.dropNuclearHydroGeo()
        self.addFuelColor() # adds fuel color column to df_0 based on fuel type
        self.processData()
      
        
    def updateDf(self, new_data_frame):
        self.df_0 = new_data_frame
        self.df = self.df_0.copy(deep=True)
        self.processData()


    def dropNuclearHydroGeo(self):
        """ 
        Removes nuclear, hydro, and geothermal plants from self.df_0 (since they don't show up in CEMS)
        ---
        """
        self.df_0 = self.df_0[(self.df_0.fuel!='nuc') & (self.df_0.fuel!='wat') & (self.df_0.fuel!='geo')]


    def updateEmissionsAndFuelTaxes(self, co2_price_new, so2_price_new, nox_price_new, coal_price_new):
        """ Updates self. emissions prices (in $/kg) and self.coal_dol_per_mmbtu (in $/mmbtu)
        ---
        """
        self.co2_dol_per_kg = co2_price_new
        self.so2_dol_per_kg = so2_price_new
        self.nox_dol_per_kg = nox_price_new   
        self.coal_dol_per_mmbtu = coal_price_new
    
    
    def processData(self):
        """ runs a few of the internal functions. There are couple of places in the class that run these functions in this order, so it made sense to just locate this set of function runs in a single location
        ---
        """
        self.calcGenCost()  # calculates average generator cost based on VOM, fuel price, and any taxes on emissions
        self.createTotalInterpolationFunctions() # creates interpolation functions 
        self.createMarginalPiecewise() # creates dataframe with original demand and shifted demand
        self.calcFullMeritOrder() # calculates base and marginal price, fuel use, and emissions for each unit
        self.createMarginalPiecewise() # do this again with FullMeritOrder so that it includes the new full_####_marg columns
        self.createTotalInterpolationFunctionsFull() # calculates interpolation function again
    
    
    def updateTime(self, t_new):
        """ Updates self.time
        ---
        """
        self.time = t_new
        self.processData()
    
    
    def addFuelColor(self):
        """ Assign a fuel type for each fuel and a color for each fuel type to be used in charts
        ---
        creates 'fuel_type' and 'fuel_color' columns
        """
        c = {'gas':'#888888', 'coal':'#bf5b17', 'oil':'#252525' , 'nuclear':'#984ea3', 'hydro':'#386cb0', 'biomass':'#7fc97f', 'geothermal':'#e31a1c', 'ofsl': '#c994c7'}
        self.df_0['fuel_color'] = '#bcbddc'
        for c_key in c.keys():
            self.df_0.loc[self.df_0.fuel_type == c_key, 'fuel_color'] = c[c_key]            
     
           
    def calcGenCost(self):
        """ Calculate average costs that are function of generator data, fuel cost, and emissions prices.
        gen_cost ($/MWh) = (heat_rate * "fuel"_price) + (co2 * co2_price) + (so2 * so2_price) + (nox * nox_price) + vom 
        """
        df = self.df_0.copy(deep=True)
        #pre-processing:
        #adjust coal fuel prices by the "coal_dol_per_mmbtu" input
        df.loc[df.fuel_type=='coal', 'fuel_price' + str(self.time)] = scipy.maximum(0, df.loc[df.fuel_type=='coal', 'fuel_price' + str(self.time)] + self.coal_dol_per_mmbtu)
        #adjust coal capacity by the "coal_capacity_derate" input
        df.loc[df.fuel_type=='coal', 'mw' + str(self.time)] = df.loc[df.fuel_type=='coal', 'mw' + str(self.time)] * (1.0 -  self.coal_capacity_derate)
        #calculate the generation cost:
        df['fuel_cost'] = df['heat_rate' + str(self.time)] * df['fuel_price' + str(self.time)] 
        df['co2_cost'] = df['co2' + str(self.time)] * self.co2_dol_per_kg 
        df['so2_cost'] = df['so2' + str(self.time)] * self.so2_dol_per_kg 
        df['nox_cost'] = df['nox' + str(self.time)] * self.nox_dol_per_kg 
        with scipy.errstate(all='ignore'): # to suppress warnings
            df['gen_cost'] = scipy.maximum(0.01, df.fuel_cost + df.co2_cost + df.so2_cost + df.nox_cost + df.vom)
        # #add a zero generator so that the bid stack goes all the way down to zero. This is important for calculating information for the 
        # marginal generator when the marginal generator is the first one in the bid stack.
        # df['dmg_easiur'] = df['dmg' + str(self.time)] # NOTE: EASIUR commented out because I (Eric) don't need it
        if self.initialization:
            dtype_dict = df.dtypes.to_dict() # to preserve data types
            empty_row = df.loc[0]*0 # creates empty row
            df = pandas.concat([df, empty_row.to_frame().T, empty_row.to_frame().T], axis=0, ignore_index=True) # appends 2 empty rows to dataframe
            df = df.astype(dtype_dict) # re-casts columns to same types as original dataframe
            self.initialization = False
        df.sort_values('gen_cost', inplace=True)
        #move coal_0 and ngcc_0 to the front of the merit order regardless of their gen cost
        coal_0_ind = df[df.orispl_unit=='coal_0'].index[0]
        ngcc_0_ind = df[df.orispl_unit=='ngcc_0'].index[0]
        df = pandas.concat([df.iloc[[0],:], df[df.orispl_unit=='coal_0'], df[df.orispl_unit=='ngcc_0'], df.drop([0, coal_0_ind, ngcc_0_ind], axis=0)], axis=0)
        df.reset_index(drop=True, inplace=True) # end result is that 5 rows in beginning of dataframe are null (0, coal, ng, 0, 0)
        df['demand'] = df['mw' + str(self.time)].cumsum() # cumulative function for demand, calculated by historical mw
        # NOTE: this actually sets the last generator as a really large demand, not create it
        df.loc[len(df)-1, 'demand'] = df.loc[len(df)-1, 'demand'] + 1000000 
        df['f'] = df['demand'] # copy of demand column
        df['s'] = scipy.append(0, scipy.array(df.f[0:-1])) # column of cumulative demand of all prior units in merit order
        df['a'] = scipy.maximum(df.s - df.min_out*10.0, 1.0) # remaining cumulative demand after unit's base capacity removed*10 (less the minimum capacity)
        self.df = df  
        
        
    def createMarginalPiecewise(self):
        """ Creates a piecewsise dataframe of the generator data. We can then interpolate this data frame for marginal data instead of querying.
        """
        test = self.df.copy()      
        test_shift = test.copy()
        test_shift['demand'] = test_shift.demand + 0.1      
        test.index = test.index * 2
        test_shift.index = test_shift.index * 2 + 1
        df_marg_piecewise = pandas.concat([test, test_shift]).sort_index() # combines original and shifted dataframes
        df_marg_piecewise['demand'] = pandas.concat([df_marg_piecewise.demand[0:1], df_marg_piecewise.demand[0:-1]]).reset_index(drop=True)
        df_marg_piecewise['demand'] = df_marg_piecewise.demand - 0.1
        self.df_marg_piecewise = df_marg_piecewise


    def returnMarginalGenerator(self, demand, return_type):
        """ Returns marginal data by interpolating self.df_marg_piecewise, which is much faster than the returnMarginalGenerator function below.
        ---
        demand : [MW]
        return_type : column header of self.df being returned (e.g. 'gen', 'fuel_type', 'gen_cost', etc.)
        """
        try: #try interpolation as it's much faster. 
            try: #for columns with a time value at the end (i.e. nox30)
                return scipy.interp(demand, self.df_marg_piecewise['demand'], scipy.array(self.df_marg_piecewise[return_type + str(self.time)], dtype='float64'))
            except: #for columns without a time value at the end (i.e. gen_cost)
                return scipy.interp(demand, self.df_marg_piecewise['demand'], scipy.array(self.df_marg_piecewise[return_type], dtype='float64'))   
        except: #interpolation will only work for floats, so we use querying below otherwise (~8x slower)
            ind = scipy.minimum(self.df.index[self.df.demand <= demand][-1], len(self.df)-2)
            return self.df[return_type][ind+1]
	
					
    def createTotalInterpolationFunctions(self):
        """ Creates interpolation functions for the total data (i.e. total cost, total emissions, etc.) depending on total demand. 
        Then the returnTotalCost, returnTotal###, ..., functions use these interpolations rather than querying the dataframes as in previous versions. 
        This reduces solve time by ~90x. Dataframe is sorted in merit order prior to input into the functions
        """       
        test = self.df.copy()      
        #cost
        self.f_totalCost = scipy.interpolate.interp1d(test.demand, (test['mw' + str(self.time)] * test['gen_cost']).cumsum()) # cumulative total cost (in increasing cost order) vs demand
        #emissions and health damages
        self.f_totalCO2 = scipy.interpolate.interp1d(test.demand, (test['mw' + str(self.time)] * test['co2' + str(self.time)]).cumsum())
        self.f_totalSO2 = scipy.interpolate.interp1d(test.demand, (test['mw' + str(self.time)] * test['so2' + str(self.time)]).cumsum())
        self.totalSO2 = (test['mw' + str(self.time)] * test['so2' + str(self.time)]).cumsum() # TEMPORARY for debugging interpolation function
        self.f_totalNOX = scipy.interpolate.interp1d(test.demand, (test['mw' + str(self.time)] * test['nox' + str(self.time)]).cumsum())
        # self.f_totalDmg = scipy.interpolate.interp1d(test.demand, (test['mw' + str(self.time)] * test['dmg' + str(self.time)]).cumsum())
        #for coal units only
        self.f_totalCO2_Coal = scipy.interpolate.interp1d(test.demand, (test['mw' + str(self.time)] * test['co2' + str(self.time)] * test['is_coal']).cumsum())
        self.f_totalSO2_Coal = scipy.interpolate.interp1d(test.demand, (test['mw' + str(self.time)] * test['so2' + str(self.time)] * test['is_coal']).cumsum())
        self.f_totalNOX_Coal = scipy.interpolate.interp1d(test.demand, (test['mw' + str(self.time)] * test['nox' + str(self.time)] * test['is_coal']).cumsum())
        # self.f_totalDmg_Coal = scipy.interpolate.interp1d(test.demand, (test['mw' + str(self.time)] * test['dmg' + str(self.time)] * test['is_coal']).cumsum())       
        #fuel mix
        self.f_totalGas = scipy.interpolate.interp1d(test.demand, (test['is_gas'] * test['mw' + str(self.time)]).cumsum())
        self.f_totalCoal = scipy.interpolate.interp1d(test.demand, (test['is_coal'] * test['mw' + str(self.time)]).cumsum())
        self.f_totalOil = scipy.interpolate.interp1d(test.demand, (test['is_oil'] * test['mw' + str(self.time)]).cumsum())
        self.f_totalNuclear = scipy.interpolate.interp1d(test.demand, (test['is_nuclear'] * test['mw' + str(self.time)]).cumsum())
        self.f_totalHydro = scipy.interpolate.interp1d(test.demand, (test['is_hydro'] * test['mw' + str(self.time)]).cumsum())
        self.f_totalGeothermal = scipy.interpolate.interp1d(test.demand, (test['is_geothermal'] * test['mw' + str(self.time)]).cumsum())
        self.f_totalBiomass = scipy.interpolate.interp1d(test.demand, (test['is_biomass'] * test['mw' + str(self.time)]).cumsum())
        #fuel consumption
        self.f_totalConsGas = scipy.interpolate.interp1d(test.demand, (test['is_gas'] * test['heat_rate' + str(self.time)] * test['mw' + str(self.time)]).cumsum())
        self.f_totalConsCoal = scipy.interpolate.interp1d(test.demand, (test['is_coal'] * test['heat_rate' + str(self.time)] * test['mw' + str(self.time)]).cumsum())
        self.f_totalConsOil = scipy.interpolate.interp1d(test.demand, (test['is_oil'] * test['heat_rate' + str(self.time)] * test['mw' + str(self.time)]).cumsum())
        self.f_totalConsNuclear = scipy.interpolate.interp1d(test.demand, (test['is_nuclear'] * test['heat_rate' + str(self.time)] * test['mw' + str(self.time)]).cumsum())
        self.f_totalConsHydro = scipy.interpolate.interp1d(test.demand, (test['is_hydro'] * test['heat_rate' + str(self.time)] * test['mw' + str(self.time)]).cumsum())
        self.f_totalConsGeothermal = scipy.interpolate.interp1d(test.demand, (test['is_geothermal'] * test['heat_rate' + str(self.time)] * test['mw' + str(self.time)]).cumsum())
        self.f_totalConsBiomass = scipy.interpolate.interp1d(test.demand, (test['is_biomass'] * test['heat_rate' + str(self.time)] * test['mw' + str(self.time)]).cumsum())
                
					
    def returnTotalCost(self, demand):
        """ Given demand input, return the integral of the bid stack generation cost (i.e. the total operating cost of the online power plants).
        ---
        demand : [MW]
        return : integral value of the bid stack cost = total operating costs of the online generator fleet [$].
        """
        return self.f_totalCost(demand)
      
       
    def returnTotalEmissions(self, demand, emissions_type):
        """ Given demand and emissions_type inputs, return the integral of the bid stack emissions (i.e. the total emissions of the online power plants).
        ---
        demand : [MW]
        emissions_type : 'co2', 'so2', 'nox', etc.
        return : integral value of the bid stack emissions = total emissions of the online generator fleet [lbs].
        """
        if emissions_type == 'co2':
            return self.f_totalCO2(demand)
        if emissions_type == 'so2':
            return self.f_totalSO2(demand)
        if emissions_type == 'nox':
            return self.f_totalNOX(demand)
            
            
    def returnTotalEmissions_Coal(self, demand, emissions_type):
        """ Given demand and emissions_type inputs, return the integral of the bid stack emissions (i.e. the total emissions of the online power plants).
        ---
        demand : [MW]
        emissions_type : 'co2', 'so2', 'nox', etc.
        return : integral value of the bid stack emissions = total emissions of the online generator fleet [lbs].
        """
        if emissions_type == 'co2':
            return self.f_totalCO2_Coal(demand)
        if emissions_type == 'so2':
            return self.f_totalSO2_Coal(demand)
        if emissions_type == 'nox':
            return self.f_totalNOX_Coal(demand)
    
    
    def returnTotalEasiurDamages(self, demand):
        """ Given demand input, return the integral of the bid stack EASIUR damages (i.e. the total environmental damages of the online power plants).
        ---
        demand : [MW]
        return : integral value of the bid environmental damages = total damages of the online generator fleet [$].
        """
        return self.f_totalDmg(demand)
        
    
    def returnTotalEasiurDamages_Coal(self, demand):
        """ Given demand input, return the integral of the bid stack EASIUR damages (i.e. the total environmental damages of the online power plants).
        ---
        demand : [MW]
        return : integral value of the bid environmental damages = total damages of the online generator fleet [$].
        """
        return self.f_totalDmg_Coal(demand)
    
    
    def returnTotalFuelMix(self, demand, is_fuel_type):
        """ Given demand and is_fuel_type inputs, return the total MW of online generation of is_fuel_type.
        ---
        demand : [MW]
        is_fuel_type : 'is_coal', etc.
        return : total amount of online generation of type is_fuel_type
        """
        if is_fuel_type == 'is_gas':
            return self.f_totalGas(demand) 
        if is_fuel_type == 'is_coal':
            return self.f_totalCoal(demand)
        if is_fuel_type == 'is_oil':
            return self.f_totalOil(demand)
        if is_fuel_type == 'is_nuclear':
            return self.f_totalNuclear(demand)
        if is_fuel_type == 'is_hydro':
            return self.f_totalHydro(demand)
        if is_fuel_type == 'is_geothermal':
            return self.f_totalGeothermal(demand)
        if is_fuel_type == 'is_biomass':
            return self.f_totalBiomass(demand)
    
    
    def returnTotalFuelConsumption(self, demand, is_fuel_type):
        """ Given demand and is_fuel_type inputs, return the total MW of online generation of is_fuel_type.
        ---
        demand : [MW]
        is_fuel_type : 'is_coal', etc.
        return : total amount of fuel consumption of type is_fuel_type
        """
        if is_fuel_type == 'is_gas':
            return self.f_totalConsGas(demand) 
        if is_fuel_type == 'is_coal':
            return self.f_totalConsCoal(demand)
        if is_fuel_type == 'is_oil':
            return self.f_totalConsOil(demand)
        if is_fuel_type == 'is_nuclear':
            return self.f_totalConsNuclear(demand)
        if is_fuel_type == 'is_hydro':
            return self.f_totalConsHydro(demand)
        if is_fuel_type == 'is_geothermal':
            return self.f_totalConsGeothermal(demand)
        if is_fuel_type == 'is_biomass':
            return self.f_totalConsBiomass(demand)
      
    def calcFullMeritOrder(self):
        """ Calculates the base_ and marg_ co2, so2, nox, and coal_mix, where "base_" represents the online "base load" that does not 
        change with marginal changes in demand and "marg_" represents the marginal portion of the merit order that does change with marginal
        changes in demand. The calculation of base_ and marg_ changes depending on whether the minimum output constraint (the include_min_output variable)
        is being used. In general, "base" is a value (e.g. 'full_gen_cost_tot_base' has units [$], and 'full_co2_base' has units [kg]) while "marg" 
        is a rate (e.g. 'full_gen_cost_tot_marg' has units [$/MWh], and 'full_co2_marg' has units [kg/MWh]). When the dispatch object solves the dispatch, 
        it calculates the total emissions for one time period as 'full_co2_base' + 'full_co2_marg' * (marginal generation MWh) to end up with units of [kg].
        ---
        """
        df = self.df.copy(deep=True)
        temp = df.f.apply(self.returnTotalFuelMix, args=(('is_coal'),)) - self.returnTotalFuelMix(self.coal_mdt_demand_threshold, 'is_coal')
        binary_demand_is_below_demand_threshold = (scipy.maximum(0, - temp.fillna(0)) > 0).values.astype(int) # calcs if min downtime
        weight_marginal_unit = (1-self.mdt_weight) + self.mdt_weight*(1-binary_demand_is_below_demand_threshold) # calcs min downtime weight
        weight_mindowntime_units = 1 - weight_marginal_unit
        #INCLUDING MIN OUTPUT
        if self.include_min_output:
            
            #total production cost
            df['full_gen_cost_tot_base'] = (0.1*df.a.apply(self.returnTotalCost) + 0.9*df.s.apply(self.returnTotalCost) 
                                            + df.s.apply(self.returnMarginalGenerator, args=('gen_cost',)) 
                                            * df.s.apply(self.returnMarginalGenerator, args=('min_out',))) #calculate the base production cost [$]
            df['full_gen_cost_tot_marg'] = (((df.s.apply(self.returnTotalCost) - df.a.apply(self.returnTotalCost)) 
                                             / (df.s-df.a) * (df.min_out/(df.f-df.s)) + df.s.apply(self.returnMarginalGenerator, args=('gen_cost',)) 
                                             * (1 -(df.min_out/(df.f-df.s)))).fillna(0.0)) #calculate the marginal base production cost [$/MWh]
            # FOR DEBUGGING: tempDF = df.loc[:][["f",  "s", "min_out", "a", "full_gen_cost_tot_base", "full_gen_cost_tot_marg"]]
            #emissions
            for e in ['co2', 'so2', 'nox']:
                # base emissions here is not meant to match base calculations when not including min_output, NOTE: not sure where this equation comes from
                df['full_' + e + '_base'] = (0.1*df.a.apply(self.returnTotalEmissions, args=(e,)) + 
                                             0.9*df.s.apply(self.returnTotalEmissions, args=(e,)) + df.s.apply(self.returnMarginalGenerator, args=(e,)) 
                                             * df.s.apply(self.returnMarginalGenerator, args=('min_out',))) #calculate the base emissions [kg]
                #scipy.multiply(MEF of normal generation, weight of normal genearation) + 
                #scipy.multiply(MEF of mdt_reserves, weight of mdt_reserves) where MEF of normal generation 
                #is the calculation that happens without accounting for mdt, weight of normal generation is ((f-s) / ((f-s)) + mdt_reserves) 
                #and MEF of mdt_reserves is total_value_mdt_emissions / total_mw_mdt_reserves
                df['full_' + e + '_marg'] = (scipy.multiply(((df.s.apply(self.returnTotalEmissions, args=(e,)) - 
                                                              df.a.apply(self.returnTotalEmissions, args=(e,))) / (df.s-df.a) * 
                                                             (df.min_out/(df.f-df.s)) + df.s.apply(self.returnMarginalGenerator, args=(e,)) * 
                                                             (1 -(df.min_out/(df.f-df.s)))).fillna(0.0), weight_marginal_unit) # emissions of minimum downtime units
                                             + scipy.multiply(scipy.divide(
                                                 scipy.maximum(0, - (df.f.apply(self.returnTotalEmissions_Coal, args=(e,)) 
                                                                     - self.returnTotalEmissions_Coal(self.coal_mdt_demand_threshold, e))),  
                                                 scipy.maximum(0, - (df.f.apply(self.returnTotalFuelMix, args=(('is_coal'),)) 
                                                                     - self.returnTotalFuelMix(self.coal_mdt_demand_threshold, 'is_coal'))))
                                                 .fillna(0.0).replace(scipy.inf, 0.0),  weight_mindowntime_units))
            
            # #emissions damages
            # df['full_dmg_easiur_base'] = 0.1*df.a.apply(self.returnTotalEasiurDamages) + 0.9*df.s.apply(self.returnTotalEasiurDamages) + df.s.apply(self.returnMarginalGenerator, args=('dmg_easiur',)) * df.s.apply(self.returnMarginalGenerator, args=('min_out',)) #calculate the base easiur damages [$]
            # #scipy.multiply(MEF of normal generation, weight of normal genearation) + scipy.multiply(MEF of mdt_reserves, weight of mdt_reserves) where MEF of normal generation is the calculation that happens without accounting for mdt, weight of normal generation 
            # is ((f-s) / ((f-s)) + mdt_reserves) and MEF of mdt_reserves is total_value_mdt_emissions / total_mw_mdt_reserves
            # df['full_dmg_easiur_marg'] = scipy.multiply(  ((df.s.apply(self.returnTotalEasiurDamages) - df.a.apply(self.returnTotalEasiurDamages)) / (df.s-df.a) * (df.min_out/(df.f-df.s)) + df.s.apply(self.returnMarginalGenerator, args=('dmg_easiur',)) * (1 -(df.min_out/(df.f-df.s)))).fillna(0.0)  
            # ,  weight_marginal_unit  ) + scipy.multiply(  scipy.divide(scipy.maximum(0, - (df.f.apply(self.returnTotalEasiurDamages_Coal) - self.returnTotalEasiurDamages_Coal(self.coal_mdt_demand_threshold)))  ,  scipy.maximum(0, - (df.f.apply(self.returnTotalFuelMix, args=(('is_coal'),)) - 
            # self.returnTotalFuelMix(self.coal_mdt_demand_threshold, 'is_coal')))).fillna(0.0).replace(scipy.inf, 0.0)  ,  weight_mindowntime_units  )
            
            #fuel mix
            for fl in ['gas', 'coal', 'oil', 'nuclear', 'hydro', 'geothermal', 'biomass']:
            #for fl in ['gas', 'coal', 'oil']:
                df['full_' + fl + '_mix_base'] = 0.1*df.a.apply(self.returnTotalFuelMix, args=(('is_'+fl),)) + 0.9*df.s.apply(self.returnTotalFuelMix, args=(('is_'+fl),)) + self.df['is_'+fl] * df.s.apply(self.returnMarginalGenerator, args=('min_out',)) #calculate the base fuel_mix [MWh]
                #scipy.multiply(dmgs of normal generation, weight of normal genearation) + scipy.multiply(dmgs of mdt_reserves, weight of mdt_reserves) where dmgs of normal generation is the calculation that happens without accounting for mdt, weight of normal generation is ((f-s) / ((f-s)) + mdt_reserves) and dmgs of mdt_reserves is total_value_mdt_reserves / total_mw_mdt_reserves
                fuel_multiplier = scipy.where(fl=='coal', 1.0, 0.0)
                df['full_' + fl + '_mix_marg'] = (scipy.multiply(((
                    df.s.apply(self.returnTotalFuelMix, args=(('is_'+fl),)) - df.a.apply(self.returnTotalFuelMix, args=(('is_'+fl),)))
                    / (df.s-df.a) * (df.min_out/(df.f-df.s)) + 
                    df.s.apply(self.returnMarginalGenerator, args=(('is_'+fl),)) * (1 -(df.min_out/(df.f-df.s)))).fillna(0.0)  ,  weight_marginal_unit  ) 
                    +  scipy.multiply(  scipy.divide(scipy.maximum(0, - (df.f.apply(self.returnTotalFuelMix, args=(('is_coal'),))
                                                                         - self.returnTotalFuelMix(self.coal_mdt_demand_threshold, 'is_coal'))),
                                                     scipy.maximum(0, - (df.f.apply(self.returnTotalFuelMix, args=(('is_coal'),)) 
                                                                         - self.returnTotalFuelMix(self.coal_mdt_demand_threshold, 'is_coal'))))
                                      .fillna(0.0).replace(scipy.inf, 0.0) * fuel_multiplier  ,  weight_mindowntime_units  ))
            
            #fuel consumption
            for fl in ['gas', 'coal', 'oil', 'nuclear', 'hydro', 'geothermal', 'biomass']:
                df['full_' + fl + '_consumption_base'] = (0.1*df.a.apply(self.returnTotalFuelConsumption, args=(('is_'+fl),)) + 
                                                          0.9*df.s.apply(self.returnTotalFuelConsumption, args=(('is_'+fl),)) + 
                                                          self.df['is_'+fl] * df.s.apply(self.returnMarginalGenerator, args=('heat_rate',)) * 
                                                          df.s.apply(self.returnMarginalGenerator, args=('min_out',))) #calculate the base fuel consumption [mmBtu]
                #scipy.multiply(mmbtu/mw of normal generation, weight of normal genearation) + 
                #scipy.multiply(mmbtu/mw of mdt_reserves, weight of mdt_reserves) 
                #where mmbtu/mw of normal generation is the calculation that happens without accounting for mdt, 
                #weight of normal generation is ((f-s) / ((f-s)) + mdt_reserves) and mmbtu/mw of mdt_reserves is total_value_mdt_reserves / total_mw_mdt_reserves
                fuel_multiplier = scipy.where(fl=='coal', 1.0, 0.0)
                df['full_' + fl + '_consumption_marg'] = (scipy.multiply(((df.s.apply(self.returnTotalFuelConsumption, args=('is_'+fl,)) 
                                                                           - df.a.apply(self.returnTotalFuelConsumption, args=('is_'+fl,))) / (df.s-df.a) 
                                                                          * (df.min_out/(df.f-df.s)) + df.s.apply(self.returnMarginalGenerator, args=('is_'+fl,))
                                                                          * df.s.apply(self.returnMarginalGenerator, args=('heat_rate',)) 
                                                                          * (1 -(df.min_out/(df.f-df.s)))).fillna(0.0),  weight_marginal_unit  )  
                                                          +  scipy.multiply(scipy.divide(
                                                              scipy.maximum(0, - (df.f.apply(self.returnTotalFuelConsumption, args=(('is_coal'),)) 
                                                                                  - self.returnTotalFuelConsumption(self.coal_mdt_demand_threshold, 'is_coal'))),
                                                              scipy.maximum(0, - (df.f.apply(self.returnTotalFuelMix, args=(('is_coal'),)) 
                                                                                  - self.returnTotalFuelMix(self.coal_mdt_demand_threshold, 'is_coal'))))
                                                              .fillna(0.0).replace(scipy.inf, 0.0) * fuel_multiplier,  weight_mindowntime_units))         
        
        #EXCLUDING MIN OUTPUT
        if not self.include_min_output:
            #total production cost
            df['full_gen_cost_tot_base'] = df.s.apply(self.returnTotalCost) #calculate the base production cost, which is now the full load production cost of the generators in the merit order below the marginal unit [$]
            df['full_gen_cost_tot_marg'] = df.s.apply(self.returnMarginalGenerator, args=('gen_cost',)) #calculate the marginal production cost, which is now just the generation cost of the marginal generator [$/MWh]
            #emissions
            for e in ['co2', 'so2', 'nox']:
                # full_base will differ depending on if using min_output because marginal plant will not automatically have some min_output capacity when fired
                df['full_' + e + '_base'] = df.s.apply(self.returnTotalEmissions, args=(e,)) #calculate the base emissions, which is now the full load emissions of the generators in the merit order below the marginal unit [kg]
                df['full_' + e + '_marg'] = (scipy.multiply(df.s.apply(self.returnMarginalGenerator, args=(e,))  ,   weight_marginal_unit  ) 
                                             + scipy.multiply(
                                                 scipy.divide(scipy.maximum(0, - (df.f.apply(self.returnTotalEmissions_Coal, args=(e,)) 
                                                                                  - self.returnTotalEmissions_Coal(self.coal_mdt_demand_threshold, e))),  
                                                              scipy.maximum(0, - (df.f.apply(self.returnTotalFuelMix, args=(('is_coal'),)) 
                                                                                  - self.returnTotalFuelMix(self.coal_mdt_demand_threshold, 'is_coal'))))
                                                 .fillna(0.0).replace(scipy.inf, 0.0)  ,  weight_mindowntime_units  ))
            # #emissions damages
            # df['full_dmg_easiur_base'] = df.s.apply(self.returnTotalEasiurDamages) #calculate the total Easiur damages
            # df['full_dmg_easiur_marg'] = scipy.multiply(  df.s.apply(self.returnMarginalGenerator, args=('dmg_easiur',))  ,  
            # weight_marginal_unit  ) + scipy.multiply(  scipy.divide(scipy.maximum(0, - (df.f.apply(self.returnTotalEasiurDamages_Coal) 
            # - self.returnTotalEasiurDamages_Coal(self.coal_mdt_demand_threshold)))  ,  scipy.maximum(0, - (df.f.apply(self.returnTotalFuelMix,
            # args=(('is_coal'),)) - self.returnTotalFuelMix(self.coal_mdt_demand_threshold, 'is_coal')))).fillna(0.0).replace(scipy.inf, 0.0)  ,  weight_mindowntime_units  )
            #fuel mix
            for fl in ['gas', 'coal', 'oil', 'nuclear', 'hydro', 'geothermal', 'biomass']:
                df['full_' + fl + '_mix_base'] = df.s.apply(self.returnTotalFuelMix, args=(('is_'+fl),)) #calculate the base fuel_mix, which is now the full load coal mix of the generators in the merit order below the marginal unit [MWh]
                fuel_multiplier = scipy.where(fl=='coal', 1.0, 0.0)
                df['full_' + fl + '_mix_marg'] = (scipy.multiply(df.s.apply(self.returnMarginalGenerator, args=(('is_'+fl),))  ,  weight_marginal_unit  )
                                                  +scipy.multiply(scipy.divide(
                                                      scipy.maximum(0, - (df.f.apply(self.returnTotalFuelMix, args=(('is_coal'),)) 
                                                                          - self.returnTotalFuelMix(self.coal_mdt_demand_threshold, 'is_coal'))), 
                                                      scipy.maximum(0, - (df.f.apply(self.returnTotalFuelMix, args=(('is_coal'),)) 
                                                                          - self.returnTotalFuelMix(self.coal_mdt_demand_threshold, 'is_coal'))))
                                                      .fillna(0.0).replace(scipy.inf, 0.0) * fuel_multiplier  ,  weight_mindowntime_units  ))
            #fuel consumption
            for fl in ['gas', 'coal', 'oil', 'nuclear', 'hydro', 'geothermal', 'biomass']:
                #calculate the base fuel_consumption, which is now the fuel consumption of the generators in the merit order below the marginal unit [MWh]
                df['full_' + fl + '_consumption_base'] = df.s.apply(self.returnTotalFuelConsumption, args=(('is_'+fl),)) 
                fuel_multiplier = scipy.where(fl=='coal', 1.0, 0.0)
                df['full_' + fl + '_consumption_marg'] = (scipy.multiply(df.s.apply(self.returnMarginalGenerator, args=('is_'+fl,)) 
                                                                         * df.s.apply(self.returnMarginalGenerator, args=('heat_rate',)),  
                                                                         weight_marginal_unit)  +  
                                                          scipy.multiply(  scipy.divide(
                                                              scipy.maximum(0, - (df.f.apply(self.returnTotalFuelConsumption, args=(('is_coal'),)) 
                                                                                  - self.returnTotalFuelConsumption(self.coal_mdt_demand_threshold, 'is_coal'))),
                                                              scipy.maximum(0, - (df.f.apply(self.returnTotalFuelMix, args=(('is_coal'),)) 
                                                                                  - self.returnTotalFuelMix(self.coal_mdt_demand_threshold, 'is_coal'))))
                                                              .fillna(0.0).replace(scipy.inf, 0.0) * fuel_multiplier ,  weight_mindowntime_units))
        #update the master dataframe df
        self.df = df
        
        ## if subsetting, prepare subset dataframe
        if self.states_to_subset != []: # check if there are states in the list
            df_subset = df.copy(deep=True) # create a copy to manipulate
            
            ## steps to subset; the goal is to create a step function that is constant between demand from non-subset units
            ## and increases for demand from subset units.
            ## in the existing code, base X + marginal X*(max capacity) != base X of n+1 unit
            ## from prior testing, these differences are slight (<0.1%) and will not significantly impact results
            ## but we follow these steps to maintain a flat slope in the demand between units we are trying to subset
            # 1. set X-marg to 0 for non-subset units
            # 2. create a new column of X-base (X-base-temp) shifted down one (move to n+1 row)
            # 3. set X-base-temp equal to X-base - X-base-temp
            # 4. set X-base-temp to 0 for rows not after subset unit rows
            # 5. new X-base (X-base-new) = cumulative sum of X-base-temp (cumsum) for subset units
            # 6. drop X-base-temp
            # 7. drop non-subset units that are not directly before subset units
            # 8. set unit directly following subsetted unit and is not itself a subsetted unit to have base X (n) = base X (n-1) + marginal X * marginal demand (n-1)

            mask_of_subset_units = df_subset["state"].isin(self.states_to_subset) # subset units in states we want 
            for e in ['co2', 'so2', 'nox']: # loop through emissions columns
                # 1. set X-marg to 0 for non-subset units
                df_subset.loc[~mask_of_subset_units, 'full_' + e + '_marg'] = 0 
                # 2. create a new column of X-base (X-base-temp) shifted down one (move to n+1 row)
                df_subset['full_' + e + '_base_temp'] = df_subset['full_' + e + '_base'].shift(1).fillna(0) 
                # 3. set X-base-temp equal to X-base - X-base-temp
                df_subset['full_' + e + '_base_temp'] = df_subset['full_' + e + '_base'] - df_subset['full_' + e + '_base_temp'] 
                # 4. set X-base-temp to 0 for rows not after subset unit rows
                temp_mask = mask_of_subset_units.shift(1).fillna(False) # rows directly after subset unit rows
                df_subset.loc[~temp_mask, 'full_' + e + '_base_temp'] = 0
                # 5. new X-base (X-base-new) = cumulative sum of X-base-temp (cumsum)
                df_subset['full_' + e + '_base'] = df_subset['full_' + e + '_base_temp'].cumsum() 
                # 6. drop X-base-temp
                df_subset = df_subset.drop(['full_' + e + '_base_temp'], axis=1) 

            # 7. drop non-subset units that are not directly before subset units
            temp_mask = mask_of_subset_units.shift(-1).fillna(False) # mask for units before subsetted units
            temp_mask = temp_mask | mask_of_subset_units # mask for units before subsetted units or subsetted units
            df_subset = pandas.concat([df_subset.loc[[1, 2], :], # preserve first 2 null rows for edge cases 
                                df_subset.drop(df_subset[~temp_mask].index, axis=0)], axis=0) # drop rest of un-needed rows
            mask_of_subset_units = df_subset["state"].isin(self.states_to_subset) # re-make mask of subsetted units in states we want
            
            # 8. set unit directly following subsetted unit and is not itself a subsetted unit to have base X (n) = base X (n-1) + marginal X * marginal demand (n-1)
            temp_mask = mask_of_subset_units.shift(1).fillna(False) # mask for n+1 units, where n rows are subsetted units
            temp_mask = temp_mask & ~mask_of_subset_units # mask for n+1 units that are not themselves n units
            temp_mask2 = temp_mask.shift(-1).fillna(False) # rows that precede the prior mask (subsetted units)
            for e in ['co2', 'so2', 'nox']: # I coded poorly, so we gotta loop through emissions columns again
                temp = (df_subset.loc[temp_mask2, 'full_' + e + '_base'] + 
                        numpy.multiply(df_subset.loc[temp_mask2, 'full_' + e + '_marg'], 
                                       df_subset.loc[temp_mask2, "demand"] - df_subset.loc[temp_mask2, "s"])) # perform the operation
                # replace relevant units
                df_subset.loc[temp_mask, 'full_' + e + '_base'] = temp.reindex_like(df_subset.loc[temp_mask, 'full_' + e + '_base'], method='ffill') 
            
            self.df_subset = df_subset # update subsetted copy of dataframe

    def returnFullMarginalValue(self, demand, col_type):
        """ Given demand and col_type inputs, return the col_type (i.e. 'co2' for marginal co2 emissions rate or 'coal_mix' for coal share of the generation)
        of the marginal units in the Full model (the Full model includes the minimum output constraint).
        ---
        demand : [MW]
        col_type : 'co2', 'so2', 'nox', 'coal_mix', etc.
        return : full_"emissions_type"_marg as calculated in the Full model: calcFullMeritOrder
        """
        return self.returnMarginalGenerator(demand, 'full_' + col_type + '_marg')


    def createTotalInterpolationFunctionsFull(self):
        """ Creates interpolation functions for the full total data (i.e. total cost, total emissions, etc.) depending on total demand.
        general form of equations:
            x is cumulative demand (test.demand) in order of the merit order (price order)
            y is base emissions/cost (test['full_xxx_base']; aka emissions from prior units) 
                + marginal emissions (cumulative demand less demand from prior units) * emissions rate from marginal unit
        """       
        test = self.df.copy()      
        #cost
        self.f_totalCostFull = scipy.interpolate.interp1d(test.demand, test['full_gen_cost_tot_base'] + (test['demand'] - test['s']) * test['full_gen_cost_tot_marg'])  
        #emissions and health damages
        self.f_totalCO2Full = scipy.interpolate.interp1d(test.demand, test['full_co2_base'] + (test['demand'] - test['s']) * test['full_co2_marg'])
        self.f_totalSO2Full = scipy.interpolate.interp1d(test.demand, test['full_so2_base'] + (test['demand'] - test['s']) * test['full_so2_marg'])
        self.f_totalNOXFull = scipy.interpolate.interp1d(test.demand, test['full_nox_base'] + (test['demand'] - test['s']) * test['full_nox_marg'])
        # self.f_totalDmgFull = scipy.interpolate.interp1d(test.demand, test['full_dmg_easiur_base'] + (test['demand'] - test['s']) * test['full_dmg_easiur_marg'])
        #fuel mix
        self.f_totalGasFull = scipy.interpolate.interp1d(test.demand, test['full_gas_mix_base'] + (test['demand'] - test['s']) * test['full_gas_mix_marg'])
        self.f_totalCoalFull = scipy.interpolate.interp1d(test.demand, test['full_coal_mix_base'] + (test['demand'] - test['s']) * test['full_coal_mix_marg'])
        self.f_totalOilFull = scipy.interpolate.interp1d(test.demand, test['full_oil_mix_base'] + (test['demand'] - test['s']) * test['full_oil_mix_marg'])
        self.f_totalNuclearFull = scipy.interpolate.interp1d(test.demand, test['full_nuclear_mix_base'] + (test['demand'] - test['s']) * test['full_nuclear_mix_marg'])
        self.f_totalHydroFull = scipy.interpolate.interp1d(test.demand, test['full_hydro_mix_base'] + (test['demand'] - test['s']) * test['full_hydro_mix_marg'])
        self.f_totalGeothermalFull = scipy.interpolate.interp1d(test.demand, test['full_geothermal_mix_base'] + (test['demand'] - test['s']) * test['full_geothermal_mix_marg'])
        self.f_totalBiomassFull = scipy.interpolate.interp1d(test.demand, test['full_biomass_mix_base'] + (test['demand'] - test['s']) * test['full_biomass_mix_marg'])
        #fuel consumption
        self.f_totalConsGasFull = scipy.interpolate.interp1d(test.demand, test['full_gas_consumption_base'] + (test['demand'] - test['s']) * test['full_gas_consumption_marg'])
        self.f_totalConsCoalFull = scipy.interpolate.interp1d(test.demand, test['full_coal_consumption_base'] + (test['demand'] - test['s']) * test['full_coal_consumption_marg'])
        self.f_totalConsOilFull = scipy.interpolate.interp1d(test.demand, test['full_oil_consumption_base'] + (test['demand'] - test['s']) * test['full_oil_consumption_marg'])
        self.f_totalConsNuclearFull = scipy.interpolate.interp1d(test.demand, test['full_nuclear_consumption_base'] + (test['demand'] - test['s']) * test['full_nuclear_consumption_marg'])
        self.f_totalConsHydroFull = scipy.interpolate.interp1d(test.demand, test['full_hydro_consumption_base'] + (test['demand'] - test['s']) * test['full_hydro_consumption_marg'])
        self.f_totalConsGeothermalFull = scipy.interpolate.interp1d(test.demand, test['full_geothermal_consumption_base'] + (test['demand'] - test['s']) * test['full_geothermal_consumption_marg'])
        self.f_totalConsBiomassFull = scipy.interpolate.interp1d(test.demand, test['full_biomass_consumption_base'] + (test['demand'] - test['s']) * test['full_biomass_consumption_marg'])
        
        ## if subsetting, prepare subset functions (only for emissions)
        if self.states_to_subset != []: # check if there are states in the list
            test = self.df_subset.copy()
            # for all functions, set out of bounds value equal to the highest value in the list 
            temp = test['full_co2_base'] + (test['demand'] - test['s']) * test['full_co2_marg'] # CO2
            self.f_totalCO2Full_subset = scipy.interpolate.interp1d(test.demand, temp, 
                                                        bounds_error=False, fill_value=temp.iloc[-1])
            temp = test['full_so2_base'] + (test['demand'] - test['s']) * test['full_so2_marg'] # SO2
            self.f_totalSO2Full_subset = scipy.interpolate.interp1d(test.demand, temp, 
                                                        bounds_error=False, fill_value=temp.iloc[-1])
            temp = test['full_nox_base'] + (test['demand'] - test['s']) * test['full_nox_marg'] # NOx
            self.f_totalNOXFull_subset = scipy.interpolate.interp1d(test.demand, temp, 
                                                        bounds_error=False, fill_value=temp.iloc[-1])
        

    def returnFullTotalValue(self, demand, col_type):
        """ Given demand and col_type inputs, return the total column of the online power plants in the Full model 
        (the Full model includes the minimum output constraint).
        ---
        demand : [MW]
        col_type : 'co2', 'so2', 'nox', 'coal_mix', etc.
        return : total emissions = base emissions (marginal unit) + marginal emissions (marginal unit) * (D - s)
        """
        if col_type == 'gen_cost_tot':
            return self.f_totalCostFull(demand)       
        if col_type == 'co2':
            return self.f_totalCO2Full(demand)
        if col_type == 'so2':
            return self.f_totalSO2Full(demand)
        if col_type == 'nox':
            return self.f_totalNOXFull(demand)
        if col_type == 'dmg_easiur':
            return self.f_totalDmgFull(demand)
        if col_type == 'gas_mix':
            return self.f_totalGasFull(demand) 
        if col_type == 'coal_mix':
            return self.f_totalCoalFull(demand)
        if col_type == 'oil_mix':
            return self.f_totalOilFull(demand)
        if col_type == 'nuclear_mix':
            return self.f_totalNuclearFull(demand)
        if col_type == 'hydro_mix':
            return self.f_totalHydroFull(demand)
        if col_type == 'geothermal_mix':
            return self.f_totalGeothermalFull(demand)
        if col_type == 'biomass_mix':
            return self.f_totalBiomassFull(demand)
        if col_type == 'gas_consumption':
            return self.f_totalConsGasFull(demand) 
        if col_type == 'coal_consumption':
            return self.f_totalConsCoalFull(demand)
        if col_type == 'oil_consumption':
            return self.f_totalConsOilFull(demand)
        if col_type == 'nuclear_consumption':
            return self.f_totalConsNuclearFull(demand)
        if col_type == 'hydro_consumption':
            return self.f_totalConsHydroFull(demand)
        if col_type == 'geothermal_consumption':
            return self.f_totalConsGeothermalFull(demand)
        if col_type == 'biomass_consumption':
            return self.f_totalConsBiomassFull(demand)
        
    
    def returnFullTotalValueSubset(self, demand, col_type):
        """ Given demand and col_type inputs, return the total column of the online power plants in the Full model 
        (the Full model includes the minimum output constraint).
        This is a modification of the above function (of the same name minus "Subset") that calculates total emissions for subset states
        ---
        demand : [MW]
        col_type : 'co2', 'so2', 'nox'
        return : total emissions = base emissions (marginal unit) + marginal emissions (marginal unit) * (D - s)
        """
        if col_type == 'co2':
            return self.f_totalCO2Full_subset(demand)
        if col_type == 'so2':
            return self.f_totalSO2Full_subset(demand)
        if col_type == 'nox':
            return self.f_totalNOXFull_subset(demand)
    
    
    def plotBidStack(self, df_column, plot_type, fig_dim = (4,4), production_cost_only=True):
        """ Given a name for the df_column, plots a bid stack with demand on the x-axis and the df_column data on the y-axis. 
        For example bidStack.plotBidStack('gen_cost', 'bar') would output the traditional merit order curve.
        ---
        df_column : column header from df, e.g. 'gen_cost', 'co2', 'so2', etc.
        plot_type : 'bar' or 'line'
        production_cost_only : if True, the dispatch cost will exclude carbon, so2 and nox taxes. if False, all costs will be included
        fig_dim = figure dimensions (#,#)
        return : bid stack plot
        """	
        #color for any generators without a fuel_color entry
        empty_color = '#dd1c77'
        #create color array for the emissions cost
        color_2 = self.df.fuel_color.replace('', empty_color)
        color_2 = color_2.replace('#888888', '#F0F0F0')
        color_2 = color_2.replace('#bf5b17', '#E0E0E0')
        color_2 = color_2.replace('#7fc97f', '#D8D8D8')
        color_2 = color_2.replace('#252525', '#D0D0D0')
        color_2 = color_2.replace('#dd1c77', '#C0C0C0')
        color_2 = color_2.replace('#bcbddc', '#E0E0E0')
        #set up the y data
        y_data_e = self.df.gen_cost * 0 #emissions bar chart. Default is zero unless not production_cost_only                    
        if df_column == 'gen_cost':
            y_lab = 'Generation Cost [$/MWh]'
            y_data = self.df[df_column] - (self.df.co2_cost + self.df.so2_cost + self.df.nox_cost) #cost excluding emissions taxes
            if not production_cost_only:
                y_data_e = self.df[df_column]
        if df_column == 'co2':
            y_lab = 'CO$_2$ Emissions [kg/MWh]'
            y_data = self.df[df_column + str(self.time)]
        if df_column == 'so2':
            y_lab = 'SO$_2$ Emissions [kg/MWh]'
            y_data = self.df[df_column + str(self.time)]
        if df_column == 'nox':
            y_lab = 'NO$_x$ Emissions [kg/MWh]'
            y_data = self.df[df_column + str(self.time)]
        #create the data to be stacked on y_data to show the cost of the emission tax
        matplotlib.pylab.clf()
        f = matplotlib.pylab.figure(figsize=fig_dim)
        ax = f.add_subplot(111)
        if plot_type == 'line':
            ax.plot( self.df.demand/1000, y_data, linewidth=2.5)
        elif plot_type == 'bar':
            ax.bar(self.df.demand/1000, height=y_data_e, width=-scipy.maximum(0.2, self.df['mw' + str(self.time)]/1000), color=color_2, align='edge'),
            ax.bar(self.df.demand/1000, height=y_data, width=-scipy.maximum(0.2, self.df['mw' + str(self.time)]/1000), 
                   color=self.df.fuel_color.replace('', empty_color), align='edge')
            
            ## add legend above chart
            color_legend = []
            for c in self.df.fuel_color.unique():
                color_legend.append(matplotlib.patches.Patch(color=c, label=self.df.fuel_type[self.df.fuel_color==c].iloc[0]))
            ax.legend(handles=color_legend, bbox_to_anchor=(0.5, 1.2), loc='upper center', ncol=3, fancybox=True, shadow=True)
        else:
            print('***Error: enter valid argument for plot_type')
            pass
        matplotlib.pylab.ylim(ymax=y_data.quantile(0.98)) #take the 98th percentile for the y limits.
        #ax.set_xlim(self.hist_dispatch.demand.quantile(0.025)*0.001, self.hist_dispatch.demand.quantile(0.975)*0.001) #take the 2.5th and 97.5th percentiles for the x limits
        ax.set_xlim(0, self.hist_dispatch.demand.quantile(0.975)*0.001) #take 0 and the 97.5th percentiles for the x limits
        if self.nerc == 'MRO':
            ax.set_xticks((10,15,20))
        if self.nerc == 'TRE':
            ax.set_xticks((20,30,40,50))
        if self.nerc == 'FRCC':
            ax.set_xticks((15,20,25,30))
        if self.nerc == 'WECC':
            ax.set_xticks((20,30,40,50,60))
        if self.nerc == 'SERC':
            ax.set_xticks((0,25,50,75,100))
        if df_column == 'gen_cost':
            if production_cost_only:
                ax.set_ylim(0, 65)
                ax.set_yticks((0, 15, 30, 45, 60))
            if not production_cost_only:
                ax.set_ylim(0, 160)
                ax.set_yticks((0, 30, 60, 90, 120, 150))    
        if df_column == 'co2':
            ax.set_ylim(0, 1300)
            ax.set_yticks((250, 500, 750, 1000, 1250))
        matplotlib.pylab.xlabel('Generation [GW]')
        matplotlib.pylab.ylabel(y_lab)
        matplotlib.pylab.tight_layout()
        matplotlib.pylab.show()
        return f
    
    
    def plotBidStackMultiColor(self, df_column, plot_type, fig_dim = (4,4), production_cost_only=True, show_legend=True):
        """
        plots merit order and emission rates

        Parameters
        ----------
        df_column : string
            "gen_cost", "co2", "so2", or "nox" (column to plot)
        plot_type : string
            "line" or "bar"
        fig_dim : TYPE, optional
            DESCRIPTION. The default is (4,4).
        production_cost_only : TYPE, optional
            DESCRIPTION. The default is True.
        show_legend : TYPE, optional
            DESCRIPTION. The default is True.

        Returns
        -------
        f : matplotlib plot
            plotted merit order or emissions rates

        """
        bs_df_fuel_color = self.df.copy()
        # colors for all fuel and prime mover types
        c = {'ng': {'cc': '#377eb8', 'ct': '#377eb8', 'gt': '#4daf4a', 'st': '#984ea3'}, 
             'sub': {'st': '#e41a1c'}, 
             'lig': {'st': '#ffff33'}, 
             'bit': {'st': '#ff7f00'}, 
             'rc': {'st': '#252525'}, # refined coal
             'rfo': {'st': '#D0F43B'}, # residual oil
             'dfo': {'st': '#47EDFA'}} # distillate oil # SC (syncoal) & og ("other" gas) still not represented
        labels = {'sub_st': 'Subbituminous Coal',
                  'bit_st': 'Bituminous Coal',
                  'ng_ct': 'Natural Gas Combined Cycle',
                  'ng_st': 'Natural Gas Steam Turbine',
                  'ng_gt': 'Natural Gas Combustion Turbine'}
                    
        bs_df_fuel_color['fuel_color'] = '#bcbddc' # default color
        for c_key in c.keys():
            for p_key in c[c_key].keys():
                bs_df_fuel_color.loc[(bs_df_fuel_color.fuel == c_key) & (bs_df_fuel_color.prime_mover == p_key), 'fuel_color'] = c[c_key][p_key]
        
        #color for any generators without a fuel_color entry; NOTE: can probably remove?
        empty_color = '#dd1c77'
        color_2 = bs_df_fuel_color.fuel_color.replace('', empty_color)

        #set up the y data
        y_data_e = self.df.gen_cost * 0 #emissions bar chart. Default is zero unless not production_cost_only
        if df_column == 'gen_cost':
            y_lab = 'Generation Cost [$/MWh]'
            y_data = self.df[df_column] - (self.df.co2_cost + self.df.so2_cost + self.df.nox_cost) #cost excluding emissions taxes
            if not production_cost_only:
                y_data_e = self.df[df_column]
        if df_column == 'co2':
            y_lab = 'CO$_2$ Emissions [kg/MWh]'
            y_data = self.df[df_column + str(self.time)]
        if df_column == 'so2':
            y_lab = 'SO$_2$ Emissions [kg/MWh]'
            y_data = self.df[df_column + str(self.time)]
        if df_column == 'nox':
            y_lab = 'NO$_x$ Emissions [kg/MWh]'
            y_data = self.df[df_column + str(self.time)]
        #create the data to be stacked on y_data to show the cost of the emission tax
        matplotlib.pylab.clf()
        f = matplotlib.pylab.figure(figsize=fig_dim)
        ax = f.add_subplot(111)
        if plot_type == 'line':
            ax.plot( self.df.demand/1000, y_data, linewidth=2.5)
        elif plot_type == 'bar':
            ax.bar(self.df.demand/1000, height=y_data_e, width=-scipy.maximum(0.2, self.df['mw' + str(self.time)]/1000), color=color_2, align='edge') 
            ax.bar(self.df.demand/1000, height=y_data, width=-scipy.maximum(0.2, self.df['mw' + str(self.time)]/1000), color=color_2, align='edge') # self.df[mw_] retrieves historical capacity limit
            
            ## add legend above chart
            if show_legend:
                color_legend = []
                for c in bs_df_fuel_color.fuel_color.unique():
                    # color_legend.append(matplotlib.patches.Patch(color=c, 
                    #                                              label=bs_df_fuel_color.fuel[bs_df_fuel_color.fuel_color==c].iloc[0] 
                    #                                              + '_' + bs_df_fuel_color.prime_mover[bs_df_fuel_color.fuel_color==c].iloc[0]))
                    orig_label = (bs_df_fuel_color.fuel[bs_df_fuel_color.fuel_color==c].iloc[0] 
                                  + '_' + bs_df_fuel_color.prime_mover[bs_df_fuel_color.fuel_color==c].iloc[0])
                    if orig_label in labels:
                        color_legend.append(matplotlib.patches.Patch(color=c, 
                                                                     label=labels[orig_label]))
                ax.legend(handles=color_legend, bbox_to_anchor=(0.5,1.5), loc='upper center', 
                          ncol=2, fancybox=False, shadow=False) # empty ('_') will be ignored
        else:
            print('***Error: enter valid argument for plot_type')
            pass
        matplotlib.pylab.ylim(ymax=y_data.quantile(0.975)) #take the 97.5th percentile for the y limits.
        #ax.set_xlim(bs.hist_dispatch.demand.quantile(0.025)*0.001, bs.hist_dispatch.demand.quantile(0.975)*0.001) #take the 2.5th and 97.5th percentiles for the x limits
        ax.set_xlim(0, self.hist_dispatch.demand.quantile(0.975)*0.001) #take 0 and the 97.5th percentiles for the x limits
        if df_column == 'gen_cost':
            if production_cost_only:
                ax.set_ylim(0, 90)
                ax.set_yticks((0, 15, 30, 45, 60, 75, 90))
            if not production_cost_only:
                ax.set_ylim(0, 160)
                ax.set_yticks((0, 30, 60, 90, 120, 150))    
        if df_column == 'co2':
            ax.set_ylim(0, 1300)
            ax.set_yticks((250, 500, 750, 1000, 1250))
        if df_column == 'so2':
            ax.set_ylim(0, 15)
            ax.set_yticks((0, 3, 6, 9, 12, 15))
        matplotlib.pylab.xlabel('Generation [GW]')
        matplotlib.pylab.ylabel(y_lab)
        matplotlib.pylab.tight_layout()
        matplotlib.pylab.show()
        return f
    
    
    def plotBidStackMultiColor_Coal_NGCC_NGGT_NGOther(self, df_column, plot_type, fig_dim = (4,4), production_cost_only=True):    
        bs_df_fuel_color = self.df.copy()
        
        c = {'ng': {'cc': '#1b9e77', 'ct': '#1b9e77', 'gt': '#fc8d62', 'st': '#8da0cb'}, 'sub': {'st': '#252525'}, 'lig': {'st': '#252525'}, 'bit': {'st': '#252525'}, 'rc': {'st': '#252525'}}
                    
        bs_df_fuel_color['fuel_color'] = '#bcbddc'
        for c_key in c.keys():
            for p_key in c[c_key].keys():
                bs_df_fuel_color.loc[(bs_df_fuel_color.fuel == c_key) & (bs_df_fuel_color.prime_mover == p_key), 'fuel_color'] = c[c_key][p_key]
        
        #color for any generators without a fuel_color entry
        empty_color = '#dd1c77'
        #hold the colors
        color_2 = bs_df_fuel_color.fuel_color.replace('', empty_color)
        #create color array for the emissions cost
        color_3 = self.df.fuel_color.replace('', empty_color)
        color_3 = color_3.replace('#888888', '#F0F0F0')
        color_3 = color_3.replace('#bf5b17', '#E0E0E0')
        color_3 = color_3.replace('#7fc97f', '#D8D8D8')
        color_3 = color_3.replace('#252525', '#D0D0D0')
        color_3 = color_3.replace('#dd1c77', '#C0C0C0')
        color_3 = color_3.replace('#bcbddc', '#E0E0E0')
        #set up the y data
        y_data_e = self.df.gen_cost * 0 #emissions bar chart. Default is zero unless not production_cost_only                    
        if df_column == 'gen_cost':
            y_lab = 'Generation Cost [$/MWh]'
            y_data = self.df[df_column] - (self.df.co2_cost + self.df.so2_cost + self.df.nox_cost) #cost excluding emissions taxes
            if not production_cost_only:
                y_data_e = self.df[df_column]
        if df_column == 'co2':
            y_lab = 'CO$_2$ Emissions [kg/MWh]'
            y_data = self.df[df_column + str(self.time)]
        if df_column == 'so2':
            y_lab = 'SO$_2$ Emissions [kg/MWh]'
            y_data = self.df[df_column + str(self.time)]
        if df_column == 'nox':
            y_lab = 'NO$_x$ Emissions [kg/MWh]'
            y_data = self.df[df_column + str(self.time)]
        #create the data to be stacked on y_data to show the cost of the emission tax
        matplotlib.pylab.clf()
        f = matplotlib.pylab.figure(figsize=fig_dim)
        ax = f.add_subplot(111)
        if plot_type == 'line':
            ax.plot( self.df.demand/1000, y_data, linewidth=2.5)
        elif plot_type == 'bar':
            ax.bar(self.df.demand/1000, 
                   height=y_data_e, width=-scipy.maximum(0.2, self.df['mw' + str(self.time)]/1000), 
                   color=color_3, align='edge'), 
            ax.bar(self.df.demand/1000, 
                   height=y_data, 
                   width=-scipy.maximum(0.2, self.df['mw' + str(self.time)]/1000), 
                   color=color_2, align='edge')
            ##add legend above chart
            #color_legend = []
            #for c in self.df.fuel_color.unique():
            #    color_legend.append(matplotlib.patches.Patch(color=c, label=self.df.fuel_type[self.df.fuel_color==c].iloc[0]))
            #ax.legend(handles=color_legend, bbox_to_anchor=(0.5, 1.2), loc='upper center', ncol=3, fancybox=True, shadow=True)
        else:
            print('***Error: enter valid argument for plot_type')
            pass
        matplotlib.pylab.ylim(ymax=y_data.quantile(0.98)) #take the 98th percentile for the y limits.
        #ax.set_xlim(self.hist_dispatch.demand.quantile(0.025)*0.001, self.hist_dispatch.demand.quantile(0.975)*0.001) #take the 2.5th and 97.5th percentiles for the x limits
        ax.set_xlim(0, self.hist_dispatch.demand.quantile(0.975)*0.001) #take 0 and the 97.5th percentiles for the x limits
        if self.nerc == 'MRO':
            ax.set_xticks((10,15,20))
        if self.nerc == 'TRE':
            ax.set_xticks((20,30,40,50))
        if self.nerc == 'FRCC':
            ax.set_xticks((15,20,25,30))
        if self.nerc == 'WECC':
            ax.set_xticks((20,30,40,50,60))
        if self.nerc == 'SERC':
            ax.set_xticks((0,25,50,75,100))
        if self.nerc == 'SPP':
            ax.set_xlim(0, 40)
            ax.set_xticks((0,10, 20, 30, 40))
        if df_column == 'gen_cost':
            if production_cost_only:
                ax.set_ylim(0, 65)
                ax.set_yticks((0, 15, 30, 45, 60))
            if not production_cost_only:
                ax.set_ylim(0, 120)
                ax.set_yticks((0, 30, 60, 90, 120))    
        if df_column == 'co2':
            ax.set_ylim(0, 1300)
            ax.set_yticks((250, 500, 750, 1000, 1250))
        matplotlib.pylab.xlabel('Generation [GW]')
        matplotlib.pylab.ylabel(y_lab)
        matplotlib.pylab.tight_layout()
        matplotlib.pylab.show()
        return f
    
    
    
    
    

class dispatch(object):
    def __init__(self, bid_stack_object, demand_df,  states_to_subset = [], time_array=0):
        """ Read in bid stack object and the demand data. Solve the dispatch by projecting the bid stack onto the demand time series,
            updating the bid stack object regularly according to the time_array
        ---
        gen_data_object : a object defined by class generatorData
        states_to_subset : list of 2-letter capital abbreviation of all states in which emissions will be subset
        bid_stack_object : a bid stack object defined by class bidStack
        demand_df : a dataframe with the demand data 
        time_array : a scipy array containing the time intervals that we are changing fuel price etc. 
        for. E.g. if we are doing weeks, then time_array=numpy.arange(52) + 1 to get an array of (1, 2, 3, ..., 51, 52)
        """
        self.bs = bid_stack_object
        self.df = demand_df
        self.time_array = time_array
        self.states_to_subset = states_to_subset
        self.addDFColumns() # adds columns to demand df to hold results
        
               
    def addDFColumns(self):
        """ Add additional columns to self.df to hold the results of the dispatch. New cells initially filled with zeros
        ---
        """
        indx = self.df.index
        cols = scipy.array(('gen_cost_marg', 'gen_cost_tot', 'co2_marg', 'co2_tot', 'so2_marg', 'so2_tot', 'nox_marg', 
                            'nox_tot', 'biomass_mix', 'coal_mix', 'gas_mix', 'geothermal_mix', 'hydro_mix', 'nuclear_mix',
                            'oil_mix', 'marg_gen', 'coal_mix_marg', 'marg_gen_fuel_type', 'mmbtu_coal', 'mmbtu_gas', 'mmbtu_oil'))
        dfExtension = pandas.DataFrame(index=indx, columns=cols).fillna(0)
        self.df = pandas.concat([self.df, dfExtension], axis=1)
        
        if self.states_to_subset != []: # if there are states to subset, create miniature dataframe for subset results
            self.df_subset = self.df[['datetime', 'demand', 'co2_tot', 'so2_tot', 'nox_tot']].copy(deep=True)


    def calcDispatchSlice(self, bstack, start_date=0, end_date=0):
        """ For each datum in demand time series (e.g. each hour) between start_date and end_date calculate the dispatch
        ---
        bstack: an object created using the simple_dispatch.bidStack class
        start_datetime : string of format '2014-01-31' i.e. 'yyyy-mm-dd'. If argument == 0, uses start date of demand time series
        end_datetime : string of format '2014-01-31' i.e. 'yyyy-mm-dd'. If argument == 0, uses end date of demand time series
        """
        if start_date==0:
            start_date = self.df.datetime.min()
        else:
            start_date = pandas._libs.tslib.Timestamp(start_date)
        if end_date==0:
            end_date = self.df.datetime.max()
        else:
            end_date = pandas._libs.tslib.Timestamp(end_date)
        #slice of self.df within the desired dates    
        df_slice = self.df[(self.df.datetime >= pandas._libs.tslib.Timestamp(start_date)) & 
                           (self.df.datetime < pandas._libs.tslib.Timestamp(end_date))].copy(deep=True)
        #calculate the dispatch for the slice by applying the return###### functions of the bstack object
        df_slice['gen_cost_marg'] = df_slice.demand.apply(bstack.returnMarginalGenerator, args=('gen_cost',)) #generation cost of the marginal generator ($/MWh)
        df_slice['gen_cost_tot'] = df_slice.demand.apply(bstack.returnFullTotalValue, args=('gen_cost_tot',)) #generation cost of the total generation fleet ($)
        for e in ['co2', 'so2', 'nox']:
            df_slice[e + '_marg'] = df_slice.demand.apply(bstack.returnFullMarginalValue, args=(e,)) #emissions rate (kg/MWh) of marginal generators
            df_slice[e + '_tot'] = df_slice.demand.apply(bstack.returnFullTotalValue, args=(e,)) #total emissions (kg) of online generators
        # df_slice['dmg_easiur'] = df_slice.demand.apply(bstack.returnFullTotalValue, args=('dmg_easiur',)) #total easiur damages ($)
        for f in ['gas', 'oil', 'coal', 'nuclear', 'biomass', 'geothermal', 'hydro']:
            df_slice[f + '_mix'] = df_slice.demand.apply(bstack.returnFullTotalValue, args=(f+'_mix',))
        df_slice['coal_mix_marg'] = df_slice.demand.apply(bstack.returnFullMarginalValue, args=('coal_mix',))
        df_slice['marg_gen_fuel_type'] = df_slice.demand.apply(bstack.returnMarginalGenerator, args=('fuel_type',))
        df_slice['mmbtu_coal'] = df_slice.demand.apply(bstack.returnFullTotalValue, args=('coal_consumption',)) #total coal mmBtu
        df_slice['mmbtu_gas'] = df_slice.demand.apply(bstack.returnFullTotalValue, args=('gas_consumption',)) #total gas mmBtu        
        df_slice['mmbtu_oil'] = df_slice.demand.apply(bstack.returnFullTotalValue, args=('oil_consumption',)) #total oil mmBtu
        self.df[(self.df.datetime >= pandas._libs.tslib.Timestamp(start_date)) & (self.df.datetime < pandas._libs.tslib.Timestamp(end_date))] = df_slice
        
        if self.states_to_subset != []: # if there are states to subset, repeat for emissions
            #slice of self.df within the desired dates    
            df_slice = self.df_subset[(self.df_subset.datetime >= pandas._libs.tslib.Timestamp(start_date)) & 
                               (self.df_subset.datetime < pandas._libs.tslib.Timestamp(end_date))].copy(deep=True)
            for e in ['co2', 'so2', 'nox']:
                df_slice[e + '_tot'] = df_slice.demand.apply(bstack.returnFullTotalValueSubset, args=(e,)) #total emissions (kg) of subsetted online generators 
            # replace df slice in relevant period
            self.df_subset[(self.df_subset.datetime >= pandas._libs.tslib.Timestamp(start_date)) 
                           & (self.df_subset.datetime < pandas._libs.tslib.Timestamp(end_date))] = df_slice
  

    def createDfMdtCoal(self, demand_threshold, time_t):
        """ For a given demand threshold, creates a new version of the generator data that approximates the minimum down time constraint for coal plants
        ---
        demand_threshold: the system demand below which some coal plants will turn down to minimum rather than turning off
        returns a dataframe of the same format as gd.df but updated so the coal generators in the merit order below demand_threshold have 
        their capacities reduced by their minimum output, their minimum output changed to zero, and the sum of their minimum outputs applied 
        to the capacity of coal_0, where coal_0 also takes the weighted average of their heat rates, emissions, rates, etc. 
        Note that this new dataframe only contains the updated coal plants, but not the complete gd.df information 
        (i.e. for gas plants and higher cost coal plants), but it can be incorporated back into the original df (i.e. bs.df_0) using the pandas update command.
        """
        #set the t (time i.e. week) object
        t = time_t
        #get the orispl_unit information for the generators you need to adjust
        coal_mdt_orispl_unit_list = list(self.bs.df[(self.bs.df.fuel_type=='coal') & (self.bs.df.demand <= demand_threshold)].orispl_unit.copy().values)
        coal_mdt_gd_idx = self.bs.df_0[self.bs.df_0.orispl_unit.isin(coal_mdt_orispl_unit_list)].index
        
        #create a new set of generator data where there is a large coal unit at the very bottom representing the baseload of the coal generators 
        # if they do not turn down below their minimum output, and all of the coal generators have their capacity reduced to (1-min_output).         
        df_mdt_coal = self.bs.df_0[self.bs.df_0.orispl_unit.isin(coal_mdt_orispl_unit_list)][
            ['orispl_unit', 'fuel', 'fuel_type', 'prime_mover', 'vom', 'min_out_multiplier', 'min_out', 
             'co2%i'%t, 'so2%i'%t, 'nox%i'%t, 'heat_rate%i'%t, 'mw%i'%t, 'fuel_price%i'%t]].copy()
        df_mdt_coal = df_mdt_coal[df_mdt_coal.orispl_unit != 'coal_0']
        #create a pandas Series that will hold the large dummy coal unit that represents coal base load
        df_mdt_coal_base = df_mdt_coal.copy().iloc[0]
        df_mdt_coal_base[['orispl_unit', 'fuel', 'fuel_type', 'prime_mover', 'min_out_multiplier', 'min_out']] = ['coal_0', 'sub', 'coal', 'st', 0.0, 0.0]
        #columns for the week we are currently solving
        # t_columns = ['orispl_unit', 'fuel_type', 'prime_mover', 'vom', 'min_out_multiplier', 'co2%i'%t, 'so2%i'%t, 'nox%i'%t, 'heat_rate%i'%t, 'mw%i'%t, 'fuel_price%i'%t, 'dmg%i'%t]
        t_columns = ['orispl_unit', 'fuel_type', 'prime_mover', 'vom', 'min_out_multiplier', 'co2%i'%t, 'so2%i'%t, 'nox%i'%t, 'heat_rate%i'%t, 'mw%i'%t, 'fuel_price%i'%t]
        df_mdt_coal_base_temp = df_mdt_coal[t_columns].copy()       
        #the capacity of the dummy coal unit will be the sum of the minimum output of all the coal units      
        df_mdt_coal_base_temp['mw%i'%t] = df_mdt_coal_base_temp['mw%i'%t] * df_mdt_coal_base_temp.min_out_multiplier
        #the vom, co2, so2, nox, heat_rate, fuel_price, and dmg of the dummy unit will equal the weighted average of the other coal plants
        weighted_cols = df_mdt_coal_base_temp.columns.drop(['orispl_unit', 'fuel_type', 'prime_mover', 'min_out_multiplier', 'mw%i'%t])
        # append a new row with 1 mw capacity if the sum of all capacities would be 0 otherwise. This avoides an error in the next dividing operation
        if df_mdt_coal_base_temp['mw%i'%t].sum() == 0: df_mdt_coal_base_temp.loc[0, ['mw%i'%t]] = 1
        df_mdt_coal_base_temp[weighted_cols] = (df_mdt_coal_base_temp[weighted_cols].multiply(df_mdt_coal_base_temp['mw%i'%t], axis='index') 
                                                / df_mdt_coal_base_temp['mw%i'%t].sum()) 
        df_mdt_coal_base_temp = df_mdt_coal_base_temp.sum(axis=0)
        #update df_mdt_coal_base with df_mdt_coal_base_temp, which holds the weighted average characteristics of the other coal plants
        df_mdt_coal_base[['vom', 'co2%i'%t, 'so2%i'%t, 'nox%i'%t, 'heat_rate%i'%t, 'mw%i'%t, 'fuel_price%i'%t]] = df_mdt_coal_base_temp[
            ['vom', 'co2%i'%t, 'so2%i'%t, 'nox%i'%t, 'heat_rate%i'%t, 'mw%i'%t, 'fuel_price%i'%t]]
        #reduce the capacity of the other coal plants by their minimum outputs (since their minimum outputs are now a part of coal_0)
        df_mdt_coal.loc[df_mdt_coal.fuel_type == 'coal','mw%i'%t] = df_mdt_coal[df_mdt_coal.fuel_type == 'coal'][['mw%i'%t]].multiply(
            (1-df_mdt_coal[df_mdt_coal.fuel_type == 'coal'].min_out_multiplier), axis='index')
        #add coal_0 to df_mdt_coal    
        dtype_dict = df_mdt_coal.dtypes.to_dict() # to preserve data types
        df_mdt_coal = pandas.concat([df_mdt_coal, df_mdt_coal_base.to_frame().T], axis=0, ignore_index = True) # appends coal_0
        df_mdt_coal = df_mdt_coal.astype(dtype_dict) # re-casts types to correct ones
        # df_mdt_coal = df_mdt_coal.append(df_mdt_coal_base, ignore_index = True)
        #change the minimum output of the coal plants to 0.0
        df_mdt_coal.loc[df_mdt_coal.fuel_type == 'coal',['min_out_multiplier', 'min_out']] = [0.0, 0.0]
        #update the index to match the original bidStack
        df_mdt_coal.index = coal_mdt_gd_idx
        return df_mdt_coal
    
    
    def calcMdtCoalEventsT(self, start_datetime, end_datetime, coal_merit_order_input_df):
        """ For a given demand threshold, creates a new version of the generator data that approximates the minimum down time constraint for coal plants
        ---
        demand_threshold: the system demand below which some coal plants will turn down to minimum rather than turning off
        returns a dataframe of the same format as gd.df but updated so the coal generators in the merit order below demand_threshold 
        have their capacities reduced by their minimum output, their minimum output changed to zero, 
        and the sum of their minimum outputs applied to the capacity of coal_0, where coal_0 also takes the 
        weighted average of their heat rates, emissions, rates, etc.
        """
        #the function below returns the demand value of the merit_order_input_df that is just above the demand_input_scalar
        def bisect_column(demand_input_scalar, merit_order_input_df):
            try: 
                out = coal_merit_order_input_df.iloc[bisect_left(list(coal_merit_order_input_df.demand),demand_input_scalar)].demand   
        #if demand_threshold exceeds the highest coal_merit_order.demand value (i.e. all of min output constraints are binding for coal)
            except:
                out = coal_merit_order_input_df.iloc[-1].demand
            return out  
        #bring in the coal mdt events calculated in generatorData        
        mdt_coal_events_t = self.bs.mdt_coal_events.copy()
        #slice the coal mdt events based on the current start/end section of the dispatch solution
        mdt_coal_events_t = mdt_coal_events_t[(mdt_coal_events_t.end >= start_datetime) & (mdt_coal_events_t.start <= end_datetime)]
        #translate the demand_thresholds into the next highest demand data in the merit_order_input_df. This will allow us to reduce the number of bidStacks we need to generate. 
        # E.g. if two days have demand thresholds of 35200 and 35250 but the next highest demand in the coal merit order is 36000, then both of these days can use the 36000 mdt_bidStack, 
        # and we can recalculate the bidStack once instead of twice. 
        mdt_coal_events_t['demand_threshold'] = mdt_coal_events_t.demand_threshold.apply(bisect_column, args=(coal_merit_order_input_df,))
        return mdt_coal_events_t
    
              
    def calcDispatchAll(self):
        """ Runs calcDispatchSlice for each time slice in the fuel_prices_over_time dataframe, NOTE: this description looks really old
        creating a new bidstack each time. So, fuel_prices_over_time contains multipliers (e.g. 0.95 or 1.14) for each 
        fuel type (e.g. ng, lig, nuc) for different slices of time (e.g. start_date = '2014-01-07' and end_date = '2014-01-14'). 
        We use these multipliers to change the fuel prices seen by each generator in the bidStack object. 
        After changing each generator's fuel prices (using bidStack.updateFuelPrices), we re-calculate the bidStack merit order (using bidStack.calcGenCost),
        and then calculate the dispatch for the slice of time defined by the fuel price multipliers. 
        This way, instead of calculating the dispatch over the whole year, 
        we can calculate it in chunks of time (e.g. weeks) where each chunk of time has different fuel prices for the generators. 
        Right now the only thing changing per chunk of time is the fuel prices based on trends in national commodity prices. 
        Future versions might try and do regional price trends and add things like maintenance downtime or other seasonal factors.
        ---
        fills in the self.df dataframe one time slice at a time
        """
        #run the whole solution if self.fuel_prices_over_time isn't being used
        if scipy.shape(self.time_array) == (): #might be a more robust way to do this. Would like to say if ### == 0, but doing that when ### is a dataframe gives an error
            self.calcDispatchSlice(self.bs)
        #otherwise, run the dispatch in time slices, updating the bid stack each slice
        else:
            for t in self.time_array:
                #update the bidStack object to the current week - reprocesses merit order for current week's fuel prices
                self.bs.updateTime(t)
                #calculate the dispatch for the time slice over which the updated fuel prices are relevant
                start = (datetime.datetime.strptime(str(self.bs.year) + '-01-01', '%Y-%m-%d') + datetime.timedelta(days=7.05*(t-1)-1)).strftime('%Y-%m-%d') 
                end = (datetime.datetime.strptime(str(self.bs.year) + '-01-01', '%Y-%m-%d') + datetime.timedelta(days=7.05*(t)-1)).strftime('%Y-%m-%d')
                if (self.bs.year % 4 == 0) & (t == 52): # account for leap years, add in extra day in last week
                    end = (datetime.datetime.strptime(str(self.bs.year) + '-01-01', '%Y-%m-%d') + datetime.timedelta(days=7.05*(t))).strftime('%Y-%m-%d') 
                #note that calcDispatchSlice updates self.df, so there is no need to do it in this calcDispatchAll function
                self.calcDispatchSlice(self.bs, start_date=start ,end_date=end)
                #coal minimum downtime
                #recalculate the dispatch for times when generatorData pre-processing estimates that the minimum downtime constraint for coal plants would trigger
                #define the coal merit order
                coal_merit_order = self.bs.df[(self.bs.df.fuel_type == 'coal')][['orispl_unit', 'demand']]
                #slice and bin the coal minimum downtime events
                events_mdt_coal_t = self.calcMdtCoalEventsT(start, end, coal_merit_order)  
                #create a dictionary for holding the updated bidStacks, which change depending on the demand_threshold                
                bs_mdt_dict = {}
                #for each unique demand_threshold
                for dt in events_mdt_coal_t.demand_threshold.unique():
                    #create an updated version of gd.df
                    gd_df_mdt_temp = self.bs.df_0.copy()
                    gd_df_mdt_temp.update(self.createDfMdtCoal(dt, t))
                    #use that updated gd.df to create an updated bidStack object, and store it in the bs_mdt_dict
                    bs_temp = copy.deepcopy(self.bs)
                    bs_temp.coal_mdt_demand_threshold = dt
                    bs_temp.updateDf(gd_df_mdt_temp)
                    bs_mdt_dict.update({dt:bs_temp})
                #for each minimum downtime event, recalculate the dispatch by inputting the bs_mdt_dict bidStacks into calcDispatchSlice to override the existing dp.df results dataframe
                for i, e in events_mdt_coal_t.iterrows():
                    self.calcDispatchSlice(bs_mdt_dict[e.demand_threshold], start_date=e.start ,end_date=e.end)
                print(str(round(t/float(len(self.time_array)),3)*100) + '% Complete')
                

if __name__ == '__main__': 
    print('nothing')