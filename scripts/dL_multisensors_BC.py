#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  7 16:12:07 2022

@author: vjs
"""

## Code to download turbidity data in Barkley canyon for finite chunk
import numpy as np
from onc.onc import ONC
import os
import pandas as pd
import onc_download_module as odm
import logging, sys




# %% Paths
#online_locations_path =  '/Users/vjs/turbidites/observational/data/getOnlineInstruments_jul15/onlineInstruments_all.csv'
online_locations_path =  '/home/dkilb/barkley/data/onlineInstruments_all.csv'

## Download directory:
data_dir = '/home/dkilb/barkley/data/'

## Logfile path:
logfile_path = '/home/dkilb/barkley/data/LOG_dLmultisensors_Bark_8_4_2022.log'

## codes etc. to search for
#$propertyCodes = ['turbidityftu','turbidityntu','seawatertemperature','oxygen','pressure','chlorophyll']

## After inspecting plots, these are the locations where turbidity data exist:
download_locationCodes = ['BACAX','BACHY','BACMW','BACUS']

## KEY:
    # BACAX:  Barkley Canyon Axis
    # BACCH:  Barkley Canyon Head
    # BACHY:  Barkley CAnyon Hydrates
    # BACME:  Barkley Canyon MidEast
    # BACMW:  Barkley Canyon MidWest
    # BACND:  Barkley Canyon Node
    # BACUS:  Barkley Canyon Upper Slope South


## Number of download tries:
download_tries = 40

## sleep time in seconds, in between bad 202 responses:
sleeptime = 5


## Search parmaeters needed:
search_params_list = ['method','token','locationCode','propertyCode','dataProductCode','extension','dateFrom','dateTo','dpo_qualityControl','dpo_resample','dpo_average','dpo_dataGaps']

## Set up logging, so it gives only level name (like INFO), and the message (no root)
logging.basicConfig(format='%(levelname)s:%(message)s',filename=logfile_path, level=logging.DEBUG)
logger = logging.getLogger()

## send both standard error and out here
sys.stderr.write = logger.error
sys.stdout.write = logger.info

#### NOTE!! HACK JOB!!
## A lot of data for BACAX turbidityntu were already downloaded. To avoid re=-doing those,
## modify the search indices in the ij_location_propertydata, ONLY for bacax turbidityntu.
## This index, to start on from ij_location_propertydata, is 3742, and should be 2021-09-17T00:00:00.000Z
hackindex_bacax_turbityntu = 4473

## The date:
hackdate = '2021-09-17T00:00:00.000Z'


# %%  Get Token %% #
token = odm.Token()
onc = ONC(token)

# %%

## Make download table to loop through.

## Load in table with online instruments data:
online_instruments = pd.read_csv(online_locations_path)

## Add the data download columns to it:
online_instruments['delivery_error'] = np.full(len(online_instruments),'None')
online_instruments['request_error'] = np.full(len(online_instruments),'None')
online_instruments['download_error'] = np.full(len(online_instruments),'None')
online_instruments['request_ok'] = np.full(len(online_instruments),False)
online_instruments['dL_status_counter'] = np.full(len(online_instruments),0)
online_instruments['filePath'] = np.full(len(online_instruments),'No file')

## Loop over every station of interest to determine which data to download, then make a dataframe, and download.
for i_station_index in range(len(download_locationCodes)):
    i_station = download_locationCodes[i_station_index]

    print('Working on station {}... \n'.format(i_station))
    
    # GEt the subset dataframe for this, only where data exist:
    i_station_data = online_instruments.loc[((online_instruments.locationCode == i_station) & (online_instruments.data_exists == True))]
    
    ## Find the unique dateFroms at this station for all days in which some kind of turbidity data exist:
    i_turbiditydataexist_dateFrom = i_station_data.loc[((i_station_data.propertyCode == 'turbidityntu') | (i_station_data.propertyCode == 'turbidityftu'))].dateFrom.unique()
    
    ## Get what rows exist for all sensors, for days with the same datefrom as the turbidity data existing:
    i_alldata_turbiditydates = i_station_data[i_station_data.dateFrom.isin(i_turbiditydataexist_dateFrom)]
    
    if len(i_alldata_turbiditydates) == 0:
        print('No data for this station, moving on...')
        continue    
    ##
    ## Now we start the overall download process.
    ##
    
    ## TEst if the directory for the station exists - if not, make one:
    if os.path.exists(data_dir + i_station) == False:
        ## make it...
        os.mkdir(data_dir + i_station)
        
    ## Then, loop through each instrument in this new dataframe, and make a directory/run the download separately:
    i_properties = i_alldata_turbiditydates.propertyCode.unique()
    
    ## For each property code...
    for j_property in i_properties:
        
        print('Working on {} \n'.format(j_property))
        ## Find where data exist:
        ij_location_property_data = i_alldata_turbiditydates.loc[i_alldata_turbiditydates.propertyCode == j_property]

        ## If there's no data, continue.
        if len(ij_location_property_data) == 0:
            print('No data for this instrument, moving on...')
            continue
        
        
        ## If no data directory exists...
        ij_datadir = data_dir + i_station + '/' + j_property
        if os.path.exists(ij_datadir) == False:
            ## Make it:
            os.mkdir(ij_datadir)
            os.mkdir(ij_datadir + '/metadata')
    
        ## Then, get the dataframe to use in the download:
        ## The indices:
        ij_search_indices = ij_location_property_data.index.values
        
        #### HACK JOB!!!!
        ## If it's BACAX, and turbidityntu, a lot of data are already downloaded.
        ## To avoid re=-doing those,
        ## start from an index in the i_alldata_turbiditydates dataframe, ONLY for bacax turbidityntu.
        if ((i_station == 'BACAX') & (j_property == 'turbidityntu')):
            ij_hackdate_index = ij_location_property_data.loc[ij_location_property_data.dateFrom == hackdate].index[0]
            ij_search_indices = ij_search_indices[np.where(ij_search_indices >= ij_hackdate_index)]
            
            ## Write the preserarch dataframe to the metadata directory:
            ij_location_property_data.to_csv(ij_datadir + '/metadata/predownload_metadata_TRY2_' + i_station + '_' + j_property + '.csv')
        
        else:
            ## Write the preserarch dataframe to the metadata directory:
            ij_location_property_data.to_csv(ij_datadir + '/metadata/predownload_metadata_' + i_station + '_' + j_property + '.csv')
        
        ## Search, download, and get the post search dataframe:
        ij_postsearch_parameter_df = odm.batch_test_and_download(search_params_list,ij_location_property_data,ij_search_indices,token,ij_datadir,download_tries)
        
# %%
## Download, v0. Attempt all.

# ## Search parmaeters needed:
# search_params_list = ['method','token','locationCode','propertyCode','dataProductCode','extension','dateFrom','dateTo','dpo_qualityControl','dpo_resample','dpo_average','dpo_dataGaps']
# search_indices_v0 = np.arange(numdays)

# ## Searcha nd download...
# print('RUNNING SEARCH V0 ')
# postsearch_v0_parameter_df = odm.batch_test_and_download(search_params_list,presearch_parameter_df,search_indices_v0,token,data_dir,download_tries)


# # %% SECOND TIME TO RUN: 
# ## Run where data exist, and where there is no file.
# search_indices_v1 = np.where((postsearch_v0_parameter_df['data_exists'] == True) & (postsearch_v0_parameter_df['filePath'] == 'No file'))[0]
# metadata_path_suffix = 'v1'

# ## Search:
# postsearch_v1_parameter_df = odm.batch_test_and_download(search_params_list,postsearch_v0_parameter_df,search_indices_v1,token,data_dir,download_tries,metadata_path_suffix)


    

