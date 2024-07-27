#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 24 17:20:56 2024

@author: vjs
"""

## Code to download turbidity data in Barkley canyon for finite chunk
import numpy as np
from onc.onc import ONC
import os
import pandas as pd
import onc_download_module as odm
import logging, sys


# %%

## Download directory:
data_dir = '/Users/vjs/turbidites/observational/data/testADCP'

## Logfile path:
logfile_path = '/Users/vjs/turbidites/observational/data/testADCP/LOG_dL_ADCPtest_BACAX_7_24_2024.log'

## codes etc. to search for
#$propertyCodes = ['turbidityftu','turbidityntu','seawatertemperature','oxygen','pressure','chlorophyll']


## After inspecting plots, these are the locations where turbidity data exist:
download_locationCodes = ['BACAX']

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
search_params_list = ['method','token','locationCode','deviceCategoryCode','dataProductCode','extension','dateFrom','dateTo']

## Set up logging, so it gives only level name (like INFO), and the message (no root)
logging.basicConfig(format='%(levelname)s:%(message)s',filename=logfile_path, level=logging.DEBUG)
logger = logging.getLogger()

## send both standard error and out here
sys.stderr.write = logger.error
sys.stdout.write = logger.info

# %%  Get Token %% #
token = odm.Token()
onc = ONC(token)



# %%
## Make the search paramters list, property data, etc.


ij_presearch_dict =  {'method':np.array(['request','request']),
               'token':np.array([token, token]),    # replace YOUR_TOKEN_HERE with your personal token obtained from the 'Web Services API' tab at https://data.oceannetworks.ca/Profile when logged in.
               'locationCode':np.array(['BACAX', 'BACAX']), # Barkley Canyon / Axis (POD 1)
                'deviceCategoryCode':np.array(['ADCP2MHZ','ADCP2MHZ']),    # 2MHz Acoustic Doppler Current Profiler
                'dataProductCode':np.array(['NTS','NDCP']),           # Nortek Time Series, Nortek Daily Currents Plot
                'extension':np.array(['nc','png']),                  # NetCDF, png
                'dateFrom':np.array(['2018-01-01T00:00:00.000Z','2018-01-01T00:00:00.000Z']),  # The datetime of the first data point (From Date)
                'dateTo':np.array(['2018-01-02T00:00:00.000Z','2018-01-02T00:00:00.000Z']),    # The datetime of the last data point (To Date)
                # 'dpo_qualityControl':np.array([]),             # The Quality Control data product option - See https://wiki.oceannetworks.ca/display/DP/1
                # 'dpo_resample':np.full(numdays,'average'),              # The Resampling data product option - See https://wiki.oceannetworks.ca/display/DP/1
                # 'dpo_average':np.full(numdays,60),                   # Resampling average, 60=1 minutes
                # 'dpo_dataGaps':np.full(numdays,0),
                'delivery_error':np.array(['None','None']),
                'data_exists':np.array([False,False])} 

ij_presearch_df = pd.DataFrame(ij_presearch_dict)
# Run for all indices...
ij_search_indices = ij_presearch_df.index.values   
## Directory forthe test
ij_datadir = data_dir

# d(search_params_list,presearch_parameter_df,search_indices,token,data_dir,download_tries,metadata_path_suffix='v0',sleeptime=10):
    # https://data.oceannetworks.ca/api/dataProductDelivery?method=request&token=YOUR_TOKEN_HERE&locationCode=BACAX&deviceCategoryCode=ADCP2MHZ&dataProductCode=NTS&extension=nc&dateFrom=2018-01-01T00:00:00.000Z&dateTo=2018-01-02T00:00:00.000Z


# %%
## Search, download, and get the post search dataframe:
    
    
ij_presearch_df['delivery_error'] = np.full(len(ij_presearch_df),'None')
ij_presearch_df['request_error'] = np.full(len(ij_presearch_df),'None')
ij_presearch_df['download_error'] = np.full(len(ij_presearch_df),'None')
ij_presearch_df['request_ok'] = np.full(len(ij_presearch_df),False)
ij_presearch_df['dL_status_counter'] = np.full(len(ij_presearch_df),0)
ij_presearch_df['filePath'] = np.full(len(ij_presearch_df),'No file')
    
ij_postsearch_parameter_df = odm.batch_test_and_download(search_params_list,ij_presearch_df,ij_search_indices,token,ij_datadir,download_tries)

