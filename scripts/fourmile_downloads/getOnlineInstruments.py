#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  7 16:12:07 2022

@author: vjs
"""

## Code to download turbidity data in Barkley canyon for finite chunk
import numpy as np
from onc.onc import ONC
import requests
import re
import tkinter as tk
from appdirs import user_data_dir
import os
#root = tk.Tk()  
import pandas as pd
import requests
import json
import os
from contextlib import closing
import errno
import sys
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter
from datetime import datetime, timedelta
import time


import onc_download_module as odm

# %% Paths
## Test download path:
data_dir = '/Users/vjs/turbidites/observational/data/getOnlineInstruments_jul15'

## codes etc. to search for
propertyCodes = ['turbidityftu','turbidityntu','seawatertemperature','oxygen','pressure','chlorophyll']
locationCodes = ['BACAX','BACCH','BACHY','BACME','BACMW','BACND','BACUS']

## Run later, in another script...

#other = ['FGPPN','CSBR','CSBF']

## LOCATION NOTES:
# BACAX:  B. Canyon  Axis (benthic)
# BACCH:  B. Canyon Head (benthic)
# BACHY:  B. Canyon Hydrates
# BACME:  B. Canyon Mideast (canyon wall)
# BACMW:  B. Canyon MidWest (canyon wall)
# BACND:  B. Node (shelf)
# BACUS:  B. Upper Slope South
# FGPPN:  Folger Passage (upstream of barkley canyon)
# CSBR:   Cascadia Seaweed Burrough Point Reference Site (upstream of barkley canyon)
# CSBF:   Cascadia Seaweed Burrough Point Farm (upstream of barkley canyon)


## DAtetimes to run: - NOTE: Cannot mess with microseconds!! Only seconds!
start_download = datetime(2016,1,1)
end_download = datetime(2022,7,15)

## List of parameters to use in search:
search_params_list = ['method','token','locationCode','propertyCode','dataProductCode','extension','dateFrom','dateTo','dpo_qualityControl','dpo_resample','dpo_average','dpo_dataGaps']


# %%  Get Token %% #
token = odm.Token()
onc = ONC(token)


## Make download array:
start_dates_array = np.arange(start_download,end_download,timedelta(days=1))
end_dates_array = np.arange(start_download+(timedelta(days=1)),end_download+(timedelta(days=1)),timedelta(days=1))

## Make arrays for property codes:



# %%

## Make download table to loop through.
## Get number of days to try to download:
numdays = len(start_dates_array)

## First, Convert hte times to strings in the appropriate format:
string_start_array = []
string_end_array = []

## Convert the dates to the appropriate format:
for i_row in range(numdays):
    i_date_from = start_dates_array[i_row]
    i_date_to = end_dates_array[i_row]
    
    string_start_array.append(i_date_from.astype(datetime).strftime('%Y-%m-%dT%H:%M:%S') + '.000Z')
    string_end_array.append(i_date_to.astype(datetime).strftime('%Y-%m-%dT%H:%M:%S') + '.000Z')



## Make parameter dictionaries and dataframes in a loop
dataframes_list = []

for j_location in range(len(locationCodes)):
    for i_code in range(len(propertyCodes)):
        
        ## Create dataframe and save
        i_parameter_dict = {'method':np.full(numdays,'request'),
                       'token':np.full(numdays,token),    # replace YOUR_TOKEN_HERE with your personal token obtained from the 'Web Services API' tab at https://data.oceannetworks.ca/Profile when logged in.
                       'locationCode':np.full(numdays,locationCodes[j_location]), # Barkley Canyon / Axis (POD 1)
                        'propertyCode':np.full(numdays,propertyCodes[i_code]),    # 150 kHz Acoustic Doppler Current Profiler
                        'dataProductCode':np.full(numdays,'TSSD'),           # Time Series Scalar Data
                        'extension':np.full(numdays,'csv'),                  # Comma Separated spreadsheet file
                        'dateFrom':string_start_array,  # The datetime of the first data point (From Date)
                        'dateTo':string_end_array,    # The datetime of the last data point (To Date)
                        'dpo_qualityControl':np.full(numdays,1),             # The Quality Control data product option - See https://wiki.oceannetworks.ca/display/DP/1
                        'dpo_resample':np.full(numdays,'average'),              # The Resampling data product option - See https://wiki.oceannetworks.ca/display/DP/1
                        'dpo_average':np.full(numdays,60),                   # Resampling average, 60=1 minutes
                        'dpo_dataGaps':np.full(numdays,0),
                        'delivery_error':np.full(numdays,'None'),
                        'data_exists':np.full(numdays,False)} 
     
        ## Convert to dataframe
        i_presearch_parameter_df = pd.DataFrame(i_parameter_dict)
        
        ## Append to the master dataframes list
        dataframes_list.append(i_presearch_parameter_df)
    
## At the end, concatenate dataframes
presearch_parameter_df = pd.concat(dataframes_list,ignore_index=True)
            


## Save to a pre-download file
presearch_parameter_df.to_csv(data_dir + '/metadata/pre_download_metadata.csv')

# %%
## Test download...

## Set up empty arrays for table
delivery_requestInfo = []
delivery_error = []
data_exists = []


parameter_df = presearch_parameter_df.copy(deep=True)

print('Will run for a total of {} records'.format(len(parameter_df)))

## Run for all lines
for i_request in range(len(parameter_df)):

    ## IF it's an increment of a 100th, print it:
    if i_request % 100 == 0:
        print('Running for {}th line'.format(i_request))
        
    ## STEP 1: Run the data product delivery
    ## Get sub dictionary for search, for this index:
    i_search_parameters_full = parameter_df.loc[i_request].to_dict()
    i_search_parameters = { key:value for key,value in i_search_parameters_full.items() if key in search_params_list}

    print('running data product delivery for dateFrom {}'.format(i_search_parameters['dateFrom']))
    i_delivery_requestInfo, i_delivery_error, i_data_exists = odm.data_product_delivery(i_search_parameters,verbose=False)

    ## Append to arrays:
    parameter_df['delivery_error'][i_request] = i_delivery_error
    parameter_df['data_exists'][i_request] = i_data_exists

    print('Sleeping %i seconds before next attempt \n \n' % 1)
    time.sleep(1) 

parameter_df.to_csv(data_dir + '/onlineInstruments_all.csv')

