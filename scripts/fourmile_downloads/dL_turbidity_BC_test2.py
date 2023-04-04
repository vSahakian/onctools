#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  7 16:12:07 2022

@author: vjs
"""

## Code to download turbidity data in Barkley canyon for finite chunk
import numpy as np
from onc.onc import ONC
#root = tk.Tk()  
import pandas as pd

from datetime import datetime, timedelta

import onc_download_module as odm

# %% Paths
barkley_codes_path = '/Users/vjs/turbidites/observational/data/onc_codes/locationcodes_barkley.csv'
clayoquot_codes_path = '/Users/vjs/turbidites/observational/data/onc_codes/locationcodes_clayoquot.csv'


turbidity_codes_path = '/Users/vjs/turbidites/observational/data/onc_codes/propertycodes_turbidity.csv'


## Test download path:
data_dir = '/Users/vjs/turbidites/observational/data/sleeptest'

## codes etc. to search for
#$propertyCodes = ['turbidityftu','turbidityntu','seawatertemperature','oxygen','pressure','chlorophyll']
#locationCodes = ['BACAX','BACCH','BACHY','BACME','BACMW','BACND','BACUS']

## KEY:
    # BACAX:  Barkley Canyon Axis
    # BACCH:  Barkley Canyon Head
    # BACHY:  Barkley CAnyon Hydrates
    # BACME:  Barkley Canyon MidEast
    # BACMW:  Barkley Canyon MidWest
    # BACND:  Barkley Canyon Node
    # BACUS:  Barkley Canyon Upper Slope South

propertyCode = 'turbidityntu'
locationCode = 'BACAX'

## Number of download tries:
download_tries = 40

## sleep time in seconds, in between bad 202 responses:
sleeptime = 10

## DAtetimes to run:
start_download = datetime(2019,1,1)
end_download = datetime(2019,12,31)


# %%  Get Token %% #
token = odm.Token()
onc = ONC(token)
 

## Make download array:
start_dates_array = np.arange(start_download,end_download,timedelta(days=1))
end_dates_array = np.arange(start_download+(timedelta(days=1)),end_download+(timedelta(days=1)),timedelta(days=1))

## Parameter dictionariy:
search_parameters = {'method':'request',
            'token':token,          # replace YOUR_TOKEN_HERE with your personal token obtained from the 'Web Services API' tab at https://data.oceannetworks.ca/Profile when logged in.
            'locationCode':'BACAX',             # Barkley Canyon / Axis (POD 1)
            'propertyCode':'turbidityntu',    # 150 kHz Acoustic Doppler Current Profiler
            'dataProductCode':'TSSD',           # Time Series Scalar Data
            'extension':'csv',                  # Comma Separated spreadsheet file
            'dateFrom':'2019-12-24T00:00:00.000Z',  # The datetime of the first data point (From Date)
            'dateTo':'2019-12-25T00:00:00.000Z',    # The datetime of the last data point (To Date)
            'dpo_qualityControl':1,             # The Quality Control data product option - See https://wiki.oceannetworks.ca/display/DP/1
            'dpo_resample':'average',              # The Resampling data product option - See https://wiki.oceannetworks.ca/display/DP/1
            'dpo_average':60,                   # Resampling average, 60=1 minutes
            'dpo_dataGaps':0}                   # The Data Gaps data product option - See https://wiki.oceannetworks.ca/display/DP/1

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




## Create dataframe and save
parameter_dict = {'method':np.full(numdays,'request'),
               'token':np.full(numdays,token),    # replace YOUR_TOKEN_HERE with your personal token obtained from the 'Web Services API' tab at https://data.oceannetworks.ca/Profile when logged in.
               'locationCode':np.full(numdays,locationCode),             # Barkley Canyon / Axis (POD 1)
                'propertyCode':np.full(numdays,propertyCode),    # 150 kHz Acoustic Doppler Current Profiler
                'dataProductCode':np.full(numdays,'TSSD'),           # Time Series Scalar Data
                'extension':np.full(numdays,'csv'),                  # Comma Separated spreadsheet file
                'dateFrom':string_start_array,  # The datetime of the first data point (From Date)
                'dateTo':string_end_array,    # The datetime of the last data point (To Date)
                'dpo_qualityControl':np.full(numdays,1),             # The Quality Control data product option - See https://wiki.oceannetworks.ca/display/DP/1
                'dpo_resample':np.full(numdays,'average'),              # The Resampling data product option - See https://wiki.oceannetworks.ca/display/DP/1
                'dpo_average':np.full(numdays,60),                   # Resampling average, 60=1 minutes
                'dpo_dataGaps':np.full(numdays,0),
                'delivery_error':np.full(numdays,'None'),
                'request_error':np.full(numdays,'None'),
                'download_error':np.full(numdays,'None'),
                'data_exists':np.full(numdays,False),    
                'request_ok':np.full(numdays,False),
                'dL_status_counter':np.full(numdays,0),
                'filePath':np.full(numdays,'No file')} 
 
## Convert to dataframe
presearch_parameter_df = pd.DataFrame(parameter_dict)

## Save to a pre-download file
presearch_parameter_df.to_csv(data_dir + '/metadata/pre_download_metadata.csv')


# %%
## Download, v0. Attempt all.

## Search parmaeters needed:
search_params_list = ['method','token','locationCode','propertyCode','dataProductCode','extension','dateFrom','dateTo','dpo_qualityControl','dpo_resample','dpo_average','dpo_dataGaps']
search_indices_v0 = np.arange(numdays)

## Searcha nd download...
print('RUNNING SEARCH V0 ')
postsearch_v0_parameter_df = odm.batch_test_and_download(search_params_list,presearch_parameter_df,search_indices_v0,token,data_dir,download_tries)


# %% SECOND TIME TO RUN: 
## Run where data exist, and where there is no file.
search_indices_v1 = np.where((postsearch_v0_parameter_df['data_exists'] == True) & (postsearch_v0_parameter_df['filePath'] == 'No file'))[0]
metadata_path_suffix = 'v1'

## Search:
postsearch_v1_parameter_df = odm.batch_test_and_download(search_params_list,postsearch_v0_parameter_df,search_indices_v1,token,data_dir,download_tries,metadata_path_suffix)


    
# %% THIRD TIME TO RUN: 
## Run where data exist, and where there is no file.
search_indices_v2 = np.where((postsearch_v1_parameter_df['data_exists'] == True) & (postsearch_v1_parameter_df['filePath'] == 'No file'))[0]
metadata_path_suffix = 'v2'

## Search:
postsearch_v2_parameter_df = odm.batch_test_and_download(search_params_list,postsearch_v1_parameter_df,search_indices_v2,token,data_dir,download_tries,metadata_path_suffix)


# %% FOURTH TIME TO RUN: 
## Run where data exist, and where there is no file.
search_indices_v3 = np.where((postsearch_v2_parameter_df['data_exists'] == True) & (postsearch_v2_parameter_df['filePath'] == 'No file'))[0]
metadata_path_suffix = 'v3'

## Search:
print('Will run for %i records' % len(search_indices_v3))
postsearch_v3_parameter_df = odm.batch_test_and_download(search_params_list,postsearch_v2_parameter_df,search_indices_v3,token,data_dir,download_tries,metadata_path_suffix)


# %% FIFTH TIME TO RUN: 
## Run where data exist, and where there is no file.
search_indices_v4 = np.where((postsearch_v3_parameter_df['data_exists'] == True) & (postsearch_v3_parameter_df['filePath'] == 'No file'))[0]
metadata_path_suffix = 'v4'

## Search:
print('Will run for %i records' % len(search_indices_v4))
postsearch_v4_parameter_df = odm.batch_test_and_download(search_params_list,postsearch_v3_parameter_df,search_indices_v4,token,data_dir,download_tries,metadata_path_suffix)



# %% SIXTH TIME TO RUN: 
## Run where data exist, and where there is no file.
search_indices_v5 = np.where((postsearch_v4_parameter_df['data_exists'] == True) & (postsearch_v4_parameter_df['filePath'] == 'No file'))[0]
metadata_path_suffix = 'v4'

## Search:
print('Will run for %i records' % len(search_indices_v5))
postsearch_v5_parameter_df = odm.batch_test_and_download(search_params_list,postsearch_v4_parameter_df,search_indices_v5,token,data_dir,download_tries,metadata_path_suffix)



# %% SEVENTH TIME TO RUN: 
## Run where data exist, and where there is no file.
search_indices_v6 = np.where((postsearch_v5_parameter_df['data_exists'] == True) & (postsearch_v5_parameter_df['filePath'] == 'No file'))[0]
metadata_path_suffix = 'v6'

## Search:
print('Will run for %i records' % len(search_indices_v6))
postsearch_v6_parameter_df = odm.batch_test_and_download(search_params_list,postsearch_v5_parameter_df,search_indices_v6,token,data_dir,download_tries,metadata_path_suffix)


# %% SEVENTH TIME TO RUN: 
## Run where data exist, and where there is no file.
search_indices_v7 = np.where((postsearch_v6_parameter_df['data_exists'] == True) & (postsearch_v6_parameter_df['filePath'] == 'No file'))[0]
metadata_path_suffix = 'v6'

## Search:
print('Will run for %i records' % len(search_indices_v7))
postsearch_v7_parameter_df = odm.batch_test_and_download(search_params_list,postsearch_v6_parameter_df,search_indices_v7,token,data_dir,download_tries,metadata_path_suffix)



# %% EIGTH TIME TO RUN: 
## Run where data exist, and where there is no file.
search_indices_v8 = np.where((postsearch_v7_parameter_df['data_exists'] == True) & (postsearch_v7_parameter_df['filePath'] == 'No file'))[0]
metadata_path_suffix = 'v7'

## Search:
print('Will run for %i records' % len(search_indices_v8))
postsearch_v8_parameter_df = odm.batch_test_and_download(search_params_list,postsearch_v7_parameter_df,search_indices_v8,token,data_dir,download_tries,metadata_path_suffix)


    
    

