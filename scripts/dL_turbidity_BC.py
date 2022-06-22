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

import onc_download_module as odm

# %% Paths
barkley_codes_path = '/Users/vjs/turbidites/observational/data/onc_codes/locationcodes_barkley.csv'
clayoquot_codes_path = '/Users/vjs/turbidites/observational/data/onc_codes/locationcodes_clayoquot.csv'


turbidity_codes_path = '/Users/vjs/turbidites/observational/data/onc_codes/propertycodes_turbidity.csv'


## Test download path:
data_dir = '/Users/vjs/turbidites/observational/data/december2019_BACAX_ntu'

## codes etc. to search for
propertyCodes = ['turbidityftu','turbidityntu','seawatertemperature','oxygen','pressure','chlorophyll']
locationCodes = ['BACAX']

## Number of download tries:
download_tries = 20

## DAtetimes to run:
start_download = datetime(2019,12,1)
end_download = datetime(2019,12,31)


# %%  Get Token %% #
token = odm.Token()
onc = ONC(token)
 

## Make download array:
dates_array = np.arange(start_download,end_download,timedelta(days=1))

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
## Test download...

## Set up empty arrays for table
delivery_requestInfo = []
product_request = []


delivery_error = []
request_error = []
download_error = []


data_exists = []
request_ok = []
dL_status_counter = []
filePath = []


for i_day in range(len(dates_array)-1):
    
    ## Modify the date on the parameters:
    search_parameters['dateFrom'] = dates_array[i_day].astype(datetime).strftime('%Y-%m-%dT%H:%M:%S') + '.000Z'
    search_parameters['dateTo'] = dates_array[i_day+1].astype(datetime).strftime('%Y-%m-%dT%H:%M:%S') + '.000Z'
    
    print('\n \n ##### running from %s to %s' % (search_parameters['dateFrom'],search_parameters['dateTo']))


    ## STEP 1: Run the data product delivery
    print('running data product delivery')
    i_delivery_requestInfo, i_delivery_error, i_data_exists = odm.data_product_delivery(search_parameters)
    
    ## Append to arrays:
    delivery_requestInfo.append(i_delivery_requestInfo)
    delivery_error.append(i_delivery_error)
    data_exists.append(i_data_exists)
    
    ## If there is no data, continue on to the next part of hte loop:
    if i_data_exists == False:
        continue
    
    ## STEP 2: Make a dictionary to run a data product request
    i_request_parameter_dictionary = odm.make_request_dictionary(token,i_delivery_requestInfo)
    
    ## Run the data product request
    print('running data product request')
    i_product_request, i_request_error, i_request_ok = odm.data_product_request(i_request_parameter_dictionary)
    
    ## Append to arrays:
    product_request.append(i_product_request)
    request_error.append(i_request_error)
    request_ok.append(i_request_ok)
    
    ## STEP 3: Make a dictionary to run the download
    i_request_index = 0
    i_download_parameter_dictionary = odm.make_download_dictionary(token,i_product_request,i_request_index)
    
    ## Run the download...
    print('attempting download...')
    i_download_error, i_status_counter,i_filePath = odm.download_data(i_download_parameter_dictionary,data_dir,download_tries)

    ## Append to arrays:
    download_error.append(i_download_error)
    dL_status_counter.append(i_status_counter)
    filePath.append(i_filePath)
    
## Make data exists an array:
data_exists = np.array(data_exists)

## Create dataframe and save
status_dict = {'method':np.full((len(dates_array)-1),'request'),
               'token':np.full((len(dates_array)-1),token),    # replace YOUR_TOKEN_HERE with your personal token obtained from the 'Web Services API' tab at https://data.oceannetworks.ca/Profile when logged in.
               'locationCode':np.full((len(dates_array)-1),'BACAX'),             # Barkley Canyon / Axis (POD 1)
                'propertyCode':np.full((len(dates_array)-1),'turbidityntu'),    # 150 kHz Acoustic Doppler Current Profiler
                'dataProductCode':np.full((len(dates_array)-1),'TSSD'),           # Time Series Scalar Data
                'extension':np.full((len(dates_array)-1),'csv'),                  # Comma Separated spreadsheet file
                'dateFrom':dates_array[0:-1],  # The datetime of the first data point (From Date)
                'dateTo':dates_array[1:],    # The datetime of the last data point (To Date)
                'dpo_qualityControl':np.full((len(dates_array)-1),1),             # The Quality Control data product option - See https://wiki.oceannetworks.ca/display/DP/1
                'dpo_resample':np.full((len(dates_array)-1),'average'),              # The Resampling data product option - See https://wiki.oceannetworks.ca/display/DP/1
                'dpo_average':np.full((len(dates_array)-1),60),                   # Resampling average, 60=1 minutes
                'dpo_dataGaps':np.full((len(dates_array)-1),0),
                'delivery_error':delivery_error,
                'request_error':request_error,
                'download_error':download_error,
                'data_exists':data_exists,
                'request_ok':request_ok,
                'dL_status_counter':dL_status_counter,
                'filePath':filePath} 

status_df = pd.DataFrame(status_dict)
status_df.to_csv(data_dir + '/download_metadata.csv')
