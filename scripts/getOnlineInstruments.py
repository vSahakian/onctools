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
## Test download path:
data_dir = '/Users/vjs/turbidites/observational/data/temp'

## codes etc. to search for
propertyCodes = ['turbidityftu','turbidityntu','seawatertemperature','oxygen','pressure','chlorophyll']
locationCodes = ['BACAX']


## DAtetimes to run: - NOTE: Cannot mess with microseconds!! Only seconds!
start_download = datetime(2019,12,16)
end_download = datetime(2019,12,31)


# %%  Get Token %% #
token = odm.Token()
onc = ONC(token)
 

## Make download array for dates to test:
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
delivery_error = []
data_exists = []



for i_day in range(len(dates_array)-1):
    
    ## Modify the date on the parameters:
    search_parameters['dateFrom'] = dates_array[i_day].astype(datetime).strftime('%Y-%m-%dT%H:%M:%S') + '.000Z'
    search_parameters['dateTo'] = dates_array[i_day+1].astype(datetime).strftime('%Y-%m-%dT%H:%M:%S') + '.000Z'
    
    print('running from %s to %s' % (search_parameters['dateFrom'],search_parameters['dateTo']))


    ## STEP 1: Run the data product delivery
    print('running data product delivery')
    i_delivery_requestInfo, i_delivery_error, i_data_exists = odm.data_product_delivery(search_parameters)
    
    ## Append to arrays:
    delivery_requestInfo.append(i_delivery_requestInfo)
    delivery_error.append(i_delivery_error)
    data_exists.append(i_data_exists)
    
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
                'data_exists':data_exists} 

status_df = pd.DataFrame(status_dict)
status_df.to_csv(data_dir + '/onlineInstruments_metadata.csv')
