#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  3 16:02:17 2023

@author: vjs
"""

## Code to download turbidity data in Barkley canyon for finite chunk
import numpy as np
import pandas as pd



# %% Paths
#online_locations_path =  '/Users/vjs/turbidites/observational/data/getOnlineInstruments_jul15/onlineInstruments_all.csv'
online_locations_path =  '/Users/vjs/turbidites/observational/data/datadl4fourmile/onlineInstruments_all.csv'


## Data gaps path:
datagaps_path = '/Users/vjs/turbidites/observational/data/datadl4fourmile/gap_start_end_times_april10_2023.txt'

## Download directory:
# data_dir = '/home/dkilb/barkley/data/'

## Logfile path:
# logfile_path = '/home/dkilb/barkley/data/LOG_dLmultisensors_Bark_8_4_2022.log'

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


## Search parmaeters needed:
search_params_list = ['method','token','locationCode','propertyCode','dataProductCode','extension','dateFrom','dateTo','dpo_qualityControl','dpo_resample','dpo_average','dpo_dataGaps']

## Output new metdata download dataframe:
output_onlineInstruments_path = '/Users/vjs/turbidites/observational/data/datadl4fourmile/onlineInstruments_datagaps_april10_2023.csv'


# %%

## Import the large metadata downlod file
online_instruments = pd.read_csv(online_locations_path)

##and also the data gaps file
data_gaps = pd.read_csv(datagaps_path)


#### Modify online instruments
## Get rid of the unnamed clumns:
if 'Unnamed: 0' in list(online_instruments):
    online_instruments = online_instruments.drop(labels=['Unnamed: 0'],axis=1)
    
## Add columns with datetime objects:
online_instruments['dateFromdt'] = online_instruments['dateFrom'].astype('datetime64')
online_instruments['dateTodt'] = online_instruments['dateTo'].astype('datetime64')



##### Modify data gaps:
data_gaps['gap_start'] = data_gaps['gap_start'].astype('datetime64')
data_gaps['gap_end'] = data_gaps['gap_end'].astype('datetime64')

## rounded:
data_gaps['gap_start_rounddown'] = data_gaps['gap_start'].dt.floor("D")
data_gaps['gap_end_roundup'] = data_gaps['gap_end'].dt.ceil('D')


####################
## First, trim to be only BACAX:
online_instruments_BACAX = online_instruments.loc[online_instruments['locationCode'] == 'BACAX']


## Then loop through the data gap times dataframe and get the indices for the date ranges above:
date_range_array = np.array([])
for i_gap_index in data_gaps.index:
    
    ## GEt the dates here:
    i_start = data_gaps['gap_start_rounddown'].loc[i_gap_index]
    i_end = data_gaps['gap_end_roundup'].loc[i_gap_index]
    
    ## Search for the indices in the online instruments metadata df in these dates
    i_searchindices = np.where(((online_instruments_BACAX['dateFromdt'] >= i_start) & (online_instruments_BACAX['dateTodt'] <= i_end)))[0]
    
    ## Append to big list:
    date_range_array = np.r_[date_range_array,i_searchindices]

## Get unique indices to search for:
date_range_array_unique = np.unique(date_range_array)

## Then, get the subseted metadata array for download:
online_instruments_new = online_instruments_BACAX.loc[date_range_array_unique]
online_instruments_new = online_instruments_new.drop(labels=['dateFromdt','dateTodt'],axis=1)

## SAve:
online_instruments_new.to_csv(output_onlineInstruments_path)
