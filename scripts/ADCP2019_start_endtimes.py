#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 17:04:11 2023

@author: vjs
"""
## Make a file with the metadata that has the start and stop time of each file

import pandas as pd
from datetime import datetime, timedelta
import os
from glob import glob
import numpy as np


####### PARAMETERS ########

## ADCP directory:
adcp_directory = '/Users/vjs/turbidites/observational/data/BACAX_adcp_2019/netCDF'

## Output metadata file:
adcp_metadata_output_path = '{}/{}.csv'.format(adcp_directory,'metadata_startendtimes')


###########################

# %%

## GEt a glob of all files in directory:
allfiles = np.sort(glob(adcp_directory+'/*.nc'))

## Set out start time and end time arrays, as well as file name array
starttimes = np.array([])
endtimes = np.array([])
file_basepaths = np.array([])

# Loop through them:
for i_file in allfiles:
    ## Get base file:
    i_base_path = i_file.split('/')[-1]
    ## Add to array:
    file_basepaths = np.append(file_basepaths,i_base_path)
    
    ## GEt the start time - it is the second to last, endtime-binMap.nc is last:
    i_starttime_string = i_base_path.split('_')[-2]
    ## Convert to datetime:
    i_starttime = datetime.strptime(i_starttime_string,'%Y%m%dT%H%M%SZ')
    ## Append to array:
    starttimes = np.append(starttimes,i_starttime)
    
    
    ## And end time, the thing right before -binMap.nc:
    i_endtime_string = i_base_path.split('_')[-1].split('-')[0]
    ## Convert to datetime:
    i_endtime = datetime.strptime(i_endtime_string,'%Y%m%dT%H%M%SZ')
    ## Append to array:
    endtimes = np.append(endtimes,i_endtime)
    
## Make a dict then dataframe with all of them:
adcp_times_dict = {'baseFilePath':file_basepaths, 'starttimes':starttimes, 'endtimes':endtimes}
adcp_times = pd.DataFrame(adcp_times_dict)    

## Save to file:
adcp_times.to_csv(adcp_metadata_output_path,index=False)
