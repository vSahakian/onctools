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
from datetime import datetime, timedelta
import logging, sys


# %%

## Download directory:
#data_dir = '/Users/vjs/turbidites/observational/data/testADCP'
template_main_dir = '/Users/vjs/turbidites/observational/data/templates'

## Directories to store downloaded data:
main_data_dir = '/Users/vjs/turbidites/observational/data/barkleycanyon/BACAX/adcp'
netcdf_data_dir = '/Users/vjs/turbidites/observational/data/barkleycanyon/BACAX/adcp/netcdf'
png_data_dir = '/Users/vjs/turbidites/observational/data/barkleycanyon/BACAX/adcp/png'


## Logfile path:
logfile_path = main_data_dir + '/LOG_dL_ADCPtest_BACAX_7_30_2024.log'

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
## Set up the search parameter dataframe.

## Loop over the years of interest for templates:
template_years = np.array([2018,2019,2020,2021,2022])

## Make an empty list for datetimes for start, and end of downlaods, and directory to save to
starttimes = []
endtimes =  []

## For each year, find the appropriate template directory and open the csv's:
for i_templateyear in template_years:
    ## Define the csv file with the templates for this year
    i_templatedatabase_path = os.path.join(template_main_dir,'BACAXturbidityntu'+str(i_templateyear),'template_interpretations_BACAX'+str(i_templateyear)+'.csv')
        
    ## Import the templates:
    i_template_database = pd.read_csv(i_templatedatabase_path,skiprows=6,header=0)
    
    ## For each template, set rows for download dataframe with start and end times for days before, during, and after template
    for i_template, i_row in i_template_database.iterrows():
        i_row['starttime'] = pd.to_datetime(i_row['time_min'], format='%Y%m%dT%H%M%S')
        i_row['endtime'] = pd.to_datetime(i_row['time_max'], format='%Y%m%dT%H%M%S')
        
        ## Start downloading in the day that is at least 13 hours before the starttime
        i_dL_starttime_dt = i_row.starttime - timedelta(hours=13)
        
        ## Stop downloading in the day that is at least 13 hours after the endtime (so add a day to the endtime day)
        i_dL_endtime_dt = i_row.starttime + timedelta(hours=13)
        
        ## Because it is +/- 13 hours, the start and endtime will span more than one day. 
        ##   Accommodate this by finding the number of days difference, then adding start and end time for individual days
        i_timediff = i_dL_endtime_dt - i_dL_starttime_dt
        i_daydiff = i_timediff.days
        
        ## For each day in the difference:
        for i_day_to_add in range(i_daydiff+1):
            ## Starttime is original starttime, plus one for each day in time difference
            j_dL_starttime_dt = i_dL_starttime_dt + timedelta(days=i_day_to_add)
            j_dL_starttime = datetime(j_dL_starttime_dt.year,j_dL_starttime_dt.month,j_dL_starttime_dt.day,0,0,0).strftime('%Y-%m-%dT%H:%M:%S.000Z')
            
            ## End time is that j starttime plus 1 day
            j_dL_endtime_dt = j_dL_starttime_dt + timedelta(days=1)
            j_dL_endtime = datetime(j_dL_endtime_dt.year,j_dL_endtime_dt.month,j_dL_endtime_dt.day,0,0,0).strftime('%Y-%m-%dT%H:%M:%S.000Z')
            
            ## Append:
            starttimes.append(j_dL_starttime)
            endtimes.append(j_dL_endtime) 
        

        
## Make the download dataframes with these arrays
## First make one for the netcdf's:
netcdf_presearch_dict =  {'method':np.full_like(starttimes,'request'),
               'token':np.full_like(starttimes,token,dtype='<U64'),    # replace YOUR_TOKEN_HERE with your personal token obtained from the 'Web Services API' tab at https://data.oceannetworks.ca/Profile when logged in.
               'locationCode':np.full_like(starttimes,'BACAX'), # Barkley Canyon / Axis (POD 1)
                'deviceCategoryCode':np.full_like(starttimes,'ADCP2MHZ'),    # 2MHz Acoustic Doppler Current Profiler
                'dataProductCode':np.full_like(starttimes,'NTS'),           # Nortek Time Series (NTS), Nortek Daily Currents Plot (NDCP)
                'extension':np.full_like(starttimes,'nc'),                  # NetCDF, png
                'dateFrom':np.array(starttimes),  # The datetime of the first data point (From Date)
                'dateTo':np.array(endtimes),    # The datetime of the last data point (To Date)
                # 'dpo_qualityControl':np.array([]),             # The Quality Control data product option - See https://wiki.oceannetworks.ca/display/DP/1
                # 'dpo_resample':np.full(numdays,'average'),              # The Resampling data product option - See https://wiki.oceannetworks.ca/display/DP/1
                # 'dpo_average':np.full(numdays,60),                   # Resampling average, 60=1 minutes
                # 'dpo_dataGaps':np.full(numdays,0),
                'delivery_error':np.full_like(starttimes,'None'),
                'data_exists':np.full_like(starttimes,False)} 

## Convert to dataframe
netcdf_presearch_df = pd.DataFrame(netcdf_presearch_dict)

## Add download things...
netcdf_presearch_df['delivery_error'] = np.full(len(netcdf_presearch_df),'None')
netcdf_presearch_df['request_error'] = np.full(len(netcdf_presearch_df),'None')
netcdf_presearch_df['download_error'] = np.full(len(netcdf_presearch_df),'None')
netcdf_presearch_df['request_ok'] = np.full(len(netcdf_presearch_df),False)
netcdf_presearch_df['dL_status_counter'] = np.full(len(netcdf_presearch_df),0)
netcdf_presearch_df['filePath'] = np.full(len(netcdf_presearch_df),'No file')
    
## Save this presearch dataframe to file:
netcdf_presearch_df.to_csv(png_data_dir + '/metadata/pre_download_metadata_netcdf.csv')


##################
## Second make one for png's:
png_presearch_dict =  {'method':np.full_like(starttimes,'request'),
               'token':np.full_like(starttimes,token,dtype='<U64'),    # replace YOUR_TOKEN_HERE with your personal token obtained from the 'Web Services API' tab at https://data.oceannetworks.ca/Profile when logged in.
               'locationCode':np.full_like(starttimes,'BACAX'), # Barkley Canyon / Axis (POD 1)
                'deviceCategoryCode':np.full_like(starttimes,'ADCP2MHZ'),    # 2MHz Acoustic Doppler Current Profiler
                'dataProductCode':np.full_like(starttimes,'NDCP'),           # Nortek Time Series (NTS), Nortek Daily Currents Plot (NDCP)
                'extension':np.full_like(starttimes,'png'),                  # NetCDF, png
                'dateFrom':np.array(starttimes),  # The datetime of the first data point (From Date)
                'dateTo':np.array(endtimes),    # The datetime of the last data point (To Date)
                # 'dpo_qualityControl':np.array([]),             # The Quality Control data product option - See https://wiki.oceannetworks.ca/display/DP/1
                # 'dpo_resample':np.full(numdays,'average'),              # The Resampling data product option - See https://wiki.oceannetworks.ca/display/DP/1
                # 'dpo_average':np.full(numdays,60),                   # Resampling average, 60=1 minutes
                # 'dpo_dataGaps':np.full(numdays,0),
                'delivery_error':np.full_like(starttimes,'None'),
                'data_exists':np.full_like(starttimes,False)} 

## Convert to dataframe
png_presearch_df = pd.DataFrame(png_presearch_dict)

## Add download things...
png_presearch_df['delivery_error'] = np.full(len(png_presearch_df),'None')
png_presearch_df['request_error'] = np.full(len(png_presearch_df),'None')
png_presearch_df['download_error'] = np.full(len(png_presearch_df),'None')
png_presearch_df['request_ok'] = np.full(len(png_presearch_df),False)
png_presearch_df['dL_status_counter'] = np.full(len(png_presearch_df),0)
png_presearch_df['filePath'] = np.full(len(png_presearch_df),'No file')
    
## Save this presearch dataframe to file:
png_presearch_df.to_csv(netcdf_data_dir + '/metadata/pre_download_metadata_png.csv')

#%%   Run the data download for netcdf

# Run for all indices...
search_indices = netcdf_presearch_df.index.values   


## Search, download, and get the post search dataframe:
netcdf_postsearch_df = odm.batch_test_and_download(search_params_list,netcdf_presearch_df,search_indices,token,netcdf_data_dir,download_tries)




#%%   Run the data download for png

# Run for all indices...
search_indices = png_presearch_df.index.values   


## Search, download, and get the post search dataframe:
png_postsearch_df = odm.batch_test_and_download(search_params_list,png_presearch_df,search_indices,token,png_data_dir,download_tries)



# %%  OLD
# ij_presearch_dict =  {'method':np.array(['request','request']),
#                'token':np.array([token, token]),    # replace YOUR_TOKEN_HERE with your personal token obtained from the 'Web Services API' tab at https://data.oceannetworks.ca/Profile when logged in.
#                'locationCode':np.array(['BACAX', 'BACAX']), # Barkley Canyon / Axis (POD 1)
#                 'deviceCategoryCode':np.array(['ADCP2MHZ','ADCP2MHZ']),    # 2MHz Acoustic Doppler Current Profiler
#                 'dataProductCode':np.array(['NTS','NDCP']),           # Nortek Time Series, Nortek Daily Currents Plot
#                 'extension':np.array(['nc','png']),                  # NetCDF, png
#                 'dateFrom':np.array(['2018-01-01T00:00:00.000Z','2018-01-01T00:00:00.000Z']),  # The datetime of the first data point (From Date)
#                 'dateTo':np.array(['2018-01-02T00:00:00.000Z','2018-01-02T00:00:00.000Z']),    # The datetime of the last data point (To Date)
#                 # 'dpo_qualityControl':np.array([]),             # The Quality Control data product option - See https://wiki.oceannetworks.ca/display/DP/1
#                 # 'dpo_resample':np.full(numdays,'average'),              # The Resampling data product option - See https://wiki.oceannetworks.ca/display/DP/1
#                 # 'dpo_average':np.full(numdays,60),                   # Resampling average, 60=1 minutes
#                 # 'dpo_dataGaps':np.full(numdays,0),
#                 'delivery_error':np.array(['None','None']),
#                 'data_exists':np.array([False,False])} 



# d(search_params_list,presearch_parameter_df,search_indices,token,data_dir,download_tries,metadata_path_suffix='v0',sleeptime=10):
    # https://data.oceannetworks.ca/api/dataProductDelivery?method=request&token=YOUR_TOKEN_HERE&locationCode=BACAX&deviceCategoryCode=ADCP2MHZ&dataProductCode=NTS&extension=nc&dateFrom=2018-01-01T00:00:00.000Z&dateTo=2018-01-02T00:00:00.000Z



