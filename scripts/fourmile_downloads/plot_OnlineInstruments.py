#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 15 14:05:46 2022

@author: vjs
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
from datetime import datetime, timedelta

### 
# %% Parameters

data_dir = '/Users/vjs/turbidites/observational/data/getOnlineInstruments_jul15'
#data_dir = '/Users/vjs/turbidites/observational/data/temp_onlineInstruments_june23'


filepath = data_dir + '/onlineInstruments_all.csv'

## Figure size?
figure_size = (20,8)

# %% 
## Online Locations Plot

## Read in file:
parameter_df = pd.read_csv(filepath)

## Get the unique locations:
locationCodes = parameter_df.locationCode.unique()


## for now just one loaction code:

location_df = parameter_df.loc[parameter_df.locationCode == locationCodes[0]].reset_index(drop=True)

## Loop over locations and make a plot for each:
for i_location in range(len(locationCodes)):
    
    ## Get location dataframe:
    i_location_df = parameter_df.loc[parameter_df.locationCode == locationCodes[i_location]].reset_index(drop=True)

    ## Make a plot:
    i_dataFigure = plt.figure(figsize=figure_size)
    
    ## Get the dates as a string:
    i_dates = i_location_df.dateFrom.unique()
    i_dates_datetimes = i_dates.astype(np.datetime64)
    
    ## Reformat to datetime with just day:
    i_dates_ym = []
    i_dates_ymd = []
    
    ## Make a list for the indices to label:
    i_xlabel_indices = []    
    ## And a list for those labels:
    i_xlabel_labels = []
    
    
    
    for i_day in range(len(i_dates)):
        i_day_reformat_ym = i_dates_datetimes[i_day].astype(datetime).strftime('%Y-%m')
        i_dates_ym.append(i_day_reformat_ym)
        
        i_day_reformat_ymd = i_dates_datetimes[i_day].astype(datetime).strftime('%Y-%m-%d')
        i_dates_ymd.append(i_day_reformat_ymd)
        
        ## If it is the first of a month, add it to the label lists:
        if i_dates_datetimes[i_day].astype(datetime).day == 1:
            i_xlabel_indices.append(i_day)
            i_xlabel_labels.append(i_day_reformat_ym)
        
    i_dates_ym = np.array(i_dates_ym)
    i_dates_ymd = np.array(i_dates_ymd)
    
    ## Make an array for plotting...
    i_dates_array = np.arange(len(i_dates_ymd))
    
    ## y values are based on the property codes in the dataframe
    i_unique_properties = i_location_df.propertyCode.unique()
    i_unique_prop_array = np.arange(len(i_unique_properties))
    
    ## Make X and Y arrays for pcolormesh:
    i_DATES,i_PROPERTY = np.meshgrid(i_dates_array,i_unique_prop_array)
    
    ## MAKE Z array:
    i_DATA_EXISTS = np.zeros_like(i_DATES)
    
    for j_property_index in range(len(i_unique_properties)):
        for k_date_index in range(len(i_dates_array)):
            ## Property?
            j_property = i_unique_properties[j_property_index]      
            
            ## Date? From string, since that's what is in dataframe
            k_date = i_dates[k_date_index]
    
            ## Does data exist?
            jk_data_exists = i_location_df.loc[((i_location_df.propertyCode == j_property) & (i_location_df.dateFrom == k_date))]['data_exists'].values[0]
            if  jk_data_exists == True:
                ## Then change the big array to be 1, i.e., plot color for this date and property:
                    i_DATA_EXISTS[j_property_index,k_date_index] = 1
    
    
    ## Plot:
    plt.pcolormesh(i_DATES,i_PROPERTY,i_DATA_EXISTS,shading='nearest',cmap='binary')

    ## Set the y limits:
    plt.gca().set_ylim([min(i_unique_prop_array)-0.5,max(i_unique_prop_array)+0.5])
    
    
    ## Set x tick labels and y tick labels:
    plt.gca().set_xticks(i_xlabel_indices,minor=False)
    plt.gca().set_xticklabels(i_xlabel_labels,fontdict={'rotation':60})
    plt.gca().set_yticks(i_unique_prop_array,minor=False)    
    plt.gca().set_yticklabels(i_unique_properties,fontdict={'rotation':60})

    plt.gca().set_yticks(i_unique_prop_array - 0.5, minor=True)
    plt.gca().yaxis.grid(False, which='major')
    plt.gca().yaxis.grid(True, which='minor')
    
    plt.gca().xaxis.grid(True,which='major')
    
    ## Add colorbar:
    i_cb = plt.colorbar(ticks=[0, 1])
    i_cb.set_label('Data exists?')
    i_cb.ax.set_yticklabels(['No Data','Data'])
    
    plt.title('Data Availability at {}'.format(locationCodes[i_location]))
    
    
    ## Save figure:
    plt.savefig(data_dir + '/figs/pdf/onlineIstruments_' + locationCodes[i_location] + '.pdf')
    plt.savefig(data_dir + '/figs/onlineIstruments_' + locationCodes[i_location] + '.png')

        
        
        