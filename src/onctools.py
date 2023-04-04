#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 27 15:34:00 2023

@author: vjs
"""


'''
Functions to use with ONC data, like plotting
'''

def extract_start_turbiditytemplate(turbiditytemplate_path):
    '''
    Get the start and end time from a path of turbidity data
    Input:
        turbiditytemplate_path:      String with the template path, in miniseed
    '''
    import numpy as np
    import os
    
    ## Get the file name:
    template_name = turbiditytemplate_path.split('/')[-1].split('.mseed')
    
    
    turbidity_path = '/Users/vjs/turbidites/observational/data/templates/BACAXturbidityntu2019/csv2mseed/reformated_BACAX_ntu_2019_template_notfiltered_2_15_1.1254_1_0_0.5_20191224T035330_signal_only.mseed'

    



def plot_adcp_timeseries(adcp_nc,turbidity_st,plot_starttime,plot_endtime,colormap='Spectral_r',adcp_figsize=(16,12)):
    '''
    Plot turbidity and ADCP data together in 4 panel plot
    Input:
        adcp_nc:            NetCDF with ADCP data
        turbidity_st:       ObsPy stream object with turbidity template
        plot_starttime:     datetime object with start time for plot
        plot_endtime:       datetime object with end time for plot
        colormap:           String with colormap for ADCP data. Default: 'Spectral_r'
        adcp_figsize:       Tuple with (width,height) of plot. Default: (16,12)
    Output:
        adcp_figure:        Pyplot figure with ADCP and turbidity data
    '''
    
    import numpy as np
    import matplotlib.pyplot as plt
    from datetime import datetime, timedelta
    import matplotlib.dates as mdates
    from matplotlib.dates import DateFormatter
    import seaborn as sns
    sns.set()

    
    #############
    ## parameters and paths

    # adcp_netcdfpath = '/Users/vjs/turbidites/observational/data/BACAX_adcp_2019/netCDF/BarkleyCanyon_BarkleyCanyonAxis_AcousticDopplerCurrentProfiler2MHz_20191224T000005Z_20191224T235955Z-binMapNone.nc'
    # turbidity_path = '/Users/vjs/turbidites/observational/data/templates/BACAXturbidityntu2019/csv2mseed/reformated_BACAX_ntu_2019_template_notfiltered_2_15_1.1254_1_0_0.5_20191224T035330_signal_only.mseed'


    # ## Output figure path:
    # adcp_path_pdf = '/Users/vjs/turbidites/observational/figs/pdf/adcpDec2019.pdf'
    # adcp_path_png = '/Users/vjs/turbidites/observational/figs/png/adcpDec2019.png'



    # ## Start and end time to plot, as datetime objects:
    # plot_starttime = datetime(2019,12,24,2,50,0)
    # plot_endtime = datetime(2019,12,24,16,50,0)
    
    
    
    # %% TO FUNCTIONIZE:
    ## Input:
    ## adcp netcdf
    ## turbidity stream
    ## starttme to plot as datetime object
    ## endttme to plot as datetime object
    
    
    ## Needed for plot:
    seconds_in_day = 24*60*60 # 24 hours, 60 minutes, 60 seconds

    ## Get start time of netcdf file:
    adcp_stime = datetime.strptime(adcp_nc.time_coverage_start,'%Y%m%dT%H%M%SZ')
    #etime = UTCDateTime(adcp.time_coverage_end)


    ## Get values to plot:
        
    ## Get turbidity times and data to plot:
    turb_times_full = []
    for i_time in turbidity_st[0].times('UTCDateTime'):
        turb_times_full.append(i_time.datetime)    
    ## And get where they're between the start and end time specified:    
    turb_times_full = np.array(turb_times_full)
    turb_range_indices = np.where((turb_times_full > plot_starttime) & (turb_times_full < plot_endtime))[0]

    ## GEt time and data to plot:
    turb_times = turb_times_full[turb_range_indices]
    turb_data = turbidity_st[0].data[turb_range_indices]
        

    ## Get time in seconds from the start time:
    seconds_from_start = adcp_nc['time'][:].data*seconds_in_day - adcp_nc['time'][:].data[0]*seconds_in_day
    ## Make an array of UTC datetime objects that are the start time:
    adcp_time_full = []
    for i in range(len(seconds_from_start)):
        adcp_time_full.append(adcp_stime + timedelta(seconds=seconds_from_start[i]))
    ## Convert to array:
    adcp_time_full = np.array(adcp_time_full).astype(datetime)    


    ###### GET DATA WITHIN TIMEFRAME TO PLOT ######
    ## Get locations of time array in netcdf to plot:
    time_range_indices = np.where((adcp_time_full > plot_starttime) & (adcp_time_full < plot_endtime))[0]

    ## Get tme range for ADCP data to plot:
    adcp_time = adcp_time_full[time_range_indices]



    ## Get other values:
    east = adcp_nc['u'][:][time_range_indices]
    north = adcp_nc['v'][:][time_range_indices]
    up = adcp_nc['w'][:][time_range_indices]
    bscat = adcp_nc['meanBackscatter'][:][time_range_indices]
    depth = adcp_nc['depth'][:]



    ## Plot order:
    plotorder = [east,north,up,bscat]
    labelorder = ['East (m/s)','North (m/s)','Up (m/s)','Backscatter (dB)']

    ## Make a meshgrid:
    TIME,DEPTH = np.meshgrid(adcp_time,depth.data)

    ## Set up axes to plot:
    adcp_figure, axes = plt.subplots(nrows=4,ncols=1,figsize=adcp_figsize)
        
    ## For ach axes, plot:
    for i_property_ind, i_property in enumerate(plotorder):
        i_axes = axes[i_property_ind]
        i_pmesh = i_axes.pcolormesh(TIME,DEPTH,i_property.data.T,cmap=colormap)
        i_axes.invert_yaxis()
        
        ## Add colorbar:
        i_cb = plt.colorbar(i_pmesh,ax=i_axes)
        ## Set colorbar label:
        i_cb.set_label(labelorder[i_property_ind],size=16)
        ## colorbar tick param font size: 
        i_cb.ax.tick_params(labelsize=14)
        
        ## Format x axis labels:
        i_axes.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d,%H:%M'))
        i_axes.xaxis.set_tick_params(labelsize=14,rotation=0)
        
        ## Set y-label:
        i_axes.yaxis.set_tick_params(labelsize=14)
        i_axes.set_ylabel('Water Depth (m)',fontsize=16)
        
        ## At the end, for the last one (backscatter), add turbidity...
        if i_property_ind == len(plotorder)-1:
            turbax = i_axes.twinx()
            turbax.plot(turb_times,turb_data,color='black',linewidth=1,label='Turbidity (NTU)')

            ## also add x label:
            i_axes.set_xlabel('UTC Date Time', fontsize=16)
            #i_axis.set_xlabel('Month/Day/Hour in 2019',fontsize=16)
           
    return adcp_figure  
           
    ####### MOVE TO OUTSIDE FUNCTION!!!!!!
    # ## Save figure
    # figure.savefig(adcp_path_pdf,transparent=True)
    # figure.savefig(adcp_path_png,transparent=True)