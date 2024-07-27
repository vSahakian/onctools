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

    

# %% ##########
def get_template_files4plot(plot_start,plot_end,metadata_df):
    '''
    Given a start and end time for the plot, find the base path of the respective
    property files to plot (i.e., given a start and end time of the plot, find 
    the basepaths of the files that contain the Chlorophyll values to plot)
    
    Input:
        plot_start:         DAtetime object with the starttime of the plot
        plot_end:           Datetime object with the end time of hte plot
        metadata_df:        pandas dataframe with the metadata for the property of interest
    Output:
        files_list:         List with file(s) path(s)
    '''
    
    import numpy as np
    import os
    
    ################
    ## Get the files of the other data to plot, for the time of the event...
    ## Empty list for the files, if there are more than one, for each property to plot:
    files_list = []

    
    print('finding start and end time for chlorophyll files')
    ## Find where in the starttime this snippet is (take the closest/last to the start time):
    property_startfile_index = np.where((metadata_df.dateFromDt <= plot_start) & (metadata_df.dateToDt >= plot_start))[0]
    property_endfile_index = np.where((metadata_df.dateFromDt <= plot_end) & (metadata_df.dateToDt >= plot_end))[0]

    
    ## If they are the same, append only one; otherwise, append both
    if property_startfile_index == property_endfile_index:
        ## Get the full path name:
        basepath = os.path.basename(metadata_df.filePath.loc[property_startfile_index].values[0])
        files_list.append(basepath)
    else:
        basepath_start = os.path.basename(metadata_df.filePath.loc[property_startfile_index].values[0])
        basepath_end = os.path.basename(metadata_df.filePath.loc[property_endfile_index].values[0])

        ## append
        files_list.append(basepath_start)
        files_list.append(basepath_end)
        
    return files_list

# %% ## Open csv's and merge if necessary
def open_merge_csv4multiplot(files_list,propertyname):
    '''
    Given a list of file paths for ONC time series data (i.e., chlorophyl, etc.), open and merge if necessary.
    Primarily needed/used for thet multiplot script.
    
    Input:
        files_list:             List with paths to csv files
        propertyname:           String with the name of the property in the f ile, as written in file header.
                                    Either: seawatertemperature, turbidityntu, chlorophyll, or oxygen
    Output:
        merged_file:            Dataframe with the merged file
    '''
    import pandas as pd
    
    ##### Opening files...
    print('opening/merging files')
        
    ## For temperature:    
    data_df = pd.DataFrame(columns=['datetime_string',propertyname,'QC','turbcount'])
    for i_path in files_list:
        if 'No file' not in i_path:
            i_data = pd.read_csv(i_path,names=['datetime_string',propertyname,'QC','turbcount'],comment='#')
            ## append:
            data_df = pd.concat([data_df,i_data],ignore_index=True)
    ## Get datetime not as a string:
    data_df['datetime'] = pd.to_datetime(data_df['datetime_string'].astype('datetime64[ms]'))
    
    return data_df


# %% ########
def plot_adcp_timeseries(adcp_nc,turbidity_st,plot_starttime,plot_endtime,event_starttime=False,event_plus12=False,event_plus12p4=False,colormap='Spectral_r',adcp_figsize=(16,12)):
    '''
    Plot turbidity and ADCP data together in 4 panel plot
    Input:
        adcp_nc:            NetCDF with ADCP data
        turbidity_st:       ObsPy stream object with turbidity template
        plot_starttime:     datetime object with start time for plot
        plot_endtime:       datetime object with end time for plot
        event_starttime:    datetime object with the start time of event for vertical line
        event_plus12:       Boolean of whether to plot the event plus 12 hours for vertical line for tides. Default: False
        event_plus12p4:     Boolean of whether to plot the event plus 12 hours for vertical line for tides. Default: False
        colormap:           String with colormap for ADCP data. Default: 'Spectral_r'
        adcp_figsize:       Tuple with (width,height) of plot. Default: (16,12)
    Output:
        adcp_figure:        Pyplot figure with ADCP and turbidity data
    '''
    
    import numpy as np
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    from datetime import datetime, timedelta
    import matplotlib.dates as mdates
    from matplotlib.dates import DateFormatter
    import cmocean
    import matplotlib.colors as colors
    
    ## Needed for plot:
    seconds_in_day = 24*60*60 # 24 hours, 60 minutes, 60 seconds

    ## Get start time of netcdf file:
    adcp_stime = datetime.strptime(adcp_nc.time_coverage_start,'%Y%m%dT%H%M%SZ')
    #etime = UTCDateTime(adcp.time_coverage_end)

    ## List for legend:
    total_legend = ([Line2D([0], [0], color='black', lw=2,linestyle='dashed',label='Event'),
                     Line2D([0], [0], color='#d18604', lw=2,linestyle=(0, (5, 10)),label='12 hours'), ## loosely dashed
                     Line2D([0], [0], color='#913126', lw=2,linestyle=(0, (3, 10, 1, 10, 1, 10)),label='12.4 hours')]) ## loosely dashdotted
    
    ## Add to the legend key for plotting depending on what is in the flags for function
    legend_key = []
    if event_starttime:
        legend_key.append(total_legend[0])
    if event_plus12:
        legend_key.append(total_legend[1])
    if event_plus12p4:
        legend_key.append(total_legend[2])


        
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
        ## If it is not backscatter, delta colormap from cmocean
        if i_property_ind != 3:
            i_colormap = cmocean.cm.delta
            ## Center the colormap:
            i_divnorm = colors.TwoSlopeNorm(vcenter=0)
            i_pmesh = i_axes.pcolormesh(TIME,DEPTH,i_property.data.T,cmap=i_colormap,norm=i_divnorm)
        ## otherwise use matter
        else:
            i_colormap = cmocean.cm.matter
            i_pmesh = i_axes.pcolormesh(TIME,DEPTH,i_property.data.T,cmap=i_colormap)
        i_axes.invert_yaxis()
        
        ## Add colorbar:
        i_cb = plt.colorbar(i_pmesh,ax=i_axes,pad=0.08)
        ## Set colorbar label:
        i_cb.set_label(labelorder[i_property_ind],size=16)
        ## colorbar tick param font size: 
        i_cb.ax.tick_params(labelsize=14)
        
        ## Format x axis labels:
        i_axes.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
        i_axes.xaxis.set_major_locator(mdates.HourLocator(interval=3))
        #i_axes.xaxis.set_minor_locator(mdates.HourLocator(interval=0.5))
        i_axes.xaxis.set_tick_params(which='major',width=2,length=7,direction='out',color='black',labelsize=14)
        i_axes.xaxis.set_tick_params(which='minor',width=1,length=4)

        
        ## Set y-label:
        i_axes.yaxis.set_tick_params(labelsize=14)
        i_axes.set_ylabel('Water\n Depth (m)',fontsize=16)
        
        ### Legend
        ## If it is one of the first three axes:
        if i_property_ind < len(plotorder)-1:
            ## If there is an event time specified and 12/12.4 hours requested on plot, add:
            if event_starttime:
                i_axes.axvline(x=event_starttime, color=total_legend[0].get_color(), 
                               linestyle=total_legend[0].get_linestyle(),
                               linewidth=total_legend[0].get_linewidth())
            if event_plus12:
                print('adding 12 hours at {}'.format(event_starttime + timedelta(hours=12)))
                i_axes.axvline(x=(event_starttime + timedelta(hours=12)), color=total_legend[1].get_color(), 
                               linestyle=total_legend[1].get_linestyle(),
                               linewidth=total_legend[1].get_linewidth())
            if event_plus12p4:
                print('adding 12.4 hours at {}'.format(event_starttime + timedelta(hours=12.4)))
                i_axes.axvline(x=(event_starttime + timedelta(hours=12.4)), color=total_legend[2].get_color(), 
                               linestyle=total_legend[2].get_linestyle(),
                               linewidth=total_legend[2].get_linewidth())
        
        ## At the end, for the last one (backscatter), add turbidity...
        if i_property_ind == len(plotorder)-1:
            turbax = i_axes.twinx()
            turbax.plot(turb_times,turb_data,color='black',linewidth=1,label='Turbidity (NTU)')

            ## also add x label:
            i_axes.set_xlabel('UTC Date Time', fontsize=16)
            #i_axis.set_xlabel('Month/Day/Hour in 2019',fontsize=16)
            # And y-label:
            turbax.set_ylabel('Turbidity (NTU)',fontsize=16)
            
    
    # Add a legend for the entire plot
    # adcp_figure.legend(handles=legend_key,loc='lower left',bbox_to_anchor=(1.01,0.5,0.5,0.5),ncol=1,fontsize='Large',frameon=False)
    adcp_figure.legend(handles=legend_key,loc='lower center',ncol=3,fontsize=16,frameon=False)

           
    return adcp_figure  
           

# %% ######
## Make multi sensor plot
def plot_multidata_timeseries(oxygen_df,chlorophyll_df,temperature_df,turbidity_df,figure_dims,xmin,xmax,event_start=False):
    '''
    Plot all 4 time series together (oxygen, chlorophyll, temp, turbidity) on a 
    4 row, 1 column plot.
    Input:
        oxygen_df:          Pandas dataframe with oxygen info
        chlorophyll_df:     Pandas df with chlorophyll info
        temp_df:            Pandas df with seatemperature info
        turbidity_df:       IS THIS A MSEED OR WHAT
        figure_dims:        Tuple with figure dimensions
        xmin:               Datetime object with xmin for x-axis
        xmax:               Datetime object with xmax for x-axis
        event_start:        Datetime object with even tstart to plot as vertical line. Defualt: False
    '''
    import matplotlib.pyplot as plt
    from matplotlib.dates import DateFormatter
    from datetime import datetime, timedelta
    import numpy as np
    import matplotlib.dates as mdates
    
    
    ## Make plot
    multifig,multiaxes = plt.subplots(nrows=4,ncols=1,figsize=figure_dims,sharex=True)
    
    dataframe_ordered = [oxygen_df,chlorophyll_df,temperature_df,turbidity_df]
    column_ordered = ['oxygen','chlorophyll','seawatertemperature','turbidityntu']
    ylabel_ordered = ['Oxygen \n(ml/L)','Chlorophyll \n(ug/L)','Sea Water \nTemp (C)','Turbidity (NTU)']
    
    ## For each data type, plot:
    for i_axis,i_dataframe,i_column,i_ylabel in zip(multiaxes,dataframe_ordered,column_ordered,ylabel_ordered):
        print('plotting %s' % i_ylabel)

        ## Find the data min and max, based on where the data is between the plot start and end:
        i_datamin = i_dataframe.loc[((i_dataframe.datetime>xmin) & (i_dataframe.datetime<xmax))][i_column].min()
        i_datamax = i_dataframe.loc[((i_dataframe.datetime>xmin) & (i_dataframe.datetime<xmax))][i_column].max()
        i_datadiff = i_datamax - i_datamin
        
        ## If there is a data min and max (i.e., there are data between teh limits), then do all the plotting
        if (np.isnan(i_datamin) == False) and (np.isnan(i_datamax) == False):
            i_axis.plot(i_dataframe.datetime,i_dataframe[i_column],linewidth=1.6,color='#056e6c')
            
            ## Add lines for events identified:
            i_ymin = i_datamin - i_datadiff*0.05
            i_ymax = i_datamax + i_datadiff*0.05
            
           
            if event_start:
                i_axis.vlines(event_start,i_ymin,i_ymax,linewidth=2,linestyle='--',color='#cc8904',label='Detection')
            # i_axis.vlines(earthquakes.FormattedTime.values,i_ymin,i_ymax,linewidth=2,linestyle='-.',color='#5c04c7',label='Earthquake')
        
            ## Set ylimits:
            i_axis.set_ylim([i_ymin,i_ymax])
            
            ## Axis formatter:
            i_axis.yaxis.set_tick_params(labelsize=16)
            
            ## Grid:
            i_axis.grid()
            
        ## Add the label anyways:
        print('adding labels, ymax)')
        ## set label
        i_axis.set_ylabel(i_ylabel,fontsize=16)
        
        ## Legend:
        i_axis.legend(fontsize=16,loc='center right')
    
    print('formatting x axis')
    ## x axis formatter etc at end        
    ## xlimits
    i_axis.set_xlim([xmin,xmax])
    
    i_axis.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d, %Hh'))
    i_axis.xaxis.set_tick_params(labelsize=16,rotation=0)  
    
    ## label:
    i_axis.set_xlabel('Month/Day/Hour in 2019',fontsize=17)
    
    return multifig
