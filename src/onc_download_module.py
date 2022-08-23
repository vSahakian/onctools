#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 23 16:13:57 2022

@author: vjs
"""
from onc.onc import ONC
import requests
from appdirs import user_data_dir
import os
#root = tk.Tk()  
import json
from contextlib import closing
import errno
import sys
import time

# %%
## Example parameter dictionary
# delivery_parameters = {'method':'request',
#             'token':token,          # replace YOUR_TOKEN_HERE with your personal token obtained from the 'Web Services API' tab at https://data.oceannetworks.ca/Profile when logged in.
#             'locationCode':'BACAX',             # Barkley Canyon / Axis (POD 1)
#             'propertyCode':'turbidityntu',    # 150 kHz Acoustic Doppler Current Profiler
#             'dataProductCode':'TSSD',           # Time Series Scalar Data
#             'extension':'csv',                  # Comma Separated spreadsheet file
#             'dateFrom':'2019-12-24T00:00:00.000Z',  # The datetime of the first data point (From Date)
#             'dateTo':'2019-12-26T00:00:00.000Z',    # The datetime of the last data point (To Date)
#             'dpo_qualityControl':1,             # The Quality Control data product option - See https://wiki.oceannetworks.ca/display/DP/1
#             'dpo_resample':'average',              # The Resampling data product option - See https://wiki.oceannetworks.ca/display/DP/1
#             'dpo_average':60,                   # Resampling average, 60=1 minutes
#             'dpo_dataGaps':0}                   # The Data Gaps data product option - See https://wiki.oceannetworks.ca/display/DP/1


## Request parameter dictionary using delivery info
# request_parameter_dictionary = {'method':'run',
#             'token':token,              # replace YOUR_TOKEN_HERE with your personal token obtained from the 'Web Services API' tab at https://data.oceannetworks.ca/Profile when logged in.
#             'dpRequestId':requestInfo['dpRequestId']}     # replace YOUR_REQUEST_ID_HERE with a requestId number returned from the request method


## Download parameter dictionary using request info
# download_parameters = {'method':'download',
#             'token':token,   # replace YOUR_TOKEN_HERE with your personal token obtained from the 'Web Services API' tab at https://data.oceannetworks.ca/Profile when logged in..
#             'dpRunId':r[0]['dpRunId'],       # replace YOUR_RUN_ID with the dpRunId returned from the 'run' method.

#             'index':1}                   # for run requests that contain more than one file, change the index number to the index of the file you would like to download.
#                                            # If the index number does not exist an HTTP 410 and a message will be returned.
 

# %% Token function
def Token():
    global tokenpath
    tokenpath = user_data_dir("ONC-Token", "ONC")
    if os.path.exists(tokenpath + r"\token.txt"):
        print("Token file exists.")
        f = open(tokenpath + r"\token.txt", "r")
        token = f.read()
        f.close()
        return token
    
# %% Request dictionary:
def make_request_dictionary(token,requestInfo):
    '''
    Make a request parameter dictionary to run the request
    Input:
        token:          ONC token
        requestInfo:    Return request response from the data product delivery
    Output:
        request_parameter_dictionary
    '''
    request_parameter_dictionary = {'method':'run',
                'token':token,              # replace YOUR_TOKEN_HERE with your personal token obtained from the 'Web Services API' tab at https://data.oceannetworks.ca/Profile when logged in.
                'dpRequestId':requestInfo['dpRequestId']}     # replace YOUR_REQUEST_ID_HERE with a requestId number returned from the request method

    return request_parameter_dictionary

# %% Request dictionary:
def make_download_dictionary(token,request_response,request_index):
    '''
    Make a request parameter dictionary to run the request
    Input:
        token:          ONC token
        request_response:    Return request response from the data product delivery
        request_index:          Index on the request to run (there may be many)
    Output:
        download_parameter_dictionary
    '''
    download_parameter_dictionary = {'method':'download',
                'token':token,   # replace YOUR_TOKEN_HERE with your personal token obtained from the 'Web Services API' tab at https://data.oceannetworks.ca/Profile when logged in..
                'dpRunId':request_response[request_index]['dpRunId'],       # replace YOUR_RUN_ID with the dpRunId returned from the 'run' method.
                'index':1}                   # for run requests that contain more than one file, change the index number to the index of the file you would like to download.
                                                # If the index number does not exist an HTTP 410 and a message will be returned.
     
    return download_parameter_dictionary

# %% Ask for a data product delivery, given search paramters. Makes a response which
##   is then Ran in a Data Run Request...
def data_product_delivery(delivery_parameter_dictionary,verbose=True):
    '''
    Ask for a data product delivery with given search parameters. 
    Input:
        delivery_parameter_dictionary:       Parameter dictionary (see above)
        verbose:                             Boolean with whether or not to print everything. DEfault: True (verbose)
    Output:
        requestInfo:                        Object with information about request to use for download)
        Error:                              Error message if no data exists)
        data_exists:                        Boolean on whether data for those parameters exists or not
    '''
    
    ## Set URL for data at ONC:
    url = 'https://data.oceannetworks.ca/api/dataProductDelivery'

    ## Run a request for data to get the response
    response = requests.get(url,params=delivery_parameter_dictionary)
      
    ## check if data exist
    if (response.ok):
        requestInfo = json.loads(str(response.content,'utf-8')) # convert the json response to an object
        
        ## Specify in the return that there is data for this request:
        data_exists = True
        error = 'None' 
        
        if verbose == True:
            print('Request Id: {}'.format(requestInfo['dpRequestId']))      # Print the Request Id
             
            if ('numFiles' in requestInfo.keys()):
                print('File Count: {}'.format(requestInfo['numFiles']))     # Print the Estimated File Size
          
            if ('fileSize' in requestInfo.keys()):
                print('File Size: {}'.format(requestInfo['fileSize']))      # Print the Estimated File Size
             
            if 'downloadTimes' in requestInfo.keys():
                print('Estimated download time:')
                for e in sorted(requestInfo['downloadTimes'].items(),key=lambda t: t[1]):
                    print('  {} - {} sec'.format(e[0],'{:0.2f}'.format(e[1])))
         
         
            if 'estimatedFileSize' in requestInfo.keys():
                print('Estimated File Size: {}'.format(requestInfo['estimatedFileSize']))
                         
            if 'estimatedProcessingTime' in requestInfo.keys():
                print('Estimated Processing Time: {}'.format(requestInfo['estimatedProcessingTime']))
      
    else:
        ## Specify that there's no data:
        data_exists = False
        requestInfo = False
        
        if(response.status_code == 400):
            error = json.loads(str(response.content,'utf-8'))      
            print(error) # json response contains a list of errors, with an errorMessage and parameter
        else:
            print ('Error {} - {}'.format(response.status_code,response.reason))
            
    ## Return error and data flag
    return requestInfo, error, data_exists
            
    
        
# %%
## Run the data product request...
def data_product_request(request_parameter_dictionary):
    '''
    Runs a data product request with the requestInfo from the data product delivery
    Input:              request_parameter_dictionary (see above for example)
    Ouput:
        r:              json response object
        error:          Error message, if any
        request_ok:     Boolean of whether the request gavean error or not
    
    '''
    
    url = 'https://data.oceannetworks.ca/api/dataProductDelivery'      
    response = requests.get(url,params=request_parameter_dictionary)
      
    if (response.ok):
        r = json.loads(str(response.content,'utf-8')) # convert the json response to an object
        
        ## Set the boolean to true
        request_ok = True
        error = 'None'
      
        for runId in [run['dpRunId'] for run in r]:
            print('Run Id: {}'.format(runId))       # Print each of the Run Ids
             
    else:
        ## Set the boolean to false
        request_ok = False
        
        ## Set runId: 
        runId = False
        
        if(response.status_code == 400):
            error = json.loads(str(response.content,'utf-8'))
            print(error) # json response contains a list of errors, with an errorMessage and parameter 
            
        
        else:
            error = 'Error {} - {}'.format(response.status_code,response.reason)
            print (error)
            
            
    return r, error, request_ok
        
# %% Download the data. Saves to outPath
def download_data(download_parameter_dictionary,outPath,numbertries,inter_try_sleep_time=2):
    '''
    Download data from a data product request
    Input:
        download_parameter_dictionary:      Dictionary with download parameters, see example above
        outPath:                            Directory you would like the file to be downloaded to w/o slash
        numbertries:                        Integer with the number of download attempts before moving on
        inter_try_sleep_time:               Integer with seconds to sleep in between tries
    Output:
        error:                              STring with error message if no download
        status_counter:                     Number of tries before it downloaded
        filePath:                           Path to where data are stored
    '''
    
    ## Set download URL:
    url = 'https://data.oceannetworks.ca/api/dataProductDelivery'

    ## Set counters for the HTML status tests
    status_code_check = False 
    status_counter = 0
    
    ## As long as the status is not downloading, keep trying...
    while (status_code_check == False) and (status_counter <= numbertries):
        print('trying download')
        with closing(requests.get(url,params=download_parameter_dictionary,stream=True)) as streamResponse:
            
            ## Add to the try status counter, for number of download tries.
            status_counter +=1            
            
            ## If the HTML response is 200, everything downloaded, so save the data.
            if streamResponse.status_code == 200: #OK
            
                ## Change code to false so it doesn't try to download again
                status_code_check = True
                ## Set the error to False
                error = False
                
                print('it worked')
                if 'Content-Disposition' in streamResponse.headers.keys():
                    content = streamResponse.headers['Content-Disposition']
                    filename = content.split('filename=')[1]
                    
                else:
                    print('Error: Invalid Header')
                    streamResponse.close()
                    sys.exit(-1)
                    
                    ## Error message:
                    error = 'Invalid Header, did not write'
                 
                ## Write to file:
                filePath = '{}/{}'.format(outPath,filename)
                
                try:
                    if (not os.path.isfile(filePath)):
                        #Create the directory structure if it doesn't already exist
                        try:
                            os.makedirs(outPath)
                        except OSError as exc:
                            if exc.errno == errno.EEXIST and os.path.isdir(outPath):
                                pass
                            else:
                                raise
                        print ("Downloading '{}'".format(filename))
         
                        with open(filePath,'wb') as handle:
                            try:
                                for block in streamResponse.iter_content(1024):
                                    handle.write(block)
                                    error = 'None'
                            except KeyboardInterrupt:
                                print('Process interupted: Deleting {}'.format(filePath))
                                handle.close()
                                streamResponse.close()
                                os.remove(filePath)
                                sys.exit(-1)
                                
                                ## Error message:
                                error = 'writing process interrupted'
                    else:
                        print ("  Skipping '{}': File Already Exists".format(filename))
                        
                        ## Error message:
                        error = 'File already exists'
                        break
                    
                except:
                    msg = 'Error streaming response.'
                    print(msg)
                    filePath = 'No file'
                    
                    continue
            else:
                filePath = 'No file'
                if((streamResponse.status_code in [202,204,400,404,410]) and (status_counter <= numbertries)):
                    payload = json.loads(str(streamResponse.content,'utf-8'))
                    if len(payload) >= 1:
                        try:
                            msg = payload['message']
                        except:
                            msg = 'No message available'
                        
                        ## Save and print error message
                        error = 'HTTP {} - {}: {}'.format(streamResponse.status_code,streamResponse.reason,msg)
                        
                        ## Only print if the number of tries is a multiple of 5:
                        if status_counter % 5 == 0:
                            print('on %ith try, %s' % (status_counter,error))
                        
                    
                    print('Sleeping %i seconds before next attempt' % inter_try_sleep_time)
                    time.sleep(inter_try_sleep_time) 
                    continue
                        
                ## IF it hits an error code/status code of 500,
                elif streamResponse.status_code == 500:
                    print('Status code 500, internal server error, moving on \n')
                    error = 'Status code is 500, internal server error'
                    time.sleep(inter_try_sleep_time)
                    continue
                
                elif status_counter > numbertries:
                    print('Tried %i times, moving on...' % numbertries)
                    error = 'surpassed download try limit'
                    break
                    
                else:
                    ## Error message:
                    error = 'Error {} - {}'.format(streamResponse.status_code,streamResponse.reason)
                    print (error)
                    print('Sleeping %i seconds before next attempt' % inter_try_sleep_time)
                    time.sleep(inter_try_sleep_time) 
                    continue
     
    streamResponse.close()
    
    return error, status_counter, filePath


# %% FUNCTION
def batch_test_and_download(search_params_list,presearch_parameter_df,search_indices,token,data_dir,download_tries,metadata_path_suffix='v0',sleeptime=10):
    '''
    Searches for (data exists), and downloads 
    
    Input:
        search_params_list:             A list with the parameters to search
        presearch_parameter_df:         A dataframe of search parameters to loop through
        search_indices:                 A list/array with the indices of the search dataframe to search/download
        token:                          String with ONC token
        data_dir:                       String with the data directory to store things in
        download_tries:                 Integer with the number of download attempts before moving on
        metadata_path_suffix:           String to append to the end of the download_metadata csv file. Default: v0
        sleeptime:                      Number of seconds to wait/sleep in between tests if get a 202. Default: 10 seconds.
    Output:
        postsearch_parameter_df:        A dataframe with metadata after the search
    '''
    
    ## Set warning to be OFF for copy thing...
    import pandas as pd
    pd.options.mode.chained_assignment = None  # default='warn'
    
    #################################################################
    ## Before going in, make a new data frame that is a copy:
    parameter_df = presearch_parameter_df.copy(deep=True)
    
    ## Run through the search indices
    for i_day in search_indices:
        
        ## Get the search parameters:
        i_search_parameters_full = presearch_parameter_df.loc[i_day].to_dict()
        i_search_parameters = { key:value for key,value in i_search_parameters_full.items() if key in search_params_list}
            
        print('\n \n ##### running from %s to %s' % (i_search_parameters['dateFrom'],i_search_parameters['dateTo']))
    
    
        ## STEP 1: Run the data product delivery
        print('running data product delivery')
        i_delivery_requestInfo, i_delivery_error, i_data_exists = data_product_delivery(i_search_parameters)
        
        ## Append to master dataframe:
        parameter_df['delivery_error'][i_day] = i_delivery_error
        parameter_df['data_exists'][i_day] = i_data_exists
    
        
        ## If there is no data, continue on to the next part of hte loop:
        if i_data_exists == False:
            continue
        
        else:
            ## STEP 2: Make a dictionary to run a data product request
            i_request_parameter_dictionary = make_request_dictionary(token,i_delivery_requestInfo)
            
            ## Run the data product request
            print('running data product request')
            i_product_request, i_request_error, i_request_ok = data_product_request(i_request_parameter_dictionary)
            
            ## Append to master dataframe:
            parameter_df['request_error'][i_day] = i_request_error
            parameter_df['request_ok'][i_day] = i_request_ok
                      
            ## STEP 3: Make a dictionary to run the download
            i_request_index = 0
            i_download_parameter_dictionary = make_download_dictionary(token,i_product_request,i_request_index)
            
            ## Run the download...
            print('attempting download...')
            i_download_error, i_status_counter,i_filePath = download_data(i_download_parameter_dictionary,data_dir,download_tries)
        
            ## Append to master dataframe:
            parameter_df['download_error'][i_day] = i_download_error
            parameter_df['dL_status_counter'][i_day] = i_status_counter
            parameter_df['filePath'][i_day] = i_filePath
            
            ## If data actually downloaded, add a 10 second sleep:
            # if i_filePath != 'No file':
            print('Sleeping %i seconds before next attempt' % sleeptime)
            time.sleep(sleeptime)    
        
    ## Save to a post search metadata file 
    parameter_df.to_csv(data_dir + '/metadata/download_metadata_' + metadata_path_suffix + '.csv')
    
    ## Return:
    return parameter_df


# %%
# Convert dataframe from the csv file to a stream object
def convert_df2stream(template_df,data_name,datetime_name='datetime',network='',station='',location='',channel='',force_sample_rate=[True,1/60]):
    '''
    Convert a dataframe with data to a stream object 
    Input: 
        dataframe:              Pandas dataframe with columns of date/time and data to convert
        data_name:              String with the column name for the data 
        datetime_name:          String with the column name for the datetime (default: 'datetime')
        network:                String with the name of the network. Default: empty ('')
        station:                String with the name of the station. Default: empty ('')
        location:               String with the name of the location code. Default: empty ('')
        channel:                String with the name of the channel or data type. Default: empty ('')
        force_sample_rate:      List with [True/False, samplerate_Hz] of [0] whether to force sample rate and if so [1] the sample rate in Hz
    Output:
        data_stream:            Obspy stream object with the data
    '''
    
    from scipy.stats import mode
    import obspy as obs
    import numpy as np
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta

    ## Reset the indices in the dataframe, just inc ase:
    template_df = template_df.reset_index(drop=True)
    
    ## If the sample rate is not forced, figure out what it should be:
    if force_sample_rate[0] == False:
        ## Get the sample rate in seconds - the mode of the difference of the 
        ##      datetime column, as a secondstimedelta covnerted to integer:
        mode_sample_rate_seconds = mode(np.diff(template_df[datetime_name])).mode.astype('timedelta64[s]').astype('int')
        
        ## Convert to Hz for the header:
        mode_sample_rate_Hz = 1/mode_sample_rate_seconds
    else:
        mode_sample_rate_Hz = force_sample_rate[1]
        
        ## and also in seconds for later:
        mode_sample_rate_seconds = 1/mode_sample_rate_Hz
    
    
    ## Make the stats for the trace header:
    trace_stats = {'network': network, 'station': station, 'location': location,
             'channel': channel,
             'sampling_rate': mode_sample_rate_Hz}
             #'mseed': {'dataquality': 'D'}}
    
    
    #### Find instances in the array where the sampling interval is not what is prescribed.
    
    ## Get the difference between each data point in seconds:
    sampling_difference_seconds = np.diff(template_df[datetime_name]).astype('timedelta64[s]').astype('int')
    ## Get locations where it's different from the prescibed/mode sample rate:
    index_databreak = np.where(sampling_difference_seconds != mode_sample_rate_seconds)[0]
    
    ## If there are no data breaks (this is empty), it's all one trace:
    if len(index_databreak) == 0:
        ## First convert to a pandas datetime object, then get the first index, then format the same as ONC wants:
        trace_starttime = pd.to_datetime(template_df[datetime_name]).loc[0].strftime('%Y-%m-%dT%H:%M:%S') + '.000Z'
        trace_stats['starttime'] = trace_starttime
        data_stream = obs.Stream([obs.Trace(data=template_df[data_name].values, header=trace_stats)])
    
    ## Otherwise, need to split it up by index
    else:
        data_stream = obs.Stream([])
        
        ## Make starting index array. 
        ##  Databreak index is the first index before the data break, 
        ##   so add 0 on front and add 1 to the rest of them
        databreak_starting_index = np.append(0,(index_databreak+1))
        
        ## And ending index array - needs to end at the index after the databreak 
        ##      because of python indexing - this index son't be included. 
        #       Append length of time series as the last index to go to.
        databreak_ending_index = np.append(index_databreak+1,len(template_df))
            
        ## Loop through the databreaks and separate into traces:
        for i_databreak in range(len(databreak_starting_index)):

            ## Define the trace as the data from the last data break to here:
            i_start = databreak_starting_index[i_databreak]
            i_end = databreak_ending_index[i_databreak]
            i_data = template_df[data_name].loc[i_start:i_end].values
            
            ## Need start time for trace stats - first get the pandas datetime object
            i_trace_starttime_pandasdt = template_df[datetime_name].loc[i_start]
            
            ## Then before adding to starttime, convert to appropriate time:
            trace_stats['starttime'] = pd.to_datetime(i_trace_starttime_pandasdt).strftime('%Y-%m-%dT%H:%M:%S') + '.000Z'
            
            ## And the number of points:
            trace_stats['npts'] = len(i_data)
                
            i_trace = obs.Trace(data=i_data, header=trace_stats)
            ## Add to the stream:
            data_stream += i_trace
    
    ## return the stream object:
    return data_stream
    
    
    