#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 23 16:13:57 2022

@author: vjs
"""
from onc.onc import ONC
import requests
import tkinter as tk
from appdirs import user_data_dir
import os
#root = tk.Tk()  
import json
import os
from contextlib import closing
import errno
import sys

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
def data_product_delivery(delivery_parameter_dictionary):
    '''
    Ask for a data product delivery with given search parameters. 
    Input:
        delivery_parameter_dictionary:       Parameter dictionary (see above)
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
def download_data(download_parameter_dictionary,outPath,numbertries):
    '''
    Download data from a data product request
    Input:
        download_parameter_dictionary:      Dictionary with download parameters, see example above
        outPath:                            Directory you would like the file to be downloaded to w/o slash
        numbertries:                        Integer with the number of download attempts before moving on
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
                        print(error)
                        
                        ## add to try counter:
                    status_counter +=1
                    continue
                        
                elif status_counter > numbertries:
                    print('Tried %i times, moving on...' % numbertries)
                    error = 'surpassed download try limit'
                    break
                    
                else:
                    ## Error message:
                    error = 'Error {} - {}'.format(streamResponse.status_code,streamResponse.reason)
                    print (error)
     
    streamResponse.close()
    
    return error, status_counter, filePath


