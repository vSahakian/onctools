#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 11:24:52 2023

@author: vjs
"""
## Check which locations/etc. shoul dhave downloaded data, vs. which ones actually did


import pandas as pd

## Files to open:
alldataDL_path = '/Users/vjs/turbidites/observational/data/datadl4fourmile/onlineInstruments_datagaps_april2023.csv'

## Files it was asked to download:
preDL_path = '/Users/vjs/turbidites/observational/data/datadl4fourmile/predownload_metadata_BACAX_turbidityntu.csv'

## Files it downloaded:
postDL_path = '/Users/vjs/turbidites/observational/data/datadl4fourmile/download_metadata_v0.csv'

# %%

alldl = pd.read_csv(alldataDL_path)

## How many all BACAX turbidity, whether data exist or not:
allBACAXturb = alldl.loc[(alldl.locationCode == 'BACAX') & (alldl.propertyCode == 'turbidityntu')]

## Find all BACAX, turbidity where data did NOT exist:
falseBACAXturb = alldl.loc[(alldl.data_exists == False) & (alldl.locationCode == 'BACAX') & (alldl.propertyCode == 'turbidityntu')]

## Find BACAX turbidity where there was data, and it should have downloaded:
allavailable = alldl.loc[(alldl.data_exists == True) & (alldl.locationCode == 'BACAX') & (alldl.propertyCode == 'turbidityntu')]

## Get pre-dl path:
preDL = pd.read_csv(preDL_path)

## Post DL:
postDL = pd.read_csv(postDL_path)

print('There are %i total days that could be asked for, whether data exist or not, BACAX/turbntu' % len(allBACAXturb))
print('%i possible days with no data existed for BACAX/turbidityntu' % len(falseBACAXturb))
print('There are %i records for BACAX, turbidityntu, with data that exist' % len(allavailable))
print('There are %i records that were asked to be downloaded for this most recent gap run, after culling with make4mile_dataGap_downloadFile_april3_2023' % len(preDL))
print('and %i records were actually downloaded last week (4/5/2023) % len(postDL')