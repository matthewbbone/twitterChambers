#!/usr/bin/env python3
# coding: utf-8

#### LIBRARIES ####
import networkx as nx
import numpy as np
import pandas as pd
import os
from datetime import timedelta 
from time import time

# LOCAL
import sys
import chambers_and_audiences as ca
import communities as cm
import similarity_metrics as sm
import polarization as pol

# HELPER FUNCS 
def get_filenames_from_path(path, extension='.csv'):
    files = []
    for file in os.listdir( path ):
        if file.endswith(extension):
            files.append(file)

    return files

def timestamps_from_date( weeks, date='01-Mar-2019', date_format='%d-%m-%Y' ):
    '''Transforms set of week numbers:array[int] into date objects starting from date `date`.    
    '''
    t0 = pd.to_datetime( date )
    t_array = []
    
    for week in range(weeks):
        
        dt = timedelta(weeks = week)
        t_array.append( (t0 + dt).strftime( date_format ) )
        
    return pd.to_datetime(t_array, dayfirst=True)#, format=date_format)

print('Read all libraries.')
tic = time()

## EDGELISTS ##
DATA_NWS_PATH = './weekly_retweet_networks/'
networks_names = sorted( get_filenames_from_path(DATA_NWS_PATH) )

edgelists = os.listdir(DATA_NWS_PATH)
# obtain all timestamps from the edgelist
times = timestamps_from_date( len(edgelists) , date_format='%d-%m-%Y')

elapsed_time = np.round( (time() - tic) / 60, 2 )
print('Read all edgelists. {} minutes passed'.format(elapsed_time))

## GET PERSISTENT USERS ## 
N = 50 # number of popular users per week
M = 50 # number of persistent users

# impact, users, users' frequencies
w_IΔ, IΔ, users_persistence = ca.temporal_leading_impacts(edgelists, N, M)
_, I_leading = ca.temporal_leading_impacts(edgelists, N)

users_leading_frequencies = ca.get_users_persistence( I_leading )
persistent_leading_users = ca.get_users_from_users_persistence_dict(users_persistence)

leading_voices_dynamics = pd.DataFrame( columns=persistent_leading_users, index=times )

for (t, w_t) in enumerate( w_IΔ ):    
    leading_voices_dynamics.loc[ times[t] ] = w_t['weight']

elapsed_time = np.round( (time() - tic) / 60, 2 )
print('Computed (persistent) leading users. {} minutes passed'.format(elapsed_time))