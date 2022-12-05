import pandas as pd
import random as rnd
import re
from dateutil import parser
import math
import json

def get_date_range(file, start, end):
    '''
    file: filepath to Effrosynidis et al.'s climate data
    start: date object for start of range
    end: date object for end of range
    output: return dataframe of tweets within a time range
    '''
    
    lines = []
    with open(file) as f:

        cols = re.sub('\n', '', f.readline()).split(',')

        for line in f:
            date = parser.parse(line.split(',')[0]).date()
            if date >= start and date <= end:
                temp = {}
                vals = re.sub('\n', '', line).split(',')
                for index, key in enumerate(cols):
                    temp[key] = vals[index]
                lines.append(temp)

    return pd.DataFrame(lines, columns=cols)

def get_sample_df(file, perc):
    '''
    file: filepath to Effrosynidis et al.'s climate data
    perc: the liklihood of sampling each row
    output: return dataframe with approximatley "perc"% of the dataset
    '''

    lines = []
    with open(file) as f:

        cols = re.sub('\n', '', f.readline()).split(',')

        for line in f:
    
            if rnd.uniform(0,1) <= perc:
                temp = {}
                vals = re.sub('\n', '', line).split(',')
                for index, key in enumerate(cols):
                    temp[key] = vals[index]
                lines.append(temp)

        df = pd.DataFrame(lines, columns=cols)

    return df