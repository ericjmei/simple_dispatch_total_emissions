# -*- coding: utf-8 -*-
"""
Created on Sun Feb 12 16:19:05 2023

@author: emei3
"""

import pandas
import matplotlib.pylab
import scipy
import scipy.interpolate
import datetime
import math
import copy
import os
from bisect import bisect_left

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

eia923_fname = 'EIA923_Schedules_2_3_4_5_M_12_2017_Final_Revision.xlsx'
ferc714_fname = 'Part 2 Schedule 6 - Balancing Authority Hourly System Lambda.csv'
ferc714IDs_csv= 'Respondent IDs.csv'

try:
    ferc714 = pandas.read_parquet(ferc714_fname.split('.')[0]+'.parquet')
except:
    ferc714 = pandas.read_csv(ferc714_fname) 
    ferc714.to_parquet(ferc714_fname.split('.')[0]+'.parquet', index=False)
    
try:
    ferc714_ids = pandas.read_parquet(ferc714IDs_fname.split('.')[0]+'.parquet')
except:
    ferc714_ids = pandas.read_csv(ferc714IDs_fname) 
    ferc714_ids.to_parquet(ferc714IDs_fname.split('.')[0]+'.parquet', index=False)