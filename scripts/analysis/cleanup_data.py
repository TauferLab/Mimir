#!/bin/python
import sys, os, glob
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import seaborn as sns

from pandas import Series, DataFrame

plt.switch_backend('agg')

"""
Get results ([benchmark]-[setting]-[dataset])
"""
def get_results_of_one_dataset(library, config, setting, \
    benchmark, datatype, testtype, datalabel, indir):

    prefix=library+'-'+setting+'-'+benchmark+'-'+datatype+'-'+testtype
    filefilter=prefix+'-'+datalabel+'-*_'+config+'_*_profile.txt'

    print filefilter

    datalist=[]
    for filename in glob.glob(indir+'/'+filefilter):
        print 'read file:'+filename
        file_data=pd.read_csv(filename)
        file_data=file_data.groupby(by='testtime').mean()
        file_data['dataset']=[datalabel]*len(file_data.index)
        datalist.append(file_data)
    
    result=None
    if len(datalist)!=0:
        result=pd.concat(datalist)
    return result

"""
Get results ([benchmark]-[setting])
"""
def get_results_of_one_setting(library, config, setting, \
    benchmark, datatype, testtype, datalist, indir):
    
    results=[]
    for datalabel in datalist:
        print datalabel
        item_data=get_results_of_one_dataset(library, config, setting, \
            benchmark, datatype, testtype, datalabel, indir)
        if item_data is not None:
            results.append(item_data)
   
    result=pd.concat(results)
    return result

"""
Get results ([benchmark])
"""
def get_results_of_one_benchmark(library, config, settings, \
    benchmark, datatype, testtype, datalist, indir):
    
    results=[]
    for setting in settings:
        print setting
        item_data=get_results_of_one_setting(library, config, setting, \
            benchmark, datatype, testtype, datalist, indir)
        item_data['setting']=[setting]*len(item_data.index)
        if item_data is not None:
            results.append(item_data)

    result=pd.concat(results)
    return result

"""
Get results of ipdps data format ([benchmark])
"""
def get_ipdps_results_of_one_benchmark(library, config, settings, \
    benchmark, datatype, testtype, datalist, indir):
    
    results=[]
    for setting in settings:
        print setting
        filename=library+'-'+setting+'-'+benchmark+'-'+datatype+'-'+testtype+'_'+'config'+'.ppn*_phases.txt'
        print filename

"""
Debug code
"""
if __name__ == "__main__":
    get_ipdps_results_of_one_benchmark(mrmpi, 'a2a',\)
    #data=get_results_of_one_benchmark(\
    #    "mimir", "c64M-p64M-i512M", ["basic"], \
    #    "bfs", "graph500s16", "singlenode", ["s20"], \
    #    "../../data/comet/bfs_singlenode", 24)
    #print data
