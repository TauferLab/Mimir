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
    if indir.find("mira")==-1:
        filefilter=prefix+'-'+datalabel+'*_'+config+'_*_profile.txt'
    else:
        filefilter=prefix+'-*'+datalabel+'*_'+config+'_*_profile.txt'

    print indir
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
        item_data=get_results_of_one_setting(library, config, setting, \
            benchmark, datatype, testtype, datalist, indir)
        item_data['setting']=[setting]*len(item_data.index)
        if item_data is not None:
            results.append(item_data)

    result=pd.concat(results)
    return result

"""
Get results of one settings
"""
def get_mrmpi_results_of_one_setting(library, config, setting, \
    benchmark, datatype, testtype, datalist, indir):
    
    filefilter=library+'-'+setting+'-'+benchmark+'-'+\
        datatype+'-'+testtype+'*_*'+config+'.ppn*_phases.txt'
    print indir
    print filefilter

    dfs=[]
    for filename in glob.glob(indir+'/'+filefilter):
        print 'read file:'+filename
        file_data=pd.read_csv(filename)
        dfs.append(file_data)
    
    result=None
    if len(dfs)!=0:
        result=pd.concat(dfs)

    final_result=[]
    for dataset in datalist:
       olddataset=dataset

       if indir.find("mira")==-1:
           if benchmark=='octree':
               olddataset=octree_label_map[dataset]
           elif benchmark=='bfs':
               olddataset=bfs_label_map[dataset]

       print olddataset
       result['dataset']=result['dataset'].apply(str)
       item_data=result[result['dataset']==olddataset]
       if indir.find("mira")==-1:
           if benchmark=='octree' or benchmark=='bfs':
               item_data.replace(olddataset, dataset, inplace=True);

       final_result.append(item_data)
    result=pd.concat(final_result)

    return result

octree_label_map={}
bfs_label_map={}

def init_label_map():
    octree_label_map['p24']='16M';
    octree_label_map['p25']='32M';
    octree_label_map['p26']='64M';
    octree_label_map['p27']='128M';
    octree_label_map['p28']='256M';
    octree_label_map['p29']='512M';
    octree_label_map['p30']='1G';
    octree_label_map['p31']='2G';
    octree_label_map['p32']='4G';
    bfs_label_map['s19']='512K';
    bfs_label_map['s20']='1M';
    bfs_label_map['s21']='2M';
    bfs_label_map['s22']='4M';
    bfs_label_map['s23']='8M';
    bfs_label_map['s24']='16M';
    bfs_label_map['s25']='32M';
    bfs_label_map['s26']='64M';
    bfs_label_map['s27']='128M';
    
"""
Get results of ipdps data format ([benchmark])
"""
def get_mrmpi_results_of_one_benchmark(library, config, settings, \
    benchmark, datatype, testtype, datalist, indir):    

    init_label_map()

    results=[]
    for setting in settings:
        item_data=get_mrmpi_results_of_one_setting(\
            library, config, setting,\
            benchmark, datatype, testtype, \
            datalist, indir)

        item_data['setting']=[setting]*len(item_data.index)
        if item_data is not None:
            results.append(item_data)

    result=pd.concat(results)
    result.rename(columns={'total': 'total_time', \
        'peakmem': 'peakmem_use'}, inplace=True)

    return result

"""
Debug code
"""
if __name__ == "__main__":
    get_mrmpi_results_of_one_benchmark('mrmpi',\
        'a2a', ['p64'], 'bfs', 'graph500s16', \
        'onenode', '1,2', '../../data/mrmpi/comet/')
