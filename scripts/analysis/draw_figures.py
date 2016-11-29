#!/bin/python
import sys, os, glob
import argparse
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import seaborn as sns

from pandas import Series, DataFrame

plt.switch_backend('agg')

sns.set_style("ticks")

"""
This function draw memory usage and excution time 
in a single figures.
The input DataFrame should contain the following 
columns:
["dataset", "setting", "total_time", "peakmem_use"]
"""
def draw_memory_and_time(data, outdir, outfile, \
  xticklist=[], \
  labellist=[], \
  xlabelname='', \
  settings= [], \
  memcolors=["red", "green", "blue"], \
  timecolors=["red", "green", "blue"], \
  markerlist=["*","v","o"], \
  hatches="x/o", \
  memlim=[0, 6],\
  timelim=[0, 400]):

    fig_data=data

    fig = plt.figure()
    ax_mem = fig.add_subplot(111)
    ax_time = ax_mem.twinx()

    " Draw memory usage bar "
    fig_data['peakmem_use']=fig_data['peakmem_use'].divide(1024*1024*1024)
    ax_mem=sns.barplot(x='dataset', y='peakmem_use', hue='setting', \
        data=fig_data, ax=ax_mem, linewidth=1, color='red', \
        palette=memcolors, fill=True)

    """
    Ensure color, marker ... in the correct order.
    """
    mapper = [None]*len(labellist)
    #print mapper
    mid=0
    for label in labellist:
        item_data=fig_data[fig_data['setting']==label]
        mapper[mid]=len(item_data['dataset'].unique())
        mid+=1
    mapper = np.array(mapper).argsort()
    rsettings=np.array(settings)[mapper].tolist()
    rtimecolors=np.array(timecolors)[mapper].tolist()
    markerlist=np.array(markerlist)[mapper].tolist()
    #print labellist
    rsettings.reverse()
    rtimecolors.reverse()
    markerlist.reverse()


    """
    Draw execution time
    """
    #print markerlist
    #print rlabellist
    #print rtimecolors
    ax_time=sns.pointplot(x='dataset', y='total_time', hue='setting', \
        data=fig_data, \
        markers=markerlist, \
        hue_order=rsettings,\
        ax=ax_time, linestyles='-', errwidth=0.5, ci=None, \
        linewidth=0.5, join=True, scale=1.5, palette=rtimecolors)

    " Draw hatches "
    bars = ax_mem.patches
    print hatches
    hatches = ''.join(h*len(xticklist) for h in hatches)
    print hatches
    for bar, hatch in zip(bars, hatches):
        bar.set_hatch(hatch)
    #print labellist
 
    """
    Set figure property
    """ 
    ax_mem.tick_params(labelsize=26)
    print xticklist
    ax_mem.set_xticklabels(xticklist, rotation=45)
    legend_mem=ax_mem.legend(loc='upper left', \
        title='peak memory usage', prop={'size':18})
    legend_mem.get_title().set_fontsize('18') 
    legend_mem.get_title().set_fontweight('bold')  
    ax_mem.set_xlabel(xlabelname, fontsize=26, fontweight="bold")
    ax_mem.set_ylabel("peak memory usage (GB)", \
        fontsize=26, fontweight="bold")
    ax_mem.set_ylim(memlim)
    
   
    ax_time.tick_params(labelsize=26)
    print xticklist
    ax_time.set_xticklabels(xticklist, rotation=45)
    handles, labels = ax_time.get_legend_handles_labels()
    handles=np.array(handles)[mapper].tolist()
    labels=np.array(labels)[mapper].tolist()
    handles.reverse()
    labels.reverse()
    legend_time=ax_time.legend(handles, labels, loc='upper right', \
        title='execution time', prop={'size':18}) 
    legend_time.get_title().set_fontsize('18') 
    legend_time.get_title().set_fontweight('bold')  
    ax_time.set_ylabel("execution time (second)", \
        fontsize=26, fontweight="bold")
    ax_time.set_ylim(timelim)

    """
    save figure
    """ 
    plt.tight_layout()
    print outdir
    print outfile
    plt.savefig(outdir+'/'+outfile)
    return

"""
This function draw execution time.
The input DataFrame should contain the following 
columns:
["dataset", "setting", "total_time"]
"""
def draw_total_time(data, outdir, outfile, \
  xticklist=[], \
  labellist=[], \
  xlabelname='', \
  settings= [], \
  colorlist=["red", "green", "blue"], \
  markerlist=["*","v","o"], \
  ylim=[0, 0]):

    fig_data=data

    mapper = [0]*len(labellist)
    mid=0
    for label in labellist:
        item_data=fig_data[fig_data['setting']==label]
        mapper[mid] = len(item_data['dataset'].unique())
        mid+=1
  
    for i in range(0,len(mapper)):
        for j in range(i+1,len(mapper)):
            if mapper[j]==mapper[i]:
                mapper[i]+=1

    mapper = np.array(mapper).argsort()[::-1][:len(labellist)]
    """
    Draw figures
    """
    sns.set_style("ticks")
    rsettings=np.array(settings)[mapper].tolist()
    rcolorlist=np.array(colorlist)[mapper].tolist()
    markerlist=np.array(markerlist)[mapper].tolist()

    #print rxticklist
    #print fig_data
    #print markerlist
    print rsettings
    ax=sns.pointplot(x='dataset', y='total_time', hue='setting', \
        data=fig_data, scale=1.5, hue_order=rsettings, \
        palette=rcolorlist, markers=markerlist, dodge=False, linestyles='-')
 
    """
    Set figure properties
    """ 
    ax.set_ylim(ylim)
    ax.tick_params(labelsize=28)    
    ax.set_xticklabels(xticklist, rotation=45)
    handles, labels = ax.get_legend_handles_labels()

    mapper1=[0]*len(mapper)
    for i in range(0,len(mapper)):
        mapper1[mapper[i]]=i
    handles=np.array(handles)[mapper1].tolist()
    labels=np.array(labels)[mapper1].tolist()
    ax.legend(handles, labels,loc=2,prop={'size':23},ncol=1)
    #if args.legend[0]=='false':
    #    ax.legend().set_visible(False)
    ax.set_xlabel(xlabelname, fontsize=26, fontweight="bold")
    ax.set_ylabel("execution time(second)", \
        fontsize=26, fontweight="bold")

    """
    Save figure
    """
    plt.grid(True)
    plt.tight_layout()
    plt.gca()
    plt.show()
    plt.savefig(outdir+'/'+outfile)
    return


