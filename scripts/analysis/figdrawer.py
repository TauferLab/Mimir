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
    mapper = [None]*len(settings)
    #print mapper
    mid=0
    for setting in settings:
        item_data=fig_data[fig_data['setting']==setting]
        mapper[mid]=len(item_data['dataset'].unique())
        mid+=1
    mapper = np.array(mapper).argsort()
    #print mapper
    rsettings=np.array(settings)[mapper].tolist()
    rtimecolors=np.array(timecolors[:len(settings)])[mapper].tolist()
    markerlist=np.array(markerlist[:len(settings)])[mapper].tolist()
    #print labellist
    rsettings.reverse()
    rtimecolors.reverse()
    markerlist.reverse()

    print rsettings

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
    #ax_mem.set_xticklabels(xticklist, rotation=45)
    handles, labels = ax_mem.get_legend_handles_labels()
    legend_mem=ax_mem.legend(loc='upper left', \
            title='peak memory usage', prop={'size':18})
    legend_mem=ax_mem.legend(handles, labellist, loc='upper left', \
            title='peak memory usage', prop={'size':18})

    legend_mem.get_title().set_fontsize('18') 
    legend_mem.get_title().set_fontweight('bold')  
    #legend_mem.set_text(labellist)

    ax_mem.set_xlabel(xlabelname, fontsize=26, fontweight="bold")
    ax_mem.set_ylabel("peak memory usage (GB)", \
            fontsize=26, fontweight="bold")
    ax_mem.set_ylim(memlim)

    ax_time.tick_params(labelsize=26)
    #print xticklist
    #ax_time.set_xticklabels(xticklist, rotation=45)
    handles, labels = ax_time.get_legend_handles_labels()
    handles=np.array(handles)[mapper].tolist()
    labels=np.array(labels)[mapper].tolist()
    handles.reverse()
    labels.reverse()
    legend_time=ax_time.legend(handles, labels, loc='upper right', \
            title='execution time', prop={'size':18}) 
    legend_time=ax_time.legend(handles, labellist, loc='upper right', \
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
This function draw memory usage and excution time 
of four subfigures in a single figure.
The input DataFrame should contain the following 
columns:
["dataset", "setting", "total_time", "peakmem_use"]
"""
def draw_subfigures_memory_and_time(
        datalist=[], 
        settings=[], \
        outdir='./', 
        outfile='./', \
        subtitles=[],\
        xticklists=[], \
        xlabelname=[], \
        labellist=[], \
        barcolors=["red", "green", "blue"], \
        linecolors=["red", "green", "blue"], \
        hatches="x/o", \
        markerlist=["*","v","o"], \
        barlims=[0, 6], \
        linelims=[0, 400]):

    "check figure data"
    if len(datalist)!=4:
        print "the datalist error!"
        exit()
        
    "create figures "
    f, axarr=plt.subplots(2, 2)
    
    bar_handles=[]
    line_handles=[]
    
    mapper = [None]*len(settings)

    " draw figure one by one "
    for i in range(0,4):

        fig_data=datalist[i]

        ax_bar=axarr[i/2, i%2]

        " Draw memory usage bar "
        fig_data['peakmem_use']=fig_data['peakmem_use'].divide(1024*1024*1024)
        print fig_data
        sns.barplot(x='dataset', y='peakmem_use', hue='setting', \
            data=fig_data, ax=ax_bar, linewidth=1, color='red', \
            palette=barcolors, fill=True)

        bar_handles, labels = ax_bar.get_legend_handles_labels()
        ax_bar.legend().set_visible(False)

        ax_bar.set_title(subtitles[i],fontsize=14, fontweight="bold")

        " Draw hatches "
        bars = ax_bar.patches
        hatches=hatches[:len(labellist)]
        bar_hatches = ''.join(h*len(xticklists[i]) for h in hatches)
        for bar, hatch in zip(bars, bar_hatches):
            bar.set_hatch(hatch)

        "set bar properties"
        ax_bar.tick_params(labelsize=14)
        ax_bar.set_xticklabels(xticklists[i], rotation=45)
        ax_bar.set_xlabel(xlabelname[i], fontsize=14, fontweight="bold")
        ax_bar.set_ylabel("peak memory (GB)", \
                fontsize=14, fontweight="bold")
        ax_bar.set_ylim(barlims[i])

        """
        Ensure color, marker ... in the correct order.
        """
        mid=0
        for setting in settings:
            item_data=fig_data[fig_data['setting']==setting]
            mapper[mid]=len(item_data['dataset'].unique())
            mid+=1
        mapper = np.array(mapper).argsort()
        print mapper
        print settings
        rsettings=np.array(settings)[mapper].tolist()
        print rsettings
        rlinecolors=np.array(linecolors[:len(settings)])[mapper].tolist()
        rmarkerlist=np.array(markerlist[:len(settings)])[mapper].tolist()
        rsettings.reverse()
        rlinecolors.reverse()
        rmarkerlist.reverse()

        """
        Draw execution time
        """
        ax_line=ax_bar.twinx()
        ax_line=sns.pointplot(x='dataset', y='total_time', hue='setting', \
                data=fig_data, \
                markers=rmarkerlist, \
                hue_order=rsettings,\
                ax=ax_line, \
                linestyles='-', \
                palette=rlinecolors)

        line_handles, labels = ax_line.get_legend_handles_labels()
        ax_line.legend().set_visible(False)

        ax_line.set_title(subtitles[i],fontsize=14, fontweight="bold")

        """
        Set figure property
        """ 
        ax_line.tick_params(labelsize=14)
        ax_line.set_xticklabels(xticklists[i], rotation=45)
        ax_line.set_xlabel(xlabelname[i], fontsize=14, fontweight="bold")
        ax_line.set_ylabel("time (seconds)", \
                fontsize=14, fontweight="bold")
        ax_line.set_ylim(linelims[i])

    "set figure properties"
    lgd1=plt.figlegend( bar_handles,\
            labellist, 'upper left', prop={'size':12}, title='peak memory usage',\
            bbox_to_anchor=(0, 1.15), \
            ncol=2, frameon=True, fancybox=True, shadow=False)
    lgd1.get_title().set_fontsize('12') 
    lgd1.get_title().set_fontweight('bold')

    plt.gca().add_artist(lgd1)

    line_handles=np.array(line_handles)[mapper].tolist()
    line_handles.reverse()
    lgd2=plt.figlegend( line_handles,\
            labellist, 'upper right', prop={'size':12}, title='execution time',\
            bbox_to_anchor=(1, 1.15), \
            ncol=2, frameon=True, fancybox=True, shadow=False)
    lgd2.get_title().set_fontsize('12') 
    lgd2.get_title().set_fontweight('bold')

    """
    save figure
    """ 
    plt.tight_layout()
    print outdir
    print outfile
    plt.savefig(outdir+'/'+outfile, bbox_extra_artists=(lgd1,lgd2), \
            bbox_inches='tight')

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
        settings=[], \
        datalist=[], \
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

    print fig_data
    print datalist
    print rsettings
    print rcolorlist
    print markerlist
    ax=sns.pointplot(x='dataset', y='total_time', hue='setting', \
            data=fig_data, scale=1.5, order=datalist, hue_order=rsettings, \
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
    ax.legend(handles, labellist,loc=2,prop={'size':23},ncol=1)
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


