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

def draw_bar_and_line(data, outfile,
        x_label = 'dataset', hue_label = 'label',
        line_label = 'total_time', bar_label = 'peakmem_use',
        xticklist = [], xlabelname = '', legendlist = [],
        barcolorlist = [], linecolorlist = [], markerlist = [],
        hatchlist = "", barylim = [], lineylim = []):

    fig_data=data

    fig = plt.figure()
    ax_bar = fig.add_subplot(111)
    ax_line = ax_bar.twinx()

    " Draw memory usage bar "
    ax_bar = sns.barplot(x = x_label, y = bar_label, hue = hue_label,
            data = fig_data, ax = ax_bar, linewidth=1, palette = barcolorlist, 
            fill=True)

    """
    Ensure color, marker ... in the correct order.
    """
    order_map = [None]*len(legendlist)
    for i in range(0, len(legendlist)):
        item_data = fig_data[fig_data[hue_label]==legendlist[i]]
        order_map[i] = len(item_data[x_label].unique())
    order_map = np.array(order_map).argsort()
    hue_order = np.array(legendlist)[order_map].tolist()
    rlinecolorlist = np.array(linecolorlist[:len(legendlist)])[order_map].tolist()
    markerlist = np.array(markerlist[:len(legendlist)])[order_map].tolist()
    hue_order.reverse()
    rlinecolorlist.reverse()
    markerlist.reverse()

    """
    Draw execution time
    """
    ax_line = sns.pointplot(x=x_label, y=line_label, hue=hue_label, 
            data = fig_data, markers = markerlist, 
            hue_order=hue_order, ax = ax_line, linestyles='-', errwidth=0.5,
            ci=None, linewidth=0.5, join=True, scale=1.5, palette=rlinecolorlist)

    """
    Draw hatches
    """
    bars = ax_bar.patches
    hatchlist = ''.join(h*len(xticklist) for h in hatchlist)
    for bar, hatch in zip(bars, hatchlist):
        bar.set_hatch(hatch)

    """
    Set figure property
    """ 
    ax_bar.tick_params(labelsize=26)
    ax_bar.set_xticklabels(xticklist, rotation=45)
    handles, labels = ax_bar.get_legend_handles_labels()
    legend_bar=ax_bar.legend(loc='upper left', \
            title='peak memory usage', prop={'size':14})
    legend_bar=ax_bar.legend(handles, legendlist, loc='upper left', \
            title='peak memory usage', prop={'size':14})

    legend_bar.get_title().set_fontsize('14')
    legend_bar.get_title().set_fontweight('bold')

    ax_bar.set_xlabel(xlabelname, fontsize=26, fontweight="bold")
    ax_bar.set_ylabel("peak memory usage (GB)", \
            fontsize=26, fontweight="bold")
    ax_bar.set_ylim(barylim)

    ax_line.tick_params(labelsize=26)
    handles, labels = ax_line.get_legend_handles_labels()
    handles=np.array(handles)[order_map].tolist()
    labels=np.array(labels)[order_map].tolist()
    handles.reverse()
    labels.reverse()
    legend_line=ax_line.legend(handles, labels, loc='upper right', \
            title='execution time', prop={'size':14}) 
    legend_line=ax_line.legend(handles, legendlist, loc='upper right', \
            title='execution time', prop={'size':14})

    legend_line.get_title().set_fontsize('14') 
    legend_line.get_title().set_fontweight('bold')  

    ax_line.set_ylabel("execution time (second)", \
            fontsize=26, fontweight="bold")
    ax_line.set_ylim(lineylim)

    """
    save figure
    """ 
    plt.tight_layout()
    plt.savefig(outfile)
    return

def draw_line(data, outfile,
        x_label = 'xlabel',
        hue_label = 'huelabel',
        y_label = 'total_time',
        hue_order = '',
        order = '',
        xlabelname = '',
        ylabelname = 'execution time (sec)', 
        xticklist = [],
        legendlist = [],
        colorlist = [],
        markerlist = [],
        ylim = [],
        ylog = False,
        isweakscale = False,
        isstrongscale = False):

    fig_data=data

    """
    Draw figures
    """
    ax=sns.pointplot(x = x_label, y = y_label, hue = hue_label,
            data=fig_data, scale=1.5, order = order, hue_order = hue_order,
            palette=colorlist, markers=markerlist, dodge=False, linestyles='-')

    """
    Set figure properties
    """
    if ylim:
        ax.set_ylim(ylim)
    print xticklist
    if xticklist:
        ax.set_xticklabels(xticklist, rotation=45)
    if ylog == True:
        ax.set_yscale('log')

    ax.tick_params(labelsize=20)
    handles, labels = ax.get_legend_handles_labels()

    ax.legend(loc=2, prop={'size':23}, ncol=1)
    ax.set_xlabel(xlabelname, fontsize=26, fontweight="bold")
    ax.set_ylabel(ylabelname, fontsize=26, fontweight="bold")

    """
    Save figure
    """
    plt.grid(True)
    plt.tight_layout()
    print "Save figure ",outfile
    plt.savefig(outfile)
    return
