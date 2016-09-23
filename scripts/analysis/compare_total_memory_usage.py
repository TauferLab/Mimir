#!/usr/bin/python
import sys,os
import argparse
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import seaborn as sns

from pandas import Series, DataFrame

plt.switch_backend('agg')

"""
Constant variables
"""
colorlist=['red', 'blue', 'green', 'yellow', \
           'cyan', 'lightgreen', 'lightblue', 'olive', 'black']
col_str=['dataset', 'memory', 'setting']

"""
Input Parameters
"""
parser=argparse.ArgumentParser(\
  description='Draw total execution time comparison')
parser.add_argument("filelist", \
  help='list for input files, seperated by "," ')
parser.add_argument("labellist", \
  help='list for labels, sperated by "," ')
parser.add_argument("datalist", \
  help='list for dataset, seperated by "," ')
parser.add_argument("xticklist", \
  help='list for xticket, seperated by "," ')
parser.add_argument("upperlist", \
  help='list for upper bound in datalist, seperated by "," ')
parser.add_argument("outfile", \
  help='output file name')
parser.add_argument('--indir', nargs=1, \
  default=['../data/comet/'], help='directory for input files \
  (defualt: ../data/comet/)')
parser.add_argument('--outdir', nargs=1, \
  default=['../figures/'], help='directory for output files \
  (defualt: ../figures)')
parser.add_argument('--plottype', nargs=1, \
  default=['point'], help='type of plot (bar, point)')
parser.add_argument('--xlabelname', nargs=1, default=['dataset size'], \
   help='name of x axis')
parser.add_argument('--colorlist', nargs=1, \
  default=["red,blue,green,yellow,cyan,lightgreen,lightblue,olive,orange"], \
  help='list for colors, seperated by ","')
parser.add_argument('--ylim', metavar='int', type=float, nargs=2, \
  default=[0.0,5.3], help='range in y axis')
args = parser.parse_args()

print args

filelist=args.filelist.split(',')
labellist=args.labellist.split(',')
datasets=args.datalist.split(',')
xticklist=args.xticklist.split(',')
upperlist=args.upperlist.split(',')
colorlist=args.colorlist[0].split(',')

"""
Draw figure
"""
def draw_figure():
  """
  Get figure data
  """
  fig_data=DataFrame(columns=col_str)
  idx=0
  print filelist
  for filename in filelist:
    print 'read data from ',filename
    data=pd.read_csv(args.indir[0]+filename)
    for dataset in datasets:
      if dataset==upperlist[idx]:
        break
      print 'read data ',dataset
      item_data=data[data['dataset']==dataset]
      max_index=int(item_data['index'].max())
      for i in range(0,max_index+1):
        index_data=item_data[item_data['index']==i]
        if index_data['peakmem'].iloc[0] > 0:
          item_data=index_data
          break
      max_index=len(item_data.index)
      for i in range(0,max_index):
        fig_data.loc[len(fig_data)]=[dataset, item_data['peakmem'].iloc[i]/1024/1024, labellist[idx]] 
    idx+=1

  #print fig_data
  """
  Draw figure
  """ 
  sns.set_style("ticks")
  ax=sns.barplot(x='dataset', y='memory', hue='setting', \
    data=fig_data, palette=colorlist, linewidth=1)

  """
  Set figure property
  """ 
  ax.tick_params(labelsize=18)
  ax.set_xticklabels(xticklist)
  ax.legend(loc=2,prop={'size':12}, ncol=2) 
  #ax.legend().set_visible(False)
  ax.set_xlabel(args.xlabelname[0], fontsize=18, fontweight="bold")
  ax.set_ylabel("peak memory usage per process (GB)", \
    fontsize=18, fontweight="bold")
  ax.set_ylim(args.ylim)

  """
  Save figure
  """ 
  plt.tight_layout()
  plt.savefig(args.outdir[0]+args.outfile)

if __name__ == "__main__":
  draw_figure()

