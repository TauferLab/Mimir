#!/usr/bin/python
import sys,os
import argparse
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
col_str=['dataset', 'testtime', 'second', 'setting']

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
parser.add_argument('--marklist', nargs=1, \
  default=["x,+,o,s,D,d,h"], \
  help='list for markers, seperated by ","')
parser.add_argument('--ylim', metavar='int', type=int, nargs=2, \
  default=[0.0,0.0], help='range in y axis')
args = parser.parse_args()

print args

filelist=args.filelist.split(',')
labellist=args.labellist.split(',')
datasets=args.datalist.split(',')
xticklist=args.xticklist.split(',')
upperlist=args.upperlist.split(',')
colorlist=args.colorlist[0].split(',')
marklist=args.marklist[0].split(',')

"""
Draw figures
"""
def draw_figure():

  """
  Get figure data
  """
  fig_data=DataFrame(columns=col_str)
  idx=0
  for filename in filelist:
    print "open input file ",filename
    data=pd.read_csv(args.indir[0]+filename)

    for dataset in datasets:
      if dataset==upperlist[idx]:
        break
      print "dataset ",dataset
      infile=filename
      item_data=data
      item_data=item_data[item_data['dataset']==dataset]
      if item_data.empty:
        continue
      max_index=int(item_data['index'].max())+1
      for i in range(0, max_index):
        index_data=item_data[item_data['index']==i]
        mytime=0.0
        if infile.find('mtmrmpi')!=-1:
          mytime=index_data['total'].mean()
        else:
          mytime=index_data['total'].mean()
        fig_data.loc[len(fig_data)]=[dataset,\
          index_data['testtime'].iloc[0],mytime,labellist[idx]]
    idx+=1

  """
  Draw figures
  """
  sns.set_style("ticks")
  if args.plottype[0]=='point':
    ax=sns.pointplot(x='dataset', y='second', hue='setting', \
      data=fig_data, palette=colorlist, markers=marklist, dodge=True, linestyles='-')
  elif args.plottype[0]=='bar':
    ax=sns.barplot(x='dataset', y='second', hue='setting', \
      data=fig_data, palette=colorlist)
  elif args.plottype[0]=='box':
    ax=sns.boxplot(x='dataset', y='second', hue='setting', \
      data= fig_data, palette=colorlist, linewidth=0.1)
  else:
    print "plot type error!"

  """
  Set figure properties
  """ 
  ax.set_ylim(args.ylim)
  ax.tick_params(labelsize=18)
  ax.legend(loc=2,prop={'size':12},ncol=2)
  ax.set_xlabel(args.xlabelname[0], fontsize=18, fontweight="bold")
  ax.set_ylabel("execution time (seconds)", \
    fontsize=18, fontweight="bold")

  """
  Save figure
  """
  plt.tight_layout()
  plt.savefig(args.outdir[0]+args.outfile)

if __name__ == "__main__":
  draw_figure()

