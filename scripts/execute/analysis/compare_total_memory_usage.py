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

'''
Constant variables
'''
#curdir=os.path.dirname(os.path.realpath(__file__))
colorlist=['red', 'blue', 'green', 'yellow', 'cyan', 'lightgreen', 'lightblue', 'olive']
col_str=['dataset', 'memory', 'setting']

parser=argparse.ArgumentParser(description='Draw time compare\' count.')
parser.add_argument("filelist", help='input file file list')
parser.add_argument("labellist", help='input file label list')
parser.add_argument("datalist", help='dataset list')
parser.add_argument("upperlist", help='upper list')
parser.add_argument("outfile", help='output file')
parser.add_argument("ppn", type=int, help='process per node')
parser.add_argument("nnode", type=int, help='number of node')
parser.add_argument('--indir', nargs=1, default=['../data/'], help='input directory')
parser.add_argument('--outdir', nargs=1, default=['../figures/'], help='output directory')
parser.add_argument('--top', nargs=1, default=['off'], help='y low')
args = parser.parse_args()

print args

filelist=args.filelist.split(',')
labellist=args.labellist.split(',')
datasets=args.datalist.split(',')
upperlist=args.upperlist.split(',')
outfile=args.outfile
ppn=args.ppn
node=args.nnode
indir=args.indir[0]
outdir=args.outdir[0]
top=args.top[0]

#print "input dir:"

datasizes=[]
for dataset in datasets:
  datasize=0
  if dataset[-1:]=='M':
    datasize=float(dataset[0:-1])*1024*1024
  elif dataset[-1:]=='G':
    datasize=float(dataset[0:-1])*1024*1024*1024
  datasizes.append(datasize)

def draw_figure():
  fig_data=DataFrame(columns=col_str)

  idx=0
  print filelist
  for filename in filelist: 
    print 'read data from ',filename
    data=pd.read_csv(indir+filename)
    #print data 
    for dataset in datasets:
      if dataset==upperlist[idx]:
        break
      print 'read data ',dataset
      item_data=data[data['dataset']==dataset]
      #print item_data
      max_index=len(item_data.index)
      #print max_index
      #print idx
      for i in range(0,max_index):
        fig_data.loc[len(fig_data)]=[dataset, item_data['maxmem'].iloc[i]/1024/1024, labellist[idx]]
    idx+=1

  #print fig_data
  sns.set_style("ticks")
  ax=sns.barplot(x='dataset', y='memory', hue='setting', \
    data=fig_data, palette=colorlist, linewidth=1)

  if top=='on':
    i=0
    for p in ax.patches:
      height = p.get_height()
      if math.isnan(height):
        i=0
        continue
      #print datasizes
      elif i==len(datasizes):
        i=0
      #print i
      ax.text(p.get_x(), height+0.6, '%2.2f%%'%(datasizes[i]/1024/1024/1024/(ppn*node)/height*100), rotation=90)
      i += 1

  ax.tick_params(labelsize=16)
  ax.set_xlabel("dataset size", fontsize=18)
  ax.set_ylabel("maximum heap memory size (GB)", fontsize=18)
  ax.set_ylim([0, 5.3])
  plt.tight_layout()
  print outdir
  print outfile
  plt.savefig(outdir+outfile)

'''
main function
'''
def main():
  draw_figure()

if __name__ == "__main__":
  main()
