#!/usr/bin/python
import sys,os
import argparse
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
col_str=['dataset', 'second', 'setting']

parser=argparse.ArgumentParser(description='Draw time compare\' count.')
parser.add_argument("filelist", help='input file file list')
parser.add_argument("labellist", help='input file label list')
parser.add_argument("datalist", help='dataset list')
parser.add_argument("upperlist", help='upper list')
parser.add_argument("outfile", help='output file')
parser.add_argument("ppn", type=int, help='process per node')
parser.add_argument("nnode", type=int, help='number of node')
parser.add_argument('--indir', nargs=1, default=['../data/comet/'], help='input directory')
parser.add_argument('--outdir', nargs=1, default=['../figures/'], help='output directory')
parser.add_argument('--plottype', nargs=1, default='point', help='plot type')
parser.add_argument('--xtickettype', nargs=1, default='dataset', help='x ticket type')
parser.add_argument('--ylim', metavar='int', type=int, nargs=2, default=0, help='y low')
parser.add_argument('--coloroff', metavar='int', type=int, nargs=1, default=0, help='offset in color list')
args = parser.parse_args()

print args

filelist=args.filelist.split(',')
labellist=args.labellist.split(',')
datasets=args.datalist.split(',')
upperlist=args.upperlist.split(',')
outfile=args.outfile
ppn=args.ppn
node=args.nnode
plottype=args.plottype[0]
xtickettype=args.xtickettype[0]
yrange=[args.ylim[0], args.ylim[1]]
indir=args.indir[0]
outdir=args.outdir[0]
#coloroff=args.coloroff[0]
#if coloroff !=0 :
#  colorlist=colorlist[coloroff:]

print filelist

def draw_figure():
  fig_data=DataFrame(columns=col_str)

  idx=0
  for filename in filelist:
    if filename.find("-mt-")!=-1:
      ppn=2
    #prefix,suffix=filename.split('*')
    print "open input file ",filename
    data=pd.read_csv(indir+filename)

    for dataset in datasets:
      if dataset==upperlist[idx]:
        break
      print "dataset ",dataset
      infile=filename
      #if os.path.isfile(indir+infile)==False:
      #  break
      #item_data=data[data['size']==ppn*node]
      item_data=data
      item_data=item_data[item_data['dataset']==dataset]
      #print item_data
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
          #mytime=index_data['map'].add(index_data['comm'])\
          #  .add(index_data['convert'])\
          #  .add(index_data['reduce']).mean()
        fig_data.loc[len(fig_data)]=[dataset, mytime, labellist[idx]]
    idx+=1

  print fig_data
  sns.set_style("ticks")
  if plottype=='point':
    ax=sns.pointplot(x='dataset', y='second', hue='setting', \
      data=fig_data, palette=colorlist)
  elif plottype=='bar':
    ax=sns.barplot(x='dataset', y='second', hue='setting', \
      data=fig_data, palette=colorlist)
  elif plottype=='box':
    ax=sns.boxplot(x='dataset', y='second', hue='setting', \
      data= fig_data, palette=colorlist)
  #unique=data.dataset.unique()
  #xticketlist=[i*i for i in range(unique)]
  #print xticketlist
  ax.set_ylim(yrange)
  #print ax.get_xtickets()
  #[for i*i in range(0,len(ax.get_xtickets()))]
  if xtickettype=='node':
    #ax.set_xticket([1,2,4,8,16,32,64])
    unique=data.dataset.unique()
    ax.set_xticklabels([2**i for i in range(0,len(unique))])
    ax.set_xlabel("number of node", fontsize=18)
  elif xtickettype=='dataset':
    ax.set_xlabel("dataset size", fontsize=18)
  ax.tick_params(labelsize=16)
  ax.set_ylabel("execution time (seconds)", fontsize=18)
  plt.tight_layout()
  print outdir+outfile
  plt.savefig(outdir+outfile)

'''
main function
'''
def main():
  draw_figure()

if __name__ == "__main__":
  main() 
