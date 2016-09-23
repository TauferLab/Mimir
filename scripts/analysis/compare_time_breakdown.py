#!/usr/bin/python
import sys,os
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches

from pandas import Series, DataFrame

parser=argparse.ArgumentParser(description='Draw timebreakdown')
parser.add_argument("filename", help='input file file name')
parser.add_argument("labelname", help='input file label name')
parser.add_argument("datalist", help='dataset list')
parser.add_argument("xlist", help='x list')
parser.add_argument("outfile", help='output file')
parser.add_argument("ppn", type=int, help='process per node')
parser.add_argument("nnode", type=int, help='number of node')
parser.add_argument('--indir', nargs=1, default=['../data/comet/'], help='input directory')
parser.add_argument('--outdir', nargs=1, default=['../figures/'], help='output directory')
parser.add_argument("--normalize", nargs=1, default="false", help="normilized")
parser.add_argument("--datatype", nargs=1, default="mean", help="data type")
parser.add_argument('--plottype', nargs=1, default='point', help='plot type')
parser.add_argument('--xlabelname', nargs=1, default=['dataset size'], help='x label name')
parser.add_argument('--style', nargs=1, default='detail', help='plot style')
parser.add_argument('--colorlist', nargs=1, \
  default=["red,blue,green,yellow,cyan,lightgreen,lightblue,olive"], \
  help='list for colors, seperated by ","')
parser.add_argument('--ylim', metavar='int', type=int, nargs=2, default=0, help='y low')
args = parser.parse_args()

print args

colorlist=args.colorlist[0].split(',')
filename=args.filename
labename=args.labelname
datasets=args.datalist.split(',')
xlist=args.xlist.split(',')
outfile=args.outfile
ppn=args.ppn
node=args.nnode
normalize=args.normalize[0]
dtype=args.datatype[0]
plottype=args.plottype[0]
xlabelname=args.xlabelname[0]
style=args.style[0]
yrange=[args.ylim[0], args.ylim[1]]
indir=args.indir[0]
outdir=args.outdir[0]

print filename

fig_str=[]
if style=='detail':
  fig_str=[
    'Read input from lustre',
    'Read pages from local disk',
    'Write pages to local disk',
    'MPI_Alltoall', 'MPI_Alltoallv', 'MPI_Allreduce', 'computing']
elif style=='simple':
  fig_str=['Lustre IO', 'Local Disk IO', 'MPI Communcation', 'Computation']

"""
Draw figures
"""
def draw_figure():
  """
  Get figure data
  """
  fig_data=DataFrame(columns=fig_str)
  idx=0
  print "open input file ",filename
  data=pd.read_csv(indir+filename)

  """
  Draw figures
  """
  for dataset in datasets:
    print dataset
    infile=filename
    item_data=data
    #item_data=data[data['size']==ppn*node]
    item_data=item_data[item_data['dataset']==dataset]
    #print item_data
    index_result=DataFrame(columns=fig_str)
    max_index=int(item_data['index'].max())+1
    for i in range(0, max_index):
      index_data=item_data[item_data['index']==i]
      mytime=0.0
      if infile.find('mtmrmpi')!=-1:
        total_time=index_data['total'].mean()
        print total_time,index_data['testtime'].iloc[0]
        pfs_time=index_data['pfsopen'].add(index_data['pfsseek'])\
          .add(index_data['pfsread']).add(index_data['pfsclose']).mean()
        read_time=write_time=0
        alltoall=index_data['alltoall'].mean()
        alltoallv=index_data['alltoallv'].mean()
        allreduce=index_data['allreduce'].mean()
        if style=='detail':
          index_result.loc[len(index_result)]=[\
            pfs_time,read_time,write_time,\
            alltoall,alltoallv,allreduce,\
            total_time-pfs_time-read_time-\
            write_time-alltoall-alltoallv-allreduce]
          index_total=index_result.sum(axis=1)
        elif style=='simple':
          index_result.loc[len(index_result)]=[\
            pfs_time,read_time+write_time,\
            alltoall+alltoallv+allreduce,\
            total_time-pfs_time-read_time-\
            write_time-alltoall-alltoallv-allreduce]
          index_total=index_result.sum(axis=1)         
      else:
          total_time=index_data['total'].mean()
          print total_time,index_data['testtime'].iloc[0] 
          pfs_time=index_data['pfstime'].mean()
          read_time=index_data['rtime'].mean()
          write_time=index_data['wtime'].mean()
          alltoall=index_data['MPI_Alltoall'].mean()
          alltoallv=index_data['MPI_Alltoallv'].mean()
          allreduce=index_data['MPI_Allreduce'].mean()
          if style=='detail':
            index_result.loc[len(index_result)]=[\
              pfs_time,read_time,write_time,\
              alltoall,alltoallv,allreduce,\
              total_time-pfs_time-read_time-\
              write_time-alltoall-alltoallv-allreduce]
            index_total=index_result.sum(axis=1)
          elif style=='simple':
            index_result.loc[len(index_result)]=[\
              pfs_time,read_time+write_time,\
              alltoall+alltoallv+allreduce,\
              total_time-pfs_time-read_time-\
              write_time-alltoall-alltoallv-allreduce]
            index_total=index_result.sum(axis=1)
    idx=-1
    if dtype=='mean':
      fig_data.loc[len(fig_data)]=index_result.mean()
    elif dtype=='min':
      idx=index_total.idxmin()
      print index_total.ix[idx]
      fig_data.loc[len(fig_data)]=index_result.ix[idx]
    elif dtype=='max':
      idx=index_total.idxmax()
      print index_total.ix[idx]
      fig_data.loc[len(fig_data)]=index_result.ix[idx]
    elif dtype=='median':
      median=index_total.median()
      diff=index_total.apply(lambda z: abs(median-z))
      diff.sort()
      idx=diff.index[0]
      print index_total.ix[idx]
      fig_data.loc[len(fig_data)]=index_result.ix[idx]
  idx+=1

  fig_data.index=datasets
  if normalize=='true':
    fig_data=fig_data.transpose()
    fig_data=fig_data.apply(lambda x: x/x.sum())
    fig_data=fig_data.transpose()
  #print normalize
  print fig_data

  """
  Draw figures
  """
  ax=fig_data.plot(kind='bar', stacked=True, color=colorlist)

  """
  Set figure properties
  """ 
  if normalize=='true':
    ax.set_ylim([0,1])
    ax.set_ylabel("percentage", fontsize=18, fontweight="bold")
  else:
    ax.set_ylim(yrange)
    ax.set_ylabel("execution time (seconds)", fontsize=18, fontweight="bold")
  ax.set_xticklabels(xlist)
  ax.tick_params(labelsize=16)
  ax.set_xlabel(xlabelname, fontsize=18, fontweight="bold")

  """
  Save figure
  """
  plt.tight_layout()
  plt.savefig(outdir+outfile)

'''
main function
'''
def main():
  draw_figure()

if __name__ == "__main__":
  main() 
