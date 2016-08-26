#!/usr/bin/python
import sys,os
import argparse
import math
import random
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import seaborn as sns
from pandas import Series, DataFrame

#Gsns.set_style("ticks") 

parser=argparse.ArgumentParser(description='Draw line of the unique words\' count.')
parser.add_argument("infile", help='input file')
parser.add_argument("outfile", help='output file')
parser.add_argument('--indir', nargs=1, default='../data', help='input directory')
parser.add_argument('--outdir', nargs=1, default='../figures', help='output directory')
parser.add_argument('--ylim', metavar='int', type=int, nargs=2, default=0, help='y low')
args = parser.parse_args()

print args

yrange=[args.ylim[0], args.ylim[1]]
print yrange

'''
main function
'''
def main():
  data=pd.read_csv(args.indir+'/'+args.infile, delimiter=' ')
  #data=data[['count']]
  #data=data.sort_values(by=['count'], ascending=False) 
  print len(data.index) 
  data=data.sample(10000) 
  data=data.sort_values(by=['count'], ascending=False) 
  #data=data[data.index%10==0]
  data.index=range(0,len(data.index))
  print data
  ax=sns.tsplot(data['count'], color="g")
  ax.tick_params(labelsize=16)
  ax.set_ylim(yrange) 
  ax.set_xlabel("unique word rank (10000 sample)", fontsize=18)
  ax.set_ylabel("number of unique word", fontsize=18)
  plt.tight_layout()
  plt.savefig(args.outdir+'/'+args.outfile)

if __name__ == "__main__":
  main() 
