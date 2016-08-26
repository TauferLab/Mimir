import sys
import os
import glob
import argparse
import networkx as nx
import matplotlib.pyplot as plt

parser=argparse.ArgumentParser(description='Draw graphs figure.')
parser.add_argument("nvertex", help='number of vertex')
parser.add_argument("outfile", help='output file name')
parser.add_argument('--indir', nargs=1, default=['../data/comet/'], help='input directory')
parser.add_argument('--outdir', nargs=1, default=['../figures/'], help='output directory')
args = parser.parse_args()


nvertex=int(args.nvertex)
outfile=args.outfile
indir=args.indir[0]
outdir=args.outdir[0]

G=nx.Graph()

# a list of nodes:
G.add_nodes_from(range(0,nvertex))

filefilter="*";
print filefilter
for filename in glob.glob(indir+filefilter):
  print filename
  with open(filename) as f:
    for line in f:
      edge=line.split()
      #print xyz
      v0=int(edge[0])
      v1=int(edge[1])
      val=int(edge[2])
      G.add_edge(v0, v1)

nx.draw(G)
plt.savefig(outdir+outfile) # save as png
plt.show() # display
