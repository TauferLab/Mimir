import sys
import os
import glob
import argparse
import numpy as np
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt

parser=argparse.ArgumentParser(description='Draw 3d points figure.')
parser.add_argument("outfile", help='output file name')
parser.add_argument('--indir', nargs=1, default=['../data/comet/'], help='input directory')
parser.add_argument('--outdir', nargs=1, default=['../figures/'], help='output directory')
args = parser.parse_args()

outfile=args.outfile
indir=args.indir[0]
outdir=args.outdir[0]

fig = plt.figure()
ax = fig.add_subplot(111, projection='3d') 

filefilter="*.txt";
print filefilter
xs=[]
ys=[]
zs=[]
for filename in glob.glob(indir+filefilter):
  print filename
  with open(filename) as f:
    for line in f:
      xyz=line.split()
      #print xyz
      xs.append(float(xyz[0]))
      ys.append(float(xyz[1]))
      zs.append(float(xyz[2]))

ax.scatter(xs, ys, zs, c='r', marker='o')
ax.set_xlim([-4,4])
ax.set_ylim([-4,4])
ax.set_zlim([-4,4])
ax.set_xlabel('X Label', fontsize=18)
ax.set_ylabel('Y Label', fontsize=18)
ax.set_zlabel('Z Label', fontsize=18)
ax.tick_params(labelsize=16)
plt.tight_layout()
plt.savefig(outdir+outfile)
#plt.show()
