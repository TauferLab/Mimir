#!/bin/python
#
# (c) 2018 by The University of Tennessee Knoxville, Argonne National
#     Laboratory, San Diego Supercomputer Center, National University of
#     Defense Technology, National Supercomputer Center in Guangzhou,
#     and Sun Yat-sen University.
#
#     See COPYRIGHT in top-level directory.
#

import sys, os, glob
from collections import defaultdict

root = int(sys.argv[1])
nvertex = int(sys.argv[2])
retdir = sys.argv[3]
outfile = sys.argv[4]

parents=[-1]*nvertex
level=[-1]*nvertex
for filename in glob.glob(retdir+'/*'):
    with open(filename, "r") as ins:
        for line in ins:
            values = line.split(' ')
            v0 = int(values[0])
            v1 = int(values[1])
            parents[v0] = v1

if parents[root] != root:
    exit(1)

level[root] = 0
active_vertex = [root]
next_vertex = []
while active_vertex:
    for i in range(0,len(level)):
        if parents[i] in active_vertex and level[i] == -1:
            level[i] = level[parents[i]]+1
            next_vertex.append(i)
    active_vertex[:] = []
    for vertex in next_vertex:
        active_vertex.append(vertex)
    next_vertex[:] = []

of = open(outfile, 'w')
for i in range(0,len(level)):
    of.write(str(i)+' '+str(level[i])+'\n')
of.close()
