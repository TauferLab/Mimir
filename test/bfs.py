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
graphdir = sys.argv[3]
outfile = sys.argv[4]

edges = defaultdict(list)

level = [-1] * nvertex

for filename in glob.glob(graphdir+'/*'):
    with open(filename, "r") as ins:
        for line in ins:
            vals = line.split(' ')
            v0 = int(vals[0])
            v1 = int(vals[1])
            if v0 != v1:
                edges[v0].append(v1)
                edges[v1].append(v0)

level[root] = 0
active_vertex = [root]
next_vertex = []
while active_vertex:
    for v0 in active_vertex:
        if v0 in edges:
            mv1 = edges[v0]
            for v1 in mv1:
                if level[v1] == -1:
                    level[v1] = level[v0] +1
                    next_vertex.append(v1)
    active_vertex[:] = []
    for vertex in next_vertex:
        active_vertex.append(vertex)
    next_vertex[:] = []

of = open(outfile, 'w')
for i in range(0,len(level)):
    of.write(str(i)+' '+str(level[i])+'\n')
of.close()
