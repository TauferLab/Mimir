#!/bin/python

import sys, os, glob

statdir = sys.argv[1]
indir = sys.argv[2]

minx = []
maxx = []
miny = []
maxy = []
minz = []
maxz = []
counts = []
results = []

print statdir

for filename in glob.glob(statdir):
    print filename
    with open(filename, "r") as ins:
        for line in ins:
            items = line.split(' ')
            X=items[0][3:-1].split(',')
            minx.append(float(X[0]))
            maxx.append(float(X[1]))
            Y=items[1][3:-1].split(',')
            miny.append(float(Y[0]))
            maxy.append(float(Y[1]))
            Z=items[2][3:-1].split(',')
            minz.append(float(Z[0]))
            maxz.append(float(Z[1]))
            counts.append(int(items[3]))

for i in range(0, len(counts)):
    results.append(0)

for filename in glob.glob(indir+'/'+'*'):
    print filename
    with open(filename, "r") as ins:
        for line in ins:
            items = line.split(' ')
            #print items
            X = float(items[1])
            Y = float(items[2])
            Z = float(items[3])
            for i in range(0, len(counts)):
                if X > minx[i] and X <= maxx[i]       \
                    and Y > miny[i] and Y <= maxy[i]  \
                    and Z > minz[i] and Z <= maxz[i]:
                        results[i] += 1

print "check results start"

print counts
print results

for i in range(0, len(counts)):
    if counts[i] != results[i]:
        print "Does not match" + str(i)
