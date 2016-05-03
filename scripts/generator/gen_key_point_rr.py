#!/usr/bin/env python
import random
from math import log,ceil
from operator import itemgetter
import sys
import glob
import os

###############generate different data configurations###########################
###############python gen_key_point_rr.py key 1024 (number of nodes) 1 (0 0 0 100 (number of points per node) normal) ## if num cluster =2, repeat the pars in ()##
###########parse command line arguments##############
key_file_prefix=sys.argv[1]
num_nodes=int(sys.argv[2])
num_clusters=int(sys.argv[3])
print ("the file prefix is:", key_file_prefix)
print ("number of cluster is:", num_clusters)
index=4
cluster_centers=[]
points_num=[]
distr=[]
for i in range(num_clusters): ##0,1,,num_clusters-1
	x=float(sys.argv[index])
	y=float(sys.argv[index+1])
	z=float(sys.argv[index+2])
	points_num_int=int(sys.argv[index+3])
	dist=sys.argv[index+4]# uniform 2: normal
	index=index+5
    
	cluster_centers.append([x,y,z])	
	points_num.append(points_num_int)
	distr.append(dist)
print ("the cluster centers :",cluster_centers)
print ("the points number: ", points_num)
print ("the distribution: ",distr)

#sys.exit()

###############################functions###########################
def gen_key_from_coord(b0, b1, b2,range_min,range_max,num_digit):
	key=''
	count=1 #count how many digits in the key
	minx = range_min; miny = range_min; minz = range_min
	maxx = range_max; maxy = range_max; maxz = range_max
	while count <= num_digit:
        	m0=0
	        m1=0
	        m2=0
	        medx = minx + float((maxx - minx)/2)
                if b0>medx:
                        m0=1;
                        minx=medx;
                else:
                        maxx=medx;
                medy = miny + float((maxy-miny)/2)
                if b1>medy:
                        m1=1;
                        miny=medy;
                else:
                        maxy=medy;
                medz = minz + float((maxz-minz)/2)
                if b2>medz:
                        m2=1;
                        minz=medz;
                else:
                        maxz=medz;
                bit=m0+(m1*2)+(m2*4);
		key=key+`bit`
                count=count+1
	return key

#################end functions###################

#############gen octkeys with distribution into keyfile########################
num_digit=15
range_min=-4
range_max=4
stdiv=0.01

###################generate keys for each node (file)###############


###################generate coordinates for each point###########################################################


for cluster in cluster_centers: #if there are multiple clusters in the data
    print cluster;
    num_points_incluster=points_num.pop(0);
    dist=distr.pop(0);
    
    
    for node_id in range(0,num_nodes): #0,1,...1023
        point_file=key_file_prefix+'points'+`node_id`+'.txt';
        point_fid=open(point_file,'a');
        key_file=key_file_prefix+'keys'+`node_id`+'.txt'; #index is the index of multiple cluster
        key_fid=open(key_file,'a');
    
        point_count=0;

        while point_count < num_points_incluster:
            coords=[]; #store the x y z coordinates in an array, for computing the key
            point_coord=''; #store the x,y,z coordiantes of a generated point, for printing in the file
            for coord in cluster:
                if dist == 'uniform':
                    dig=random.uniform(range_min,range_max);
                elif dist == 'normal':
                    dig=random.normalvariate(coord,stdiv);
                point_coord = point_coord+' '+`dig`;
                coords.append(dig);
            
            key=gen_key_from_coord(coords.pop(0), coords.pop(0), coords.pop(0), range_min,range_max,num_digit); #key is a string
            point_fid.write(point_coord+'\n');
            key_fid.write(key+'\n');
            point_count=point_count+1;

        point_fid.close();
        key_fid.close();







