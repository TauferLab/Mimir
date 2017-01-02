#!/bin/bash

BASEDIR=/usa/taogao/repo/gclab/projects/mimir/data

python compare-mrmpi-time.py wordcount uniform \
    weakscale512M 1G,2G,4G,8G,16G,32G 64G,64G,64G \
    2,4,8,16,32,64 \
    --indir1 $BASEDIR/comet/wc_uniform_weakscale512M_c64M-p64M-i512M-h20/ \
    --indir2  $BASEDIR/mrmpi/comet/ \
    --outfile comet-weakscale512M-wc-uniform-time.pdf \
    --settings1  "basic" \
    --settings2  "p64,p512" \
    --labellist1 "Mimir (basic)" \
    --labellist2 "MR-MPI (64M),MR-MPI (512M)" \
    --colorlist "darkviolet,red,blue" \
    --hatches "x+/" \
    --markerlist "*,^,v" \
    --xlabelname "number of node" \
    --ylim 0 50

python compare-mrmpi-time.py wordcount wikipedia \
    weakscale512M 1G,2G,4G,8G,16G,32G 64G,16G\
    2,4,8,16,32,64 \
    --indir1 $BASEDIR/comet/wc_wikipedia_weakscale512M_c64M-p64M-i512M-h20/ \
    --indir2  $BASEDIR/mrmpi/comet/ \
    --outfile comet-weakscale512M-wc-wikipedia-time.pdf \
    --settings1 "basic" \
    --settings2 "p512" \
    --labellist1 "Mimir (basic)" \
    --labellist2 "MR-MPI (512M)" \
    --colorlist "darkviolet,blue" \
    --hatches "x+/" \
    --markerlist "*,v" \
    --xlabelname "number of node" \
    --ylim 0 50

python compare-mrmpi-time.py bfs graph500 \
    weakscale20 s21,s22,s23,s24,s25,s26  s27,s27,s27\
    2,4,8,6,32,64 \
    --indir1 $BASEDIR/comet/bfs_graph500_weakscale20_c64M-p64M-i512M-h20/ \
    --indir2  $BASEDIR/mrmpi/comet/ \
    --outfile comet-weakscale20-bfs-graph500-time.pdf \
    --settings1  "basic" \
    --settings2  "p64,p512" \
    --labellist1  "Mimir (basic)" \
    --labellist2  "MR-MPI (64M),MR-MPI (512M)" \
    --colorlist "darkviolet,red,blue" \
    --hatches "x+/" \
    --markerlist "*,^,v" \
    --xlabelname "number of vertexes" \
    --ylim -20 100

python compare-mrmpi-time.py octree 1S \
    weakscale25 p26,p27,p28,p29,p30,p31 p32,p27,p31\
    "2^26,2^27,2^28,2^29,2^30,2^31" \
    --indir1 $BASEDIR/comet/octree_1S_weakscale25_c64M-p64M-i512M-h20/ \
    --indir2  $BASEDIR/mrmpi/comet/ \
    --outfile comet-weakscale25-octree-1S-time.pdf \
    --settings1  "basic" \
    --settings2  "p64,p512" \
    --labellist1 "Mimir (basic)" \
    --labellist2 "MR-MPI (64M),MR-MPI (512M)" \
    --colorlist "darkviolet,red,blue" \
    --hatches "x+/" \
    --markerlist "*,^,v" \
    --xlabelname "number of points" \
    --ylim -20 150

