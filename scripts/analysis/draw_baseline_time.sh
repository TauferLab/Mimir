#!/bin/bash

python draw_mimir_mrmpi_time.py wordcount uniform \
    weakscale512M 1G,2G,4G,8G,16G,32G 64G,64G,64G \
    2,4,8,16,32,64 \
    --indir1 ../../data/comet/wc_uniform_weakscale512M_c64M-p64M-i512M-h20/ \
    --indir2  ../../data/mrmpi/comet/ \
    --outfile comet-weakscale512M-wc-uniform-baseline-time.pdf \
    --settings1  "basic" \
    --settings2  "p64,p512" \
    --labellist1 "Mimir (basic)" \
    --labellist2 "MR-MPI (64M),MR-MPI (512M)" \
    --colorlist "darkviolet,red,blue" \
    --hatches "x+/" \
    --markerlist "*,^,v" \
    --xlabelname "number of node" \
    --ylim 0 50

python draw_mimir_mrmpi_time.py wordcount wikipedia \
    weakscale512M 1G,2G,4G,8G,16G,32G 64G,16G\
    2,4,8,16,32,64 \
    --indir1 ../../data/comet/wc_wikipedia_weakscale512M_c64M-p64M-i512M-h20/ \
    --indir2  ../../data/mrmpi/comet/ \
    --outfile comet-weakscale512M-wc-wikipedia-baseline-time.pdf \
    --settings1 "basic" \
    --settings2 "p512" \
    --labellist1 "Mimir (basic)" \
    --labellist2 "MR-MPI (512M)" \
    --colorlist "darkviolet,red,blue" \
    --hatches "x+/" \
    --markerlist "*,^,v" \
    --xlabelname "number of node" \
    --ylim 0 50

#python draw_mimir_mrmpi_time.py bfs graph500 \
#    weakscale20 s21,s22,s23,s24,s25,s26  s27,s27,s27\
#    2,4,8,6,32,64 \
#    --indir1 ../../data/comet/bfs_graph500_weakscale20_c64M-p64M-i512M-h20/ \
#    --indir2  ../../data/mrmpi/comet/ \
#    --outfile comet-weakscale512M-bfs-graph500-baseline-time.pdf \
#    --settings1  "basic" \
#    --settings2  "p64,p512" \
#    --labellist1  "Mimir (basic)" \
#    --labellist2  "MR-MPI (64M),MR-MPI (512M)" \
#    --colorlist "darkviolet,red,blue" \
#    --hatches "x+/" \
#    --markerlist "*,^,v" \
#    --xlabelname "number of vertexes" \
#    --ylim -20 100

#python draw_mimir_mrmpi_time.py octree 1S \
#    weakscale p24,p25,p26,p27,p28,p29,p30 p31,p26,p29\
#    "2^24,2^25,2^26,2^27,2^28,2^29,2^30" \
#    --indir1 ../../data/comet/octree_1S_singlenode_c64M-p64M-i512M-h20/ \
#    --indir2  ../../data/mrmpi/comet/ \
#    --outfile comet-singlenode-octree-1S-baseline-memory-time.pdf \
#    --settings1  "basic" \
#    --settings2  "p64,p512" \
#    --labellist1 "Mimir (basic)" \
#    --labellist2 "MR-MPI (64M),MR-MPI (512M)" \
#    --memcolor  "coral,yellow,lightblue" \
#    --timecolor "darkviolet,red,blue" \
#    --hatches "x+/" \
#    --markerlist "*,^,v" \
#    --xlabelname "number of points" \
#    --memlim 0 8 \
#    --timelim -20 150
