#!/bin/bash

python draw_mimir_mrmpi_memory_and_time.py wordcount uniform \
    singlenode 512M,1G,2G,4G,8G,16G,32G,64G 32G,128G,8G,8G \
    512M,1G,2G,4G,8G,16G,32G,64G \
    --indir1 ../../data/comet/wc_uniform_singlenode_c64M-p64M-i512M-h20/ \
    --indir2  ../../data/mrmpi/comet/ \
    --outfile comet-singlenode-wc-uniform-combine-memory-time.pdf \
    --settings1  "basic,cb" \
    --settings2  "p512,p512compress" \
    --labellist1 "Mimir,Mimir (cb)" \
    --labellist2 "MR-MPI,MR-MPI (cb)" \
    --memcolor  "coral,yellow,lightblue,lightgreen" \
    --timecolor "darkviolet,red,blue,green" \
    --hatches "x+/o" \
    --markerlist "*,^,v,o" \
    --xlabelname "dataset size" \
    --memlim 0 8 \
    --timelim -40 200

python draw_mimir_mrmpi_memory_and_time.py wordcount wikipedia \
    singlenode 512M,1G,2G,4G,8G,16G,32G,64G 32G,128G,8G,8G \
    512M,1G,2G,4G,8G,16G,32G,64G \
    --indir1 ../../data/comet/wc_wikipedia_singlenode_c64M-p64M-i512M-h20/ \
    --indir2  ../../data/mrmpi/comet/ \
    --outfile comet-singlenode-wc-wikipedia-combine-memory-time.pdf \
    --settings1 "basic,cb" \
    --settings2 "p512,p512compress" \
    --labellist1 "Mimir,Mimir (cb)" \
    --labellist2 "MR-MPI,MR-MPI (cb)" \
    --memcolor  "coral,yellow,lightblue,lightgreen" \
    --timecolor "darkviolet,red,blue,green" \
    --hatches "x+/o" \
    --markerlist "*,^,v,o" \
    --xlabelname "dataset size" \
    --memlim 0 8 \
    --timelim -20 140

#python draw_mimir_memory_and_time.py bfs graph500 \
#    singlenode s20,s21,s22,s23,s24,s25,s26,s27 \
#    "2^20,2^21,2^22,2^23,2^24,2^25,2^26,2^27" \
#    --indir ../../data/comet/bfs_graph500_singlenode_c64M-p64M-i512M-h20/ \
#    --outfile comet-singlenode-bfs-graph500-features-time.pdf \
#    --settings  "basic,kvhint,cbkvhint" \
#    --labellist "Mimir (basic),Mimir (hint),Mimir (cb;hint)" \
#    --memcolor  "coral,lightblue,lightgreen" \
#    --timecolor "darkviolet,blue,green" \
#    --markerlist "*,v,o" \
#    --xlabelname "number of vertexes" \
#    --memlim 0 6 \
#    --timelim 0 200

python draw_mimir_mrmpi_memory_and_time.py octree 1S \
     singlenode p25,p26,p27,p28,p29,p30,p31,p32 p31,p33,p29,p29\
    "2^25,2^26,2^27,2^28,2^29,2^30,2^31,2^32" \
    --indir1 ../../data/comet/octree_1S_singlenode_c64M-p64M-i512M-h20/ \
    --indir2  ../../data/mrmpi/comet/ \
    --outfile comet-singlenode-octree-1S-combine-memory-time.pdf \
    --settings1  "basic,cb" \
    --settings2  "p512,p512compress" \
    --labellist1 "Mimir,Mimir(cb)" \
    --labellist2 "MR-MPI,MR-MPI (cb)" \
    --memcolor  "coral,yellow,lightblue,lightgreen" \
    --timecolor "darkviolet,red,blue,green" \
    --hatches "x+/o" \
    --markerlist "*,^,v,o" \
    --xlabelname "number of points" \
    --memlim 0 8 \
    --timelim -10 350
