#!/bin/bash

#python draw_mimir_memory_and_time.py wordcount wikipedia \
#    singlenode 32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G \
#    --settings "basic,kvhint,cb,cbkvhint" \
#    --indir ../../data/comet/wc_wikipedia_singlenode_c64M-p64M-i512M-h20/ \
#    --outfile comet-singlenode-wc-wikipedia-features-time.pdf

python draw_mimir_memory_and_time.py bfs graph500 \
    singlenode s20,s21,s22,s23,s24,s25,s26,s27 \
    --settings "basic,kvhint,cb,cbkvhint" \
    --indir ../../data/comet/bfs_graph500_singlenode_c64M-p64M-i512M-h20/ \
    --outfile comet-singlenode-bfs-graph500-features-time.pdf

#python draw_mimir_total_time.py octree 1S \
#    singlenode p24,p25,p26,p27,p28,p29,p30,p31,p32 \
#    --settings "basic,kvhint,cb,cbkvhint" \
#    --indir ../../data/comet/octree_1S_singlenode_c64M-p64M-i512M-h20/ \
#    --outfile comet-singlenode-octree-1S-features-time.pdf
