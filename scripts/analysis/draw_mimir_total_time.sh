#!/bin/bash

python draw_mimir_total_time.py wordcount wikipedia \
    weakscale4G 8G,16G,32G,64G,128G,256G \
    2,4,8,16,32,64 \
    --indir ../../data/comet/wc_wikipedia_weakscale4G_c64M-p64M-i512M-h20/ \
    --outfile comet-weakscale4G-wc-wikipedia-features-time.pdf \
    --settings "basic,kvhint,cbkvhint" \
    --labellist "Mimir (basic),Mimir (hint),Mimir (cb;kvhint)" \
    --colorlist  "darkviolet,blue,green" \
    --markerlist "*,v,o" \
    --xlabelname "number of nodes" \
    --ylim 0 1000 


python draw_mimir_total_time.py bfs graph500 \
    weakscale23 s24,s25,s26,s27,s28,s29 \
    "2,4,8,16,32,64" \
    --indir ../../data/comet/bfs_graph500_weakscale23_c64M-p64M-i512M-h20/ \
    --outfile comet-weakscale23-bfs-graph500-features-time.pdf \
    --settings "basic,kvhint,cbkvhint" \
    --labellist "Mimir (basic),Mimir (hint),Mimir (cb;kvhint)" \
    --colorlist  "darkviolet,blue,green" \
    --markerlist "*,v,o" \
    --xlabelname "number of nodes" \
    --ylim 0 1000 

python draw_mimir_total_time.py octree 1S \
    weakscale28 p29,p30,p31,p32,p33,p34 \
    "2,4,8,16,32,64" \
    --indir ../../data/comet/octree_1S_weakscale28_c64M-p64M-i512M-h20/ \
    --outfile comet-weakscale28-octree-1S-features-time.pdf \
    --settings "basic,kvhint,cbkvhint" \
    --labellist "Mimir (basic),Mimir (hint),Mimir (cb;kvhint)" \
    --colorlist  "darkviolet,blue,green" \
    --markerlist "*,v,o" \
    --xlabelname "number of nodes" \
    --ylim 0 1000 


