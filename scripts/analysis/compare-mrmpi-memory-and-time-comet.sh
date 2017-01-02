#!/bin/bash

python compare-mrmpi-memory-and-time.py \
    --datalists \
        '256M,512M,1G,2G,4G,8G,16G' \
        '256M,512M,1G,2G,4G,8G,16G' \
        'p24,p25,p26,p27,p28,p29,p30' \
        's19,s20,s21,s22,s23,s24,s25,s26' \
    --config 'c64M-p64M-i512M-h20' \
    --mrmpiversion 'p64,p512' \
    --mimirversion 'basic' \
    --upperlists '32G,1G,8G' '32G,1G,8G' 'p31,p26,p29' 's27,s21,s24' \
    --mimirdirs \
        'comet/wc_uniform_singlenode_c64M-p64M-i512M-h20' \
        'comet/wc_wikipedia_singlenode_c64M-p64M-i512M-h20' \
        'comet/octree_1S_singlenode_c64M-p64M-i512M-h20' \
        'comet/bfs_graph500_singlenode_c64M-p64M-i512M-h20' \
    --outfile comet-baseline-cmp-memory-and-time.pdf \
    --xticklists \
        '256M,512M,1G,2G,4G,8G,16G' \
        '256M,512M,1G,2G,4G,8G,16G' \
        '2^24,2^25,2^26,2^27,2^28,2^29,2^30' \
        '2^19,2^20,2^21,2^22,2^23,2^24,2^25,2^26' \
    --labellist 'Mimir,MR-MPI (64M),MR-MPI (512M)'

python compare-mrmpi-memory-and-time.py \
    --datalists \
        '2G,4G,8G,16G,32G,64G' \
        '2G,4G,8G,16G,32G,64G' \
        'p27,p28,p29,p30,p31,p32' \
        's22,s23,s24,s25,s26' \
    --config 'c64M-p64M-i512M-h20' \
    --mrmpiversion 'p512,p512compress' \
    --mimirversion 'basic,cb' \
    --upperlists '32G,128G,8G,8G' '32G,128G,8G,8G' \
        'p31,p33,p29,p29' 's27,s27,s24,s24' \
    --mimirdirs \
        'comet/wc_uniform_singlenode_c64M-p64M-i512M-h20' \
        'comet/wc_wikipedia_singlenode_c64M-p64M-i512M-h20' \
        'comet/octree_1S_singlenode_c64M-p64M-i512M-h20' \
        'comet/bfs_graph500_singlenode_c64M-p64M-i512M-h20' \
    --outfile comet-combine-cmp-memory-and-time.pdf \
    --xticklists \
        '2G,4G,8G,16G,32G,64G' \
        '2G,4G,8G,16G,32G,64G' \
        '2^27,2^28,2^29,2^30,2^31,2^32' \
        '2^22,2^23,2^24,2^25,2^26,2^27' \
    --labellist 'Mimir,Mimir (cb),MR-MPI,MR-MPI (cps)' \
    --linelims -40 200 -40 150 -60 350 -40 150 \

python compare-mrmpi-memory-and-time.py \
    --datalists \
        '2G,4G,8G,16G,32G' \
        '2G,4G,8G,16G,32G' \
        'p27,p28,p29,p30,p31' \
        's22,s23,s24,s25,s26' \
    --config 'c64M-p64M-i512M-h20' \
    --mrmpiversion '' \
    --mimirversion 'basic,kvhint' \
    --upperlists '32G,32G' '32G,32G' \
        'p31,p31' 's27,s27' \
    --mimirdirs \
        'comet/wc_uniform_singlenode_c64M-p64M-i512M-h20' \
        'comet/wc_wikipedia_singlenode_c64M-p64M-i512M-h20' \
        'comet/octree_1S_singlenode_c64M-p64M-i512M-h20' \
        'comet/bfs_graph500_singlenode_c64M-p64M-i512M-h20' \
    --outfile comet-kvhint-cmp-memory-and-time.pdf \
    --xticklists \
        '2G,4G,8G,16G,32G' \
        '2G,4G,8G,16G,32G' \
        '2^27,2^28,2^29,2^30,2^31' \
        '2^22,2^23,2^24,2^25,2^26' \
    --labellist 'Mimir,Mimir (kvhint)' \
    --linelims 0 80 0 150 0 300 0 150

