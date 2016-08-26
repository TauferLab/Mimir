#!/bin/bash
benchmark="bfs"
datasets="graph500"

for dataset in $datasets
do
  python compare_total_memory_usage.py \
    "mrmpi-p64-$benchmark-$dataset-onenode_a2a.ppn24_maxmem.txt,\
mrmpi-p512-$benchmark-$dataset-onenode_a2a.ppn24_maxmem.txt"\
    "MR-MPI(64M page size),MR-MPI(512M page size)"\
  "32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G" "32G,32G,32G,64G,128G"\
  comet-onenode-$benchmark-$dataset-original-maxmem 24 1 --indir ../raw_data/comet/maxmem/ --top off

  python compare_total_memory_usage.py \
    "mrmpi-p64-$benchmark-$dataset-onenode_a2a.ppn24_maxmem.txt,\
mrmpi-p512-$benchmark-$dataset-onenode_a2a.ppn24_maxmem.txt,\
mtmrmpi-basic-$benchmark-$dataset-onenode_c64M-b64M-i512M-h17-a2a.ppn24_maxmem.txt,\
mtmrmpi-partreduce-$benchmark-$dataset-onenode_c64M-b64M-i512M-h17-a2a.ppn24_maxmem.txt,\
mtmrmpi-partreducekvhint-$benchmark-$dataset-onenode_c64M-b64M-i512M-h17-a2a.ppn24_maxmem.txt"\
    "MR-MPI(64M page size),\
MR-MPI(512M page size),\
MR-MPI++,\
MR-MPI++(partial-reduction),\
MR-MPI++(partial-reduction;KV-hint)" \
  "32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G" "1G,8G,32G,64G,128G"\
  comet-onenode-$benchmark-$dataset-maxmem 24 1 --indir ../raw_data/comet/maxmem/ --top off
done
