#!/bin/bash
benchmark="octree"
datasets="1S"



for dataset in $datasets
do
  python compare_total_time.py \
    "mrmpi-p64-$benchmark-$dataset-onenode_d0.01-a2a.ppn24_phases.txt,\
mrmpi-p512-$benchmark-$dataset-onenode_d0.01-a2a.ppn24_phases.txt,\
mtmrmpi-basic-$benchmark-$dataset-onenode_d0.01-c64M-b64M-i512M-h17-a2a.ppn24_phases.txt,\
mtmrmpi-partreduce-$benchmark-$dataset-onenode_d0.01-c64M-b64M-i512M-h17-a2a.ppn24_phases.txt,\
mtmrmpi-partreducekvhint-$benchmark-$dataset-onenode_d0.01-c64M-b64M-i512M-h17-a2a.ppn24_phases.txt"\
    "MR-MPI(64M page size),\
MR-MPI(512M page size),\
MR-MPI++,\
MR-MPI++(partial-reduction),\
MR-MPI++(partial-reduction;KV-hint)" \
  "1G,2G,4G,8G,16G,32G,64G,128G,256G,512G,1T,2T" "4T,4T,4T,4T,4T"\
  comet-onenode-$benchmark-$dataset-time 24 1 --indir ../data/comet/ --plottype bar --ylim 0 5000
done
