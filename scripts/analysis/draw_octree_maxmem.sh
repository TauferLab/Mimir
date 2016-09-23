#!/bin/bash
benchmark="octree"
datasets="1S"

datatype=''
if [ $# == 1 ]
then
  datatype=$1
else
  echo "./exe datatype [onenode|weakscale512M|weakscale4G|weakscale64G]"
fi


if [ $datatype == "onenode" ];then
  for dataset in $datasets
  do
    python compare_total_memory_usage.py \
      "mrmpi-p64-$benchmark-$dataset-onenode_d0.01-a2a.ppn24_phases.txt,\
mrmpi-p512-$benchmark-$dataset-onenode_d0.01-a2a.ppn24_phases.txt,\
mtmrmpi-basic-$benchmark-$dataset-onenode_d0.01-c64M-b64M-i512M-h17-a2a.ppn24_phases.txt,\
mtmrmpi-partreduce-$benchmark-$dataset-onenode_d0.01-c64M-b64M-i512M-h17-a2a.ppn24_phases.txt,\
mtmrmpi-partreducekvhint-$benchmark-$dataset-onenode_d0.01-c64M-b64M-i512M-h17-a2a.ppn24_phases.txt"\
      "MR-MPI(64M page size),\
MR-MPI(512M page size),\
Mimir,\
Mimir(partial-reduction),\
Mimir(partial-reduction;KV-hint)" \
    "1G,2G,4G,8G,16G,32G,64G,128G,256G,512G,1T,2T" \
"1G,2G,4G,8G,16G,32G,64G,128G,256G,512G,1T,2T" "256G,2T,4T,4T,4T"\
    "0.006330013,0.012660027,0.025320053,0.050640106,0.101280212,0.202560424,0.405120848,0.810241696,1.620483392,3.240966784,6.481933568,12.963867136"\
    comet-onenode-$benchmark-$dataset-maxmem 24 1 --top on
  done
fi
