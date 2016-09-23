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
    python compare_total_time.py \
      "mrmpi-p64-$benchmark-$dataset-onenode_d0.01-a2a.ppn24_phases.txt,\
mrmpi-p64compress-$benchmark-$dataset-onenode_d0.01-a2a.ppn24_phases.txt,\
mrmpi-p512-$benchmark-$dataset-onenode_d0.01-a2a.ppn24_phases.txt,\
mrmpi-p512compress-$benchmark-$dataset-onenode_d0.01-a2a.ppn24_phases.txt"\
      "MR-MPI(64M page size),MR-MPI with compress(64M page size),MR-MPI(512M page size),MR-MPI with compress(512M page size)"\
    "16M,32M,64M,128M,256M,512M,1G,2G,4G" \
    "24,25,26,27,28,29,30,31,32" "1G,1G,1G,1G,1G"\
    comet-onenode-$benchmark-$dataset-original-time --indir ../data/comet/ --plottype bar --ylim 0 1000
  done

  datasets=""
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
Mimir,\
Mimir(partial-reduction),\
Mimir(partial-reduction;KV-hint)" \
    "1G,2G,4G,8G,16G,32G,64G,128G,256G,512G,1T,2T" \
    "1G,2G,4G,8G,16G,32G,64G,128G,256G,512G,1T,2T" "256G,2T,4T,4T,4T"\
    comet-onenode-$benchmark-$dataset-time 24 1 --indir ../data/comet/ --plottype bar --ylim 0 3500
  done
fi
