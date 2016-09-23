#!/bin/bash
benchmark="bfs"
datasets="graph500s16"

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
    #python compare_total_memory_usage.py \
    #  "mrmpi-p64-$benchmark-$dataset-onenode_a2a.ppn24_phases.txt,\
#mrmpi-p512-$benchmark-$dataset-onenode_a2a.ppn24_phases.txt"\
#      "MR-MPI(64M page size),MR-MPI(512M page size)"\
#      "32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G"\
#      "16,17,18,19,20,21,22,23,24,25,26,27" \
#      "32G,32G,32G,64G,128G"\
#    comet-onenode-$benchmark-$dataset-original-maxmem 

    python compare_total_memory_usage.py \
      "mrmpi-p64-$benchmark-$dataset-onenode_a2a.ppn24_phases.txt,\
mrmpi-p64compress-$benchmark-$dataset-onenode_a2a.ppn24_phases.txt,\
mrmpi-p512-$benchmark-$dataset-onenode_a2a.ppn24_phases.txt,\
mrmpi-p512compress-$benchmark-$dataset-onenode_a2a.ppn24_phases.txt,\
mtmrmpi-basic-$benchmark-$dataset-onenode_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt,\
mtmrmpi-cps-$benchmark-$dataset-onenode_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt,\
mtmrmpi-cpskvhint-$benchmark-$dataset-onenode_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt"\
      "MR-MPI(64M page),\
MR-MPI(Compress; 64M page),\
MR-MPI(512M page),\
MR-MPI(Compress; 512M page),\
Mimir,\
Mimir(Compress),\
Mimir(Compress;KV-hint)" \
    "64K,128K,256K,512K,1M,2M,4M,8M,16M,32M,64M,128M"\
    "16,17,18,19,20,21,22,23,24,25,26,27" \
    "2M,2M,16M,16M,128M,128M,128M"\
    comet-onenode-$benchmark-$dataset-maxmem --xlabelname "number of vertex (2^x)"  --ylim 0 6
  done
fi

if [ $datatype == "weekscale512M" ];then
  for dataset in $datasets
  do
    python compare_total_memory_usage.py \
      mrmpi-p64-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mrmpi-p512-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mtmrmpi-basic-$benchmark-$dataset-$datatype"_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt",\
mtmrmpi-partreduce-$benchmark-$dataset-$datatype"_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt",\
mtmrmpi-partreducekvhint-$benchmark-$dataset-$datatype"_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt"\
      "MR-MPI(64M page size),\
MR-MPI(512M page size),\
MR-MPI++,\
MR-MPI++(partial-reduction),\
MR-MPI++(partial-reduction;KV-hint)" \
    "512M,1G,2G,4G,8G,16G,32G" \
    "64G,64G,64G,64G,64G"\
    "0.5,0.5,0.5,0.5,0.5,0.5,0.5"\
    comet-$datatype-$benchmark-$dataset-maxmem 24 1 --top on
  done
fi

if [ $datatype == "weekscale4G" ];then
  for dataset in $datasets
  do
    python compare_total_memory_usage.py \
      mrmpi-p64-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mrmpi-p512-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mtmrmpi-basic-$benchmark-$dataset-$datatype"_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt",\
mtmrmpi-cps-$benchmark-$dataset-$datatype"_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt",\
mtmrmpi-cpskvhint-$benchmark-$dataset-$datatype"_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt"\
      "MR-MPI(64M page size),\
MR-MPI(512M page size),\
MR-MPI++,\
MR-MPI++(partial-reduction),\
MR-MPI++(partial-reduction;KV-hint)" \
    "4G,8G,16G,32G,64G,128G,256G" \
    "64G,64G,64G,64G,64G"\
    "4,4,4,4,4,4,4"\
    comet-$datatype-$benchmark-$dataset-maxmem 24 1 --top on
  done
fi
