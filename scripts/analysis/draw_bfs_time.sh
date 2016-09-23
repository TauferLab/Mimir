#!/bin/bash
benchmark="bfs"
datasets="graph500s16"

datatype=''
if [ $# == 1 ]
then
  datatype=$1
else
  echo "./exe datatype[onenode|commsize|weekscale4G]"
fi

if [ $datatype == "onenode" ];then 
  for dataset in $datasets
  do
    python compare_total_time.py \
      "mrmpi-p64-$benchmark-$dataset-onenode_a2a.ppn24_phases.txt,\
mrmpi-p64compress-$benchmark-$dataset-onenode_a2a.ppn24_phases.txt,\
mrmpi-p512-$benchmark-$dataset-onenode_a2a.ppn24_phases.txt,\
mrmpi-p512compress-$benchmark-$dataset-onenode_a2a.ppn24_phases.txt,\
mtmrmpi-basic-$benchmark-$dataset-onenode_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt"\
      "MR-MPI(64M page),\
MR-MPI(Compress;64M page),\
MR-MPI(512M page),\
MR-MPI(Compress;512M page),Mimir"\
    "64K,128K,256K,512K,1M,2M,4M,8M,16M,32M,64M,128M" \
    "16,17,18,19,20,21,22,23,24,25,26,27" "64M,64M,64M,64M,64M"\
    comet-onenode-$benchmark-$dataset-original-time --indir ../data/comet/ --plottype bar --xlabelname "number of vertex (2^x)" --ylim 0 1000
  done

  #datasets=""
  for dataset in $datasets
  do
    python compare_total_time.py \
      "mrmpi-p64-$benchmark-$dataset-onenode_a2a.ppn24_phases.txt,\
mrmpi-p64compress-$benchmark-$dataset-onenode_a2a.ppn24_phases.txt,\
mrmpi-p512-$benchmark-$dataset-onenode_a2a.ppn24_phases.txt,\
mrmpi-p512compress-$benchmark-$dataset-onenode_a2a.ppn24_phases.txt,\
mtmrmpi-basic-$benchmark-$dataset-onenode_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt,\
mtmrmpi-cps-$benchmark-$dataset-onenode_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt,\
mtmrmpi-cpskvhint-$benchmark-$dataset-onenode_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt"\
      "MR-MPI(64M page),\
MR-MPI (Compress; 64M page),\
MR-MPI (512M page),\
MR-MPI (Compress; 512M page),\
Mimir,\
Mimir(partial-reduction),\
Mimir(partial-reduction;KV-hint)"\
    "64K,128K,256K,512K,1M,2M,4M,8M,16M,32M,64M,128M" \
    "16,17,18,19,20,21,22,23,24,25,26,27" "2M,2M,16M,16M,128M,128M,128M"\
    comet-onenode-$benchmark-$dataset-time --indir ../data/comet/ \
    --xlabelname "number of vertex (2^x)" --plottype bar --ylim 0 100
  done
fi

if [ $datatype == "weekscale512M" ];then 
  for dataset in $datasets
  do
    python compare_total_time.py \
    mrmpi-p64-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mrmpi-p512-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mtmrmpi-basic-$benchmark-$dataset-$datatype"_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt",\
mtmrmpi-partreduce-$benchmark-$dataset-$datatype"_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt",\
mtmrmpi-partreducekvhint-$benchmark-$dataset-$datatype"_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt"\
    "MR-MPI(64M page size),\
MR-MPI(512M page size),\
Mimir,\
Mimir(partial-reduction),\
Mimir(partial-reduction;KV-hint)" \
    "512M,1G,2G,4G,8G,16G,32G" "512M,1G,2G,4G,8G,16G,32G" "512G,512G,512G,512G,512G"\
    comet-$datatype-$benchmark-$dataset-time 24 1 --indir ../data/comet/ --xtickettype node \
    --xlabelname "number of node" --plottype bar --ylim 0 20 --coloroff 0
  done
fi

if [ $datatype == "weekscale4G" ];then 
  for dataset in $datasets
  do
    python compare_total_time.py \
      "mrmpi-p512-$benchmark-$dataset-weekscale4G_a2a.ppn24_phases.txt,\
mtmrmpi-basic-$benchmark-$dataset-weekscale4G_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt,\
mtmrmpi-cps-$benchmark-$dataset-weekscale4G_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt,\
mtmrmpi-cpskvhint-$benchmark-$dataset-weekscale4G_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt"\
      "MR-MPI(512M page size),\
Mimir,\
Mimir(partial-reduction),\
Mimir(partial-reduction;KV-hint)" \
    "8M,16M,32M,64M,128M,256M,512M" "1,2,4,8,16,32,64" "1G,1G,1G,1G,1G"\
    comet-weekscale4G-$benchmark-$dataset-time --indir ../data/comet/ \
    --xlabelname "number of node" --plottype bar --ylim 0 80 
  done
fi
