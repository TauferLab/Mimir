#!/bin/bash
benchmark="wordcount"

datatype=''
if [ $# == 1 ]
then
  datatype=$1
else
  echo "./exe datatype [onenode|weakscale512M|weakscale4G|weakscale64G]"
fi


if [ $datatype == "onenode" ];then
  datasets=""
  for dataset in $datasets
  do
    python compare_total_memory_usage.py \
      mrmpi-p64-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mrmpi-p64compress-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mrmpi-p512-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mrmpi-p512compress-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt"\
      "MR-MPI(64M page size),MR-MPI with compress(64M page size),\
MR-MPI(512M page size),MR-MPI with compress(512M page size)"\
      "32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G" \
"32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G" "32G,32G,32G,32G"\
      comet-onenode-$benchmark-$dataset-compress-maxmem \
      --colorlist 'red,green,blue,yellow' --ylim 0 5.5
  done

  datasets="uniform wikipedia"
  for dataset in $datasets
  do
    python compare_total_memory_usage.py \
    mrmpi-p64-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mrmpi-p64compress-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mrmpi-p512-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mrmpi-p512compress-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mtmrmpi-basic-$benchmark-$dataset-$datatype"_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt",\
mtmrmpi-pr-$benchmark-$dataset-$datatype"_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt",\
mtmrmpi-prkvhint-$benchmark-$dataset-$datatype"_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt",\
mtmrmpi-cps-$benchmark-$dataset-$datatype"_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt",\
mtmrmpi-cpskvhint-$benchmark-$dataset-$datatype"_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt"\
      "MR-MPI(64M page),\
MR-MPI(Compress;64M page),\
MR-MPI(512M page),\
MR-MPI(Compress;512M page),\
Mimir,\
Mimir(Partial-reduction),\
Mimir(Partial-reduction;KV-hint),\
Mimir(Compress),\
Mimir(Compress;KV-hint)" \
    "32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G" \
    "32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G" \
    "1G,1G,8G,8G,32G,64G,128G,128G,128G" \
    comet-onenode-$benchmark-$dataset-maxmem --ylim 0 6 
  done


  datasets=""
  for dataset in $datasets
  do
    python compare_total_memory_usage.py \
      mtmrmpi-basic-$benchmark-$dataset-$datatype"_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt",\
mtmrmpi-pr-$benchmark-$dataset-$datatype"_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt",\
mtmrmpi-prkvhint-$benchmark-$dataset-$datatype"_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt",\
mtmrmpi-cps-$benchmark-$dataset-$datatype"_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt",\
mtmrmpi-cpskvhint-$benchmark-$dataset-$datatype"_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt"\
      "M,\
MR-MPI(512M page size),\
Mimir(basic),\
Mimir(pr),\
Mimir(cps)" \
    "32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G" \
    "32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G" \
    "1G,8G,64G,64G,64G" \
    comet-onenode-$benchmark-$dataset-maxmem 
  done
fi

if [ $datatype == "weakscale512M" ];then
  datatype=weekscale512M

  #datasets="uniform"
  #for dataset in $datasets
  #do
  #  python compare_total_memory_usage.py \
  #    mrmpi-p64-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt" \
  #    "MR-MPI(64M page size)" \
  #    "512M,1G,2G,4G,8G,16G,32G" \
  #    "64G" \
  #    comet-$datatype-$benchmark-$dataset-maxmem 24 1 --top off
  #done

  datasets="uniform triangular wikipedia"
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
    "64G,64G,64G,64G,64G" "0.5,0.5,0.5,0.5,0.5,0.5,0.5"\
    comet-$datatype-$benchmark-$dataset-maxmem 24 1 --xtickettype node --top off
  done
fi

if [ $datatype == "weakscale4G" ];then
  datatype=weekscale4G
  datasets="uniform triangular wikipedia"
  for dataset in $datasets
  do
    python compare_total_memory_usage.py \
      mrmpi-p512-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mtmrmpi-basic-$benchmark-$dataset-$datatype"_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt",\
mtmrmpi-partreduce-$benchmark-$dataset-$datatype"_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt",\
mtmrmpi-partreducekvhint-$benchmark-$dataset-$datatype"_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt"\
      "MR-MPI(512M page size),\
MR-MPI++,\
MR-MPI++(partial-reduction),\
MR-MPI++(partial-reduction;KV-hint)" \
    "4G,8G,16G,32G,64G,128G,256G" \
    "512G,512G,512G,512G,512G" "4,4,4,4,4,4,4"\
    comet-$datatype-$benchmark-$dataset-maxmem 24 1 --xtickettype node --coloroff 1 --top on
  done
fi
