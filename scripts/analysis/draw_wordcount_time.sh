#!/bin/bash
benchmark="wordcount"

datatype=''
if [ $# == 1 ]
then
  datatype=$1
else
  echo "./exe datatype [onenode|weakscale512M|weakscale4G|weakscale64G]"
fi

# Draw wordcount onenode data
if [ $datatype == "onenode" ];then
  datasets=""
  for dataset in $datasets;do
    python compare_total_time.py \
      mrmpi-p512-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt"\
      "MR-MPI"\
      "256M,512M,1G,2G,4G,8G,16G,32G,64G" "32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G" "64G"\
      comet-$datatype-$benchmark-$dataset-p512-time \
      --colorlist red --indir ../data/comet/ --plottype box --ylim 0 400
  done

  datasets="uniform wikipedia"
  for dataset in $datasets;do
    python compare_total_time.py \
      mrmpi-p64-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mrmpi-p64compress-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mrmpi-p512-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mrmpi-p512compress-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mtmrmpi-basic-$benchmark-$dataset-$datatype"_c64M-b64M-i64M-h22-a2a.ppn24_phases.txt"\
      "MR-MPI(64M page),MR-MPI(Compress;64M page),MR-MPI(512M page),MR-MPI(Compress;512M page),Mimir"\
      "32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G" "32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G" "32G,32G,32G,32G,32G"\
      comet-$datatype-$benchmark-$dataset-original-time \
      --indir ../data/comet/ --plottype bar --ylim 0 800
  done

  datasets="uniform wikipedia"
  for dataset in $datasets
  do
    python compare_total_time.py \
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
MR-MPI(Compress; 64M page),\
MR-MPI(512M page),\
MR-MPI(Compress; 512M page),\
Mimir,\
Mimir(Partial-reduction),\
Mimir(Partial-reduction; KV-Hint),\
Mimir(Compress),\
Mimir(Compress; KV-Hint)" \
  "32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G" \
  "32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G" "1G,1G,8G,8G,32G,64G,128G,128G,128G"\
  comet-$datatype-$benchmark-$dataset-time --indir ../data/comet/ --plottype bar --ylim 0 150
  done
fi

if [ $datatype == "weakscale512M" ];then
  datatype="weekscale512M"
  datasets="uniform wikipedia"
  config="c64M-b64M-i64M-h22-a2a"
  for dataset in $datasets
  do
    python compare_total_time.py \
      mrmpi-p64-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mrmpi-p512-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mtmrmpi-basic-$benchmark-$dataset-$datatype"_$config.ppn24_phases.txt",\
mtmrmpi-pr-$benchmark-$dataset-$datatype"_$config.ppn24_phases.txt",\
mtmrmpi-prkvhint-$benchmark-$dataset-$datatype"_$config.ppn24_phases.txt"\
      "MR-MPI(64M page size),MR-MPI(512M page size),Mimir,Mimir(partial-reduction),Mimir(partial-reduction;KV-hint)" \
  "512M,1G,2G,4G,8G,16G,32G" "512M,1G,2G,4G,8G,16G,32G" "512G,512G,512G,512G,512G"\
    comet-$datatype-$benchmark-$dataset-time --indir ../data/comet/ --plottype bar  \
    --xlabelname "number of node" --ylim 0 100
  done
fi

if [ $datatype == "weakscale4G" ];then
  datatype="weekscale4G"
  datasets="wikipedia"
  config="c64M-b64M-i64M-h22-a2a"
  for dataset in $datasets;do
    python compare_total_time.py \
      mrmpi-p512-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mtmrmpi-basic-$benchmark-$dataset-$datatype"_$config.ppn24_phases.txt",\
mtmrmpi-pr-$benchmark-$dataset-$datatype"_$config.ppn24_phases.txt",\
mtmrmpi-prkvhint-$benchmark-$dataset-$datatype"_$config.ppn24_phases.txt",\
mrmpi-p512compress-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mtmrmpi-cps-$benchmark-$dataset-$datatype"_$config.ppn24_phases.txt",\
mtmrmpi-cpskvhint-$benchmark-$dataset-$datatype"_$config.ppn24_phases.txt"\
      "MR-MPI(512M page size),Mimir,Mimir(partial-reduction),Mimir(partial-reduction;KV-hint),MR-MPI with compress(512M page size),Mimir(Compress), Mimir(Compress;KV-hint)" \
  "4G,8G,16G,32G,64G,128G,256G" "1,2,4,8,16,32,64" "512G,512G,512G,512G,512G,512G,512G"\
    comet-$datatype-$benchmark-$dataset-time --indir ../data/comet/ --plottype bar  --xlabelname "number of node" --ylim 0 500
  done
  
  #datatype=""
  datasets=""
  config="c64M-b64M-i512M-h17-a2a"
  for dataset in $datasets
  do
    python compare_total_time.py \
      mrmpi-p512compress-$benchmark-$dataset-$datatype"_a2a.ppn24_phases.txt",\
mtmrmpi-partreducekvhintcps-$benchmark-$dataset-$datatype"_$config.ppn24_phases.txt"\
      "MR-MPI with compress(512M page size),Mimir(partial-reduction;KV-hint;KV-compress)" \
  "4G,8G,16G,32G,64G,128G,256G" "4G,8G,16G,32G,64G,128G,256G" "512G,512G,512G,512G,512G"\
    comet-$datatype-$benchmark-$dataset-compress-time 24 1 --indir ../data/comet/ --plottype bar  \
    --xtickettype node --xlabelname "number of node" --ylim 0 100 --coloroff 1 
  done
fi

if [ $datatype == "weakscale64G" ];then
  datasets="uniform"
  datatype="weekscale64G"
  for dataset in $datasets
  do
    python compare_total_time.py \
mtmrmpi-partreducekvhint-$benchmark-$dataset-$datatype"_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt"\
      "Mimir(partial-reduction;KV-hint)" \
  "64G,128G,256G,512G,1T,2T,4T" "64G,128G,256G,512G,1T,2T,4T"  "8T,8T,8T,8T,8T"\
    comet-$datatype-$benchmark-$dataset-time 24 1 --indir ../data/comet/ --plottype bar --xlabelname "number of node" --xtickettype node  --coloroff 4 --ylim 0 1000
  done
fi

