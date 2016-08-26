#!/bin/bash
benchmark="wordcount"

datatype=''
if [ $# == 1 ]
then
  datatype=$1
else
  echo "./exe datatype[onenode|commsize|weekscale4G]"
fi

if [ $datatype == "onenode" ];then
  dataset="wikipedia"
  testtype="onenode"
  python compare_total_time.py \
mtmrmpi-partreducekvhint-$benchmark-$dataset-$testtype"_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt",\
mtmrmpi-partreducekvhint-$benchmark-$dataset-$testtype"_c64M-b64M-i512M-h22-a2a.ppn24_phases.txt"\
    "MR-MPI++(h17),MR-MPI++(h22)" \
  "64G" "128G,128G"\
  comet-$testtype-$benchmark-$dataset-hcompare-time 24 1 --indir ../data/comet/ --plottype bar --xtickettype dataset --ylim 0 400

  datasets="uniform triangular wikipedia"
  testtype="onenode"
  for dataset in $datasets;do
    python compare_total_time.py \
      mrmpi-p64-$benchmark-$dataset-$testtype"_a2a.ppn24_phases.txt",\
mrmpi-p512-$benchmark-$dataset-$testtype"_a2a.ppn24_phases.txt"\
      "MR-MPI(64M page size),MR-MPI(512M page size)"\
      "32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G" "32G,32G"\
      comet-$testtype-$benchmark-$dataset-original-time 24 1 --indir ../data/comet/ --plottype bar --xtickettype dataset --ylim 0 1000
  done
  for dataset in $datasets
  do
    python compare_total_time.py \
      mrmpi-p64-$benchmark-$dataset-$testtype"_a2a.ppn24_phases.txt",\
mrmpi-p512-$benchmark-$dataset-$testtype"_a2a.ppn24_phases.txt",\
mtmrmpi-basic-$benchmark-$dataset-$testtype"_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt",\
mtmrmpi-partreduce-$benchmark-$dataset-$testtype"_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt",\
mtmrmpi-partreducekvhint-$benchmark-$dataset-$testtype"_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt"\
    "MR-MPI(64M page size),\
MR-MPI(512M page size),\
MR-MPI++,\
MR-MPI++(partial-reduction),\
MR-MPI++(partial-reduction;KV-hint)" \
  "32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G" "1G,8G,32G,64G,128G"\
  comet-$testtype-$benchmark-$dataset-time 24 1 --indir ../data/comet/ --plottype bar --xtickettype dataset --ylim 0 400
  done
fi

if [ $datatype == "commsize" ];then
  datasets="triangular"
  testtype="weekscale4G"
  for dataset in $datasets;do
    python compare_total_time.py \
mtmrmpi-basic-$benchmark-$dataset-$testtype"_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt",\
mtmrmpi-basic-$benchmark-$dataset-$testtype"_c512M-b64M-i512M-h17-a2a.ppn24_phases.txt"\
    "MR-MPI++(64M communcation buffer),
MR-MPI++(512M communication buffer)" \
    "4G,8G,16G,32G,64G,128G,256G" "512G,512G,512G,512G,512G"\
    comet-$testtype-$benchmark-$dataset-commsize-time 24 1 --indir ../data/comet/ --plottype box  --xtickettype node --ylim 0 100
  done
fi

if [ $datatype == "weekscale4G" ];then
  datasets="uniform triangular"
  testtype="weekscale4G"
  for dataset in $datasets
  do
    python compare_total_time.py \
      mrmpi-p64-$benchmark-$dataset-$testtype"_a2a.ppn24_phases.txt",\
mrmpi-p512-$benchmark-$dataset-$testtype"_a2a.ppn24_phases.txt",\
mtmrmpi-basic-$benchmark-$dataset-$testtype"_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt"\
      "MR-MPI(64M page size),\
MR-MPI(512M page size),\
MR-MPI++" \
  "4G,8G,16G,32G,64G,128G,256G" "512G,512G,512G,512G,512G"\
    comet-$testtype-$benchmark-$dataset-time 24 1 --indir ../data/comet/ --plottype bar  --xtickettype node --ylim 0 800
  done

  datasets="wikipedia"
  for dataset in $datasets
  do
    python compare_total_time.py \
      mrmpi-p64-$benchmark-$dataset-$testtype"_a2a.ppn24_phases.txt",\
mrmpi-p512-$benchmark-$dataset-$testtype"_a2a.ppn24_phases.txt",\
mtmrmpi-basic-$benchmark-$dataset-$testtype"_c64M-b64M-i512M-f32-a2a.ppn24_phases.txt"\
      "MR-MPI(64M page size),\
MR-MPI(512M page size),\
MR-MPI++" \
  "4G,8G,16G,32G,64G,128G,256G" "512G,512G,512G,512G,512G"\
    comet-$testtype-$benchmark-$dataset-time 24 1 --indir ../data/comet/ --plottype bar  --xtickettype node --ylim 0 800
  done

  datasets="uniform triangular"
  testtype="weekscale4G"
  for dataset in $datasets
  do
    python compare_total_time.py \
      mrmpi-p512-$benchmark-$dataset-$testtype"_a2a.ppn24_phases.txt",\
mtmrmpi-basic-$benchmark-wikipedia-$testtype"_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt",\
mtmrmpi-partreduce-$benchmark-$dataset-$testtype"_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt",\
mtmrmpi-partreducekvhint-$benchmark-$dataset-$testtype"_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt"\
      "MR-MPI(512M page size),MR-MPI++,MR-MPI++(partial-reduction),MR-MPI++(partial-reduction;KV-hint)" \
  "4G,8G,16G,32G,64G,128G,256G" "512G,512G,512G,512G,512G"\
    comet-$testtype-$benchmark-$dataset-inmem-time 24 1 --indir ../data/comet/ --plottype bar  \
    --xtickettype node --ylim 0 500 --coloroff 1 
  done

  datasets="wikipedia"
  testtype="weekscale4G"
  for dataset in $datasets
  do
    python compare_total_time.py \
      mrmpi-p512-$benchmark-$dataset-$testtype"_a2a.ppn24_phases.txt",\
mtmrmpi-basic-$benchmark-wikipedia-$testtype"_c64M-b64M-i512M-f32-a2a.ppn24_phases.txt",\
mtmrmpi-partreduce-$benchmark-$dataset-$testtype"_c64M-b64M-i512M-f32-a2a.ppn24_phases.txt",\
mtmrmpi-partreducekvhint-$benchmark-$dataset-$testtype"_c64M-b64M-i512M-f32-a2a.ppn24_phases.txt"\
      "MR-MPI(512M page size),MR-MPI++,MR-MPI++(partial-reduction),MR-MPI++(partial-reduction;KV-hint)" \
  "4G,8G,16G,32G,64G,128G,256G" "512G,512G,512G,512G,512G"\
    comet-$testtype-$benchmark-$dataset-inmem-time 24 1 --indir ../data/comet/ --plottype bar  \
    --xtickettype node --ylim 0 500 --coloroff 1 
  done
fi
