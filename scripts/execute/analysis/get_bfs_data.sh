#!/bin/bash

INDIR=../raw_data/comet/bfs/
OUTDIR=../data/comet/

datatype=''
if [ $# == 1 ]
then
  datatype=$1
else
  echo "./exe datatype[onenode|commsize|weekscale4G]"
fi

benchmark=bfs
datasets="graph500"

if [ $datatype == "onenode" ];then 
  nodelist="1"
  datasize="32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G"
  settings="p64 p512"
  for setting in $settings
  do
    for dataset in $datasets
    do
      python mrmpirawdata_to_profile.py mrmpi-$setting-$benchmark-$dataset-onenode-*_a2a $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done

  datasize="32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G"
  settings="basic partreduce partreducekvhint"
  for setting in $settings
  do
    for dataset in $datasets
    do
      python mtmrmpirawdata_to_profile.py mtmrmpi-$setting-$benchmark-$dataset-onenode-*_c64M-b64M-i512M-h17-a2a $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done
fi

if [ $datatype == "weekscale4G" ];then 
  nodelist="1,2,4,8,16,32,64"
  datasize="4G,8G,16G,32G,64G,128G,256G"
  settings="p64 p512"
  for setting in $settings
  do
    for dataset in $datasets
    do
      python mrmpirawdata_to_profile.py mrmpi-$setting-$benchmark-$dataset-weekscale4G-*_a2a $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done

  datasize="4G,8G,16G,32G,64G,128G,256G"
  settings="basic partreduce partreducekvhint"
  for setting in $settings
  do
    for dataset in $datasets
    do
      python mtmrmpirawdata_to_profile.py mtmrmpi-$setting-$benchmark-$dataset-weekscale4G-*_c64M-b64M-i512M-h17-a2a $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done
fi
