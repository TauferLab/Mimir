#!/bin/bash

INDIR=../raw_data/comet/octree/
OUTDIR=../data/comet/

benchmark=octree
datasets="1S"

datatype=''
if [ $# == 1 ]
then
  datatype=$1
else
  echo "./exe datatype[onenode|commsize|weekscale4G]"
fi

if [ $datatype == "onenode" ];then  
  nodelist="1"
  datasize="16M,32M,64M,128M,256M,512M,1G,2G,4G"
  settings="p64 p64compress p512 p512compress"
  for setting in $settings
  do
  for dataset in $datasets
    do
      python mrmpirawdata_to_profile.py mrmpi-$setting-$benchmark-$dataset-onenode-*_d0.01-a2a $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done
fi

#datasize="1G,2G,4G,8G,16G,32G,64G,128G,256G,512G,1T,2T"
#settings="basic partreduce partreducekvhint"
#for setting in $settings
#do
#  for dataset in $datasets
#  do
#    python mtmrmpirawdata_to_profile.py mtmrmpi-$setting-$benchmark-$dataset-onenode-*_d0.01-c64M-b64M-i512M-h17-a2a $datasize 24 $nodelist $INDIR $OUTDIR
#  done
#done
