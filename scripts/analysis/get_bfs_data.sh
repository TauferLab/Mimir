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
datasets="graph500s16"

if [ $datatype == "onenode" ];then 
  nodelist="1"
  datasize="64K,128K,256K,512K,1M,2M,4M,8M,16M,32M,64M"
  settings=""
  for setting in $settings
  do
    for dataset in $datasets
    do
      python mrmpirawdata_to_profile.py mrmpi-$setting-$benchmark-$dataset-onenode-*_a2a $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done

  #datasize="32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G"
  datasize="64K,128K,256K,512K,1M,2M,4M,8M,16M,32M,64M" 
  settings="basic cps cpskvhint"
  for setting in $settings
  do
    for dataset in $datasets
    do
      python mtmrmpirawdata_to_profile.py mtmrmpi-$setting-$benchmark-$dataset-onenode-*_c64M-b64M-i64M-h22-a2a $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done
fi

if [ $datatype == "weekscale512M" ];then 
  nodelist="1,2,4,8,16,32,64"
  datasize="512M,1G,2G,4G,8G,16G,32G"
  settings="p64 p512"
  for setting in $settings
  do
    for dataset in $datasets
    do
      python mrmpirawdata_to_profile.py mrmpi-$setting-$benchmark-$dataset-weekscale512M-*_a2a $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done

  #datasize="512M,1G,2G,4G,8G,16G,32G"
  #settings="basic partreduce partreducekvhint"
  #for setting in $settings
  #do
  #  for dataset in $datasets
  #  do
  #    python mtmrmpirawdata_to_profile.py mtmrmpi-$setting-$benchmark-$dataset-weekscale512M-*_c64M-b64M-i512M-h17-a2a $datasize 24 $nodelist $INDIR $OUTDIR
  #  done
  #done
fi

if [ $datatype == "weekscale4G" ];then 
  nodelist="1,2,4,8,16,32,64"
  #datasize="4G,8G,16G,32G,64G,128G,256G"
  datasize="8M,16M,32M,64M,128M,256M,512M" 
  settings=""
  for setting in $settings
  do
    for dataset in $datasets
    do
      python mrmpirawdata_to_profile.py mrmpi-$setting-$benchmark-$dataset-weekscale4G-*_a2a $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done

  datasize="8M,16M,32M,64M,128M,256M,512M"
  settings="cps cpskvhint"
  for setting in $settings
  do
    for dataset in $datasets
    do
      python mtmrmpirawdata_to_profile.py mtmrmpi-$setting-$benchmark-$dataset-weekscale4G-*_c64M-b64M-i64M-h22-a2a $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done
fi
