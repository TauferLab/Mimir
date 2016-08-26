#!/bin/bash

INDIR=../raw_data/comet/wordcount/
OUTDIR=../data/comet/
benchmark=wordcount

datatype=''
if [ $# == 1 ]
then
  datatype=$1
else
  echo "./exe datatype[onenode|commsize|weekscale4G]"
fi

if [ $datatype == "onenode" ];then
  #settings="p64 p512"
  datasets="wikipedia"
  #datasize="32M,64M,128M,256M,512M,1G,2G,4G,8G,16G"
  testtype="onenode"
  nodelist="1"
  #configure="a2a"
  #for setting in $settings
  #do
  #  for dataset in $datasets
  #  do
  #    python mrmpirawdata_to_profile.py mrmpi-$setting-$benchmark-$dataset-$testtype-*_$configure $datasize 24 $nodelist $INDIR $OUTDIR
  #  done
  #done
  settings="partreducekvhint"
  datasize="64G"
  configure="c64M-b64M-i512M-h22-a2a"
  for setting in $settings
  do
    for dataset in $datasets
    do
      python mtmrmpirawdata_to_profile.py mtmrmpi-$setting-$benchmark-$dataset-$testtype-*_$configure $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done
fi

if [ $datatype == "commsize" ];then
  datasize="4G,8G,16G,32G,64G,128G,256G"
  testtype="weekscale4G"
  nodelist="1,2,4,8,16,32,64"
  configure="a2a"
  settings="basic"
  datasets="uniform"
  configure="c64M-b64M-i512M-h17-a2a"
  for setting in $settings;do
    for dataset in $datasets;do
      python mtmrmpirawdata_to_profile.py mtmrmpi-$setting-$benchmark-$dataset-$testtype-*_$configure $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done
  configure="c512M-b64M-i512M-h17-a2a"
  for setting in $settings;do
    for dataset in $datasets;do
      python mtmrmpirawdata_to_profile.py mtmrmpi-$setting-$benchmark-$dataset-$testtype-*_$configure $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done
fi

if [ $datatype == "weekscale4G" ];then
  settings="p512"
  datasets="wikipedia"
  datasize="4G,8G,16G,32G,64G,128G,256G"
  testtype="weekscale4G"
  nodelist="1,2,4,8,16,32,64"
  configure="a2a"
  for setting in $settings;do
    for dataset in $datasets;do
      python mrmpirawdata_to_profile.py mrmpi-$setting-$benchmark-$dataset-$testtype-*_$configure $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done
  settings="basic partreduce partreducekvhint"
  datasets=""
  configure="c64M-b64M-i512M-h17-a2a"
  for setting in $settings;do
    for dataset in $datasets;do
      python mtmrmpirawdata_to_profile.py mtmrmpi-$setting-$benchmark-$dataset-$testtype-*_$configure $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done
fi
