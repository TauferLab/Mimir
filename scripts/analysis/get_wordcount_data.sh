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

# Draw onenode figures
if [ $datatype == "onenode" ];then
  settings=""
  datasets="uniform wikipedia"
  datasize="32M,64M,128M,256M,512M,1G,2G,4G,8G,16G"
  testtype="onenode"
  nodelist="1"
  configure="a2a"
  for setting in $settings
  do
    for dataset in $datasets
    do
      python mrmpirawdata_to_profile.py mrmpi-$setting-$benchmark-$dataset-$testtype-*_$configure $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done
  settings="basic pr prkvhint cps cpskvhint"
  datasize="32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G"
  configure="c64M-b64M-i64M-h22-a2a"
  for setting in $settings
  do
    for dataset in $datasets
    do
      python mtmrmpirawdata_to_profile.py mtmrmpi-$setting-$benchmark-$dataset-$testtype-*_$configure $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done
fi

# Draw 512M/node figures
if [ $datatype == "weekscale512M" ];then
  testtype="weekscale512M"
  settings="p64 p512"
  datasets="uniform triangular wikipedia"
  datasize="512M,1G,2G,4G,8G,16G,32G"
  nodelist="1,2,4,8,16,32,64"
  configure="a2a"
  for setting in $settings;do
    for dataset in $datasets;do
      python mrmpirawdata_to_profile.py mrmpi-$setting-$benchmark-$dataset-$testtype-*_$configure $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done
  settings=""
  #datasets=""
  configure="c64M-b64M-i512M-h17-a2a"
  for setting in $settings;do
    for dataset in $datasets;do
      python mtmrmpirawdata_to_profile.py mtmrmpi-$setting-$benchmark-$dataset-$testtype-*_$configure $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done
fi

# Draw 4G/node data
if [ $datatype == "weekscale4G" ];then
  settings=""
  datasets="uniform"
  datasize="4G,8G,16G,32G,64G,128G,256G"
  testtype="weekscale4G"
  nodelist="1,2,4,8,16,32,64"
  configure="a2a"
  for setting in $settings;do
    for dataset in $datasets;do
      python mrmpirawdata_to_profile.py mrmpi-$setting-$benchmark-$dataset-$testtype-*_$configure $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done
  settings="basic"
  datasets="uniform wikipedia"
  configure="c64M-b64M-i64M-h22-a2a"
  for setting in $settings;do
    for dataset in $datasets;do
      python mtmrmpirawdata_to_profile.py mtmrmpi-$setting-$benchmark-$dataset-$testtype-*_$configure $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done
fi

# Draw 64G/node data
if [ $datatype == "weekscale64G" ];then
  settings="partreducekvhint"
  datasets="uniform"
  datasize="64G,128G,256G,512G,1T,2T,4T"
  testtype="weekscale64G"
  nodelist="1,2,4,8,16,32,64"
  configure="c64M-b64M-i512M-h17-a2a"
  for setting in $settings;do
    for dataset in $datasets;do
      python mtmrmpirawdata_to_profile.py mtmrmpi-$setting-$benchmark-$dataset-$testtype-*_$configure $datasize 24 $nodelist $INDIR $OUTDIR
    done
  done
fi
