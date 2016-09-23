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
  settings="p512"
  config="a2a"
  testtype="onenode"
  datalist="256M,512M,1G,2G,4G,8G,16G"
  nodelist="1"
  for dataset in $datasets
  do
    for setting in $settings
    do
      python compare_time_breakdown.py \
        "mrmpi-$setting-$benchmark-$dataset-$testtype"_"$config.ppn24_phases.txt"\
        "MR-MPI"\
        $datalist $datalist \
        comet-$testtype-$benchmark-$dataset-$setting-breakdown 24 $nodelist \
        --plottype point --datatype median --normalize true --style simple --ylim 0 100
    done
  done

  datasets="wikipedia"
  settings="prkvhint"
  config="c64M-b64M-i64M-h22-a2a"
  testtype="onenode"
  datalist="32G,64G"
  nodelist="1"
  for dataset in $datasets
  do
    for setting in $settings
    do
      python compare_time_breakdown.py \
        "mtmrmpi-$setting-$benchmark-$dataset-$testtype"_"$config.ppn24_phases.txt"\
        "MR-MPI"\
        $datalist $datalist\
        $testtype-mtmrmpi-$setting-$benchmark-$dataset-breakdown 24 $nodelist \
        --plottype point --datatype max --normalize false --style detail --ylim 0 100
    done
  done
fi
