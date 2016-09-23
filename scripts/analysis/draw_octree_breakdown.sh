#!/bin/bash

benchmark="octree"
datasets="1S"
settings="p512"
config="d0.01-a2a"
testtype="onenode"
datalist="1G,2G,4G,8G,16G,32G,64G,128G,256G,512G,1T,2T"
nodelist="1"
for dataset in $datasets
do
  for setting in $settings
  do
    python compare_time_breakdown.py \
      "mrmpi-$setting-$benchmark-$dataset-$testtype"_"$config.ppn24_phases.txt"\
      "MR-MPI"\
      $datalist \
      $testtype-mrmpi-$setting-$benchmark-$dataset-breakdown 24 $nodelist \
      --plottype point --datatype mean --normalize true --style simple --ylim 0 50
  done
done
