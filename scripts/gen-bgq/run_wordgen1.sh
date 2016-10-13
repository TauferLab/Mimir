#!/bin/bash
BASEDIR=/projects/aurora_app/yguo_tmp/mt-mrmpi/data
datasets="uniform"

for dataset in $datasets
do
  ./sub_wordgenerator.sh $dataset words "$BASEDIR/wordcount/$dataset/weekscale_small" \
    "32M" \
    "$((32*1024*1024))" \
    "16" 16 bgq
done
