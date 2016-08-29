#!/bin/bash
BASEDIR=/oasis/scratch/comet/taogao/temp_project/
datasets="uniform"

for dataset in $datasets
do
  ./sub_wordgenerator.sh $dataset words "$BASEDIR/wordcount/$dataset/weekscale64G" \
    "64G 128G 256G 512G 1T 2T 4T" \
    "715827883 715827883 715827883 715827883 715827883 715827883 715827883" \
    "96 192 384 768 1536 3072 6144" 24 slurm
done
