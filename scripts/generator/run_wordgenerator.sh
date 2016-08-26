#!/bin/bash
#./sub_wordgenerator.sh triangular words /scratch/rice/g/gao381/wordcount/triangular/onenode \
#  "32M 64M 128M 256M 512M 1G 2G 4G 8G 16G 32G" \
#  "33554432 67108864 134217728 268435456 536870912 1073741824 2147483648 4294967296 8589934592 17179869184 34359738368" \
#  "20 20 20 20 20 20 20 20 20 20 20" 5

BASEDIR=/oasis/scratch/comet/taogao/temp_project/
#BASEDIR=/scratch/rice/g/gao381/
datasets="uniform"
#for dataset in $datasets
#do
#  ./sub_wordgenerator.sh $dataset words "$BASEDIR/wordcount/$dataset/weekscale512M" \
#    "512M 1G 2G 4G 8G 16G 32G" \
#    "536870912 1073741824 2147483648 4294967296 8589934592 17179869184 34359738368" \
#    "24 48 96 192 384 768 1536" 6 slurm
#done

#for dataset in $datasets
#do
#  ./sub_wordgenerator.sh $dataset words "$BASEDIR/wordcount/$dataset/weekscale4G" \
#    "4G 8G 16G 32G 64G 128G 256G" \
#    "4294967296 8589934592 17179869184 34359738368 68719476736 137438953472 274877906944" \
#    "24 48 96 192 384 768 1536" 6 slurm
#done

for dataset in $datasets
do
  ./sub_wordgenerator.sh $dataset words "$BASEDIR/wordcount/$dataset/weekscale64G" \
    "64G 128G 256G 512G 1T 2T 4T" \
    "715827883 715827883 715827883 715827883 715827883 715827883 715827883" \
    "96 192 384 768 1536 3072 6144" 24 slurm
done
