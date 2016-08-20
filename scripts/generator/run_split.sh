#!/bin/bash
BASEDIR=/oasis/scratch/comet/taogao/temp_project/
#BASEDIR=/scratch/rice/g/gao381/
#datasets="uniform triangular"
#for dataset in $datasets
#do
#  ./sub_wordgenerator.sh $dataset words "$BASEDIR/wordcount/$dataset/weekscale512M" \
#    "512M 1G 2G 4G 8G 16G 32G" \
#    "536870912 1073741824 2147483648 4294967296 8589934592 17179869184 34359738368" \
#    "24 48 96 192 384 768 1536" 6 slurm
#done

./sub_split.sh "$BASEDIR/wordcount/wikipedia/wikipedia_50GB" words \
  "$BASEDIR/wordcount/wikipedia/weekscale512M" \
  "512M 1G 2G 4G 8G 16G 32G" \
  "536870912 1073741824 2147483648 4294967296 8589934592 17179869184 34359738368" \
  "24 48 96 192 384 768 1536" 6 slurm

#./sub_split.sh "$BASEDIR/wordcount/wikipedia/wikipedia_50GB" words \
#  "$BASEDIR/wordcount/wikipedia/weekscale512M" \
#  "2G" \
#  "2147483648" \
#  "96" 24 slurm

#for dataset in $datasets
#do
#  ./sub_wordgenerator.sh $dataset words "$BASEDIR/wordcount/$dataset/weekscale4G" \
#    "4G 8G 16G 32G 64G 128G 256G" \
#    "4294967296 8589934592 17179869184 34359738368 68719476736 137438953472 274877906944" \
#    "24 48 96 192 384 768 1536" 6 slurm
#done
