#!/bin/bash
BASEDIR=/projects/MPICH_MCS/yguo/mt-mrmpi/data
datasets="uniform"

#for dataset in $datasets
#do
#  ./sub_wordgenerator.sh $dataset words "$BASEDIR/wordcount/$dataset/singlenode" \
#    "32M 64M 128M 256M 512M 1024M 2G 4G 8G" \
#    "$((32/16*1024*1024)) $((64/16*1024*1024)) $((128/16*1024*1024)) $((256/16*1024*1024)) $((512/16*1024*1024)) $((1024/16*1024*1024)) $((2*1024*1024*1024/16)) $((4*1024*1024*1024/16)) $((8*1024*1024*1024/16))" \
#    "16 16 16 16 16 16 16 16 16" 16 bgq
#done

for dataset in $datasets
do
  ./sub_wordgenerator.sh $dataset words "$BASEDIR/wordcount/$dataset/singlenode" \
    "512M 1024M 2G 4G 8G" \
    "$((512/16*1024*1024)) $((1024/16*1024*1024)) $((2*1024*1024*1024/16)) $((4*1024*1024*1024/16)) $((8*1024*1024*1024/16))" \
    "16 16 16 16 16" 16 bgq
done
