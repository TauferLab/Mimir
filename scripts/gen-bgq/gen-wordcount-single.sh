#! /bin/zsh

BDIR="/projects/MPICH_MCS/yguo/mt-mrmpi/data"

function create_data {
    FSIZE=$(($1*1024*1024/16))
    node=1
    qsub -A MPICH_MCS -t 15 -n $node --mode script \
        ./wordcount-uniform.bgq.job wordcount \
        "$BDIR/wordcount/uniform/singlenode/$1M" \
        $FSIZE 100000 $node
}

function create_data_G {
    FSIZE=$(($1*1024*1024*1024/16))
    node=1
    qsub -A MPICH_MCS -t 15 -n $node --mode script \
        ./wordcount-uniform.bgq.job wordcount \
        "$BDIR/wordcount/uniform/singlenode/$1G" \
        $FSIZE 100000 $node
}

#create_data 32
#create_data 64
#create_data 128
#create_data 256
#create_data 512
#create_data 1024
#create_data_G 2
#create_data_G 4
#create_data_G 8
#create_data_G 16
create_data_G 32
