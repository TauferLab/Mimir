#! /bin/zsh

BDIR="/projects/SSSPPg/yguo/mt-mrmpi/data"

function create_data {
    FSIZE=$(($1*1024*1024))
    node=$2
    qsub -A MPICH_MCS -t 15 -n $node --mode script \
        ./wordcount-uniform.bgq.job wordcount \
        "$BDIR/wordcount/uniform/weakscale$1/$node" \
        $FSIZE $((100000/16)) $node
        
}

FSIZE=$1
n_start=$2
n_end=$3

for ((n=n_start; n<=n_end; n=n*2)); do
    create_data $FSIZE $n
    while [ $? -ne 0 ]; do
        echo "wait for 5m"
        sleep 5m
        create_data $FSIZE $n
    done
done
