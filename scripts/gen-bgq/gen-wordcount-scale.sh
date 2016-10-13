#! /bin/zsh

source retry.sh

BDIR="/projects/SSSPPg/yguo/mt-mrmpi/data"

function create_data {
    FSIZE=$(($1*1024*1024/16))
    node=$2
    retry 5m \
        qsub -A MPICH_MCS -t 60 -n $node --mode script \
            ./wordcount-uniform.bgq.job wordcount \
            "$BDIR/wordcount/uniform/weakscale$1G/$node" \
            $FSIZE 100000 $node
        
}

FILESIZE=$1
n_start=$2
n_end=$3

for ((n=n_start; n<=n_end; n=n*2)); do
    echo "Submitting filesize $FILESIZE for $n node"
    create_data $FILESIZE $n
done
