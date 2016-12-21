#! /bin/zsh

BDIR="/projects/MPICH_MCS/yguo/mt-mrmpi/data"

source retry.sh

function create_data {
    node=$2
    retry 15m \
        qsub -A MPICH_MCS -t 15 -n $node --proccount $(($node*16)) --mode c16 \
            ./split-half $1.16 \
            "$BDIR/bfs/graph500_single/s$1" \
            $(echo "2^23" | bc)
}

SCALE=$1
n_start=$2
n_end=$3

for ((n=n_start; n<=n_end; n=n*2)); do
    create_data $SCALE $n
done
