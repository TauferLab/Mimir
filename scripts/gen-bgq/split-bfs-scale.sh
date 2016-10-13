#! /bin/zsh

BDIR="/projects/MPICH_MCS/yguo/mt-mrmpi/data"

function create_data {
    node=$2
    qsub -A MPICH_MCS -t 15 -n $node --proccount $(($node*16)) --mode c16 \
        ./split-half $1.16 \
        "$BDIR/bfs/graph500_single2/s$1" \
        $(echo "2^23" | bc)
}

SCALE=$1
n_start=$2
n_end=$3

for ((n=n_start; n<=n_end; n=n*2)); do
    create_data $SCALE $n
    while [ $? -ne 0 ]; do
        echo "wait for 5m"
        sleep 15m
        create_data $SCALE $n
    done
done
