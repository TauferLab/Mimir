#! /bin/zsh

BDIR="/projects/MPICH_MCS/yguo/mimir/data"

function create_data {
    N_LIGAND=$(echo "2^$1" | bc)
    node=$2
    qsub -A MPICH_MCS -t 15 -n $node --mode script \
        ./octreegenerator.bgq.job octree \
        "$BDIR/octree/1S/singlenode/$1" \
        0 0 0.5 "$N_LIGAND" normal $node
}

s_start=$1
s_end=$2

for ((n=s_start; n<=s_end; n=n+1)); do
    create_data $n 1
done
