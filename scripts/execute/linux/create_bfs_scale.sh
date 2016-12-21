#! /bin/zsh

BASEDIR="/projects/MPICH_MCS/yguo/mt-mrmpi/data/bfs"

N_PER_NODE=$1
NODE_START=$2
NODE_END=$3
N_START=$(($N_PER_NODE+1))

INDIR="$BASEDIR/graph500_single/$N_START"

NODE=1
N=$N_START

VERT=$(echo "2^$N_START" | bc)

for ((i = 1; $NODE < $NODE_END; i++)); do
    NODE=$(($NODE*2))
    OUTDIR="$BASEDIR/graph500_weakscale$N_PER_NODE/$NODE"
    mkdir -p "$OUTDIR"
    N=$((N_PER_NODE+$i))
    INDIR="$BASEDIR/graph500_single/s$N"
    echo "$NODE $INDIR $OUTDIR $N $VERT"
    N_FILES=$(($NODE*16))
    N_INPUT=$(ls -l "$INDIR" | wc -l)
    if [ $N_FILES -lt $N_INPUT ]; then
        echo "Need to create $N_FILES, but has $N_INPUT"
        continue
    fi
    pushd "$OUTDIR"
    for file in $(ls "$INDIR"); do
        LINE=$(wc -l "$INDIR/$file" | cut -d' ' -f1)
#        echo $LINE
        if [ $VERT -le $LINE ]; then
            split -l $VERT "$INDIR/$file" $file.split.
        else
            ln -s "$INDIR/$file"
        fi
    done
    popd
    N_CREATE=$(ls -l "$OUTDIR" | wc -l)
    if [ $N_FILES -gt $N_CREATE ]; then
        echo "Need to create $N_FILES, but only created $N_CREATE"
    fi
done
