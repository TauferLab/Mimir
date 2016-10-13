#! /bin/zsh -e

PPN=16

NODE=$1
EXE=$2
INDIR=$3
OUTDIR=$4
TMPDIR=$5
PREFIX=$6
NTIMES=$7
INBUFSIZE=$8
PARAM=$9

# echo "node:" $NODE
# echo "program:" $EXE
# echo "input dir:" $INDIR
# echo "output dir:" $OUTDIR
# echo "temp dir:" $TMPDIR
# echo "prefix":$PREFIX
# echo "param:" $PARAM

export NPROC=$(($PPN*$NODE))
export OMP_NUM_THRADS=1

for i in `seq 1 $NTIMES`; do
    echo $i
    if [ ! -d "$TMPDIR" ]; then mkdir -p "$TMPDIR"; fi
    runjob --np $NPROC -p $PPN --block $COBALT_PARTNAME --verbose=INFO \
        --envs MR_BUCKET_SIZE=17 \
        --envs MR_INBUF_SIZE=$INBUFSIZE \
        --envs MR_PAGE_SIZE=64M \
        --envs MR_COMM_SIZE=64M \
        : ./$EXE $PARAM $INDIR "$PREFIX" $OUTDIR "$TMPDIR" $i
    /bin/zsh "$PREFIX"
    rm -f "$PREFIX"
    rm -rf "$TMPDIR"
done

