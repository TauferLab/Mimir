#! /bin/zsh

PPN=16

NODE=$1
EXE=$2
INDIR=$3
OUTDIR=$4
TMPDIR=$5
PREFIX=$6
PARAM=$7
SIZES=$8

# echo "node:" $NODE
# echo "program:" $EXE
# echo "input dir:" $INDIR
# echo "output dir:" $OUTDIR
# echo "temp dir:" $TMPDIR
# echo "prefix":$PREFIX
# echo "param:" $PARAM

export NPROC=$(($PPN*$NODE))
export OMP_NUM_THRADS=1

BLOCKS=$(get-bootable-blocks --size $NODE $COBALT_PARTNAME)

for BLOCK in $BLOCKS; do
    boot-block --block $BLOCK &
done
wait

arr_blocks=(`echo $BLOCKS`)

index=0
for SIZE in $SIZES; do
    INDIR=$INDIR/$SIZE
    PREFIX=$PREFIX-$SIZE
    runjob --np $NPROC -p $PPN --block ${arr_blocks[$index]} --verbose=INFO \
        --envs MR_BUCKET_SIZE=17 \
        --envs MR_INBUF_SIZE=512M \
        --envs MR_PAGE_SIZE=64M \
        --envs MR_COMM_SIZE=64M \
        : ./$EXE $PARAM $INDIR/$SIZE "$PREFIX-$SIZE" $OUTDIR $TMPDIR &
    index=$(($index+1))
done
wait

for BLOCK in $BLOCKS; do
    boot-block --block $BLOCK --free &
done
wait

# runjob --np $NPROC -p $PPN --block $COBALT_PARTNAME --verbose=INFO \
#     --envs MR_BUCKET_SIZE=17 \
#     --envs MR_INBUF_SIZE=512M \
#     --envs MR_PAGE_SIZE=64M \
#     --envs MR_COMM_SIZE=64M \
#     : ./$EXE $PARAM $INDIR $PREFIX $OUTDIR $TMPDIR

# if [ $USETAU == "true" ]
# then
#   pprof | grep "  Heap Memory Used (KB)" | \
#     awk -v np=$PPN -v data=$DATASIZE '{print data","np","i++","$2}' \
#     >> $TAUFILE
#   rm profile.*
# fi
