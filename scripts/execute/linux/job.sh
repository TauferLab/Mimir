#! /bin/zsh -e

PPN=16
MIMIR_DEBUG=1

NODE=$1
EXE=$2
INDIR=$3
OUTDIR=$4
TMPDIR=$5
PREFIX=$6
NTIMES=$7
INBUFSIZE=$8
PAGESIZE=$9
PARAM=$10

# echo "node:" $NODE
# echo "program:" $EXE
# echo "input dir:" $INDIR
# echo "output dir:" $OUTDIR
# echo "temp dir:" $TMPDIR
# echo "prefix":$PREFIX
# echo "param:" $PARAM

export NPROC=$(($PPN*$NODE))
export OMP_NUM_THRADS=1

# COMM_UNIT_SIZE=4096B
# COMM_UNIT_SIZE=8388608B

if [ ! -d "$OUTDIR" ]; then mkdir -p "$OUTDIR"; fi

export MIMIR_BUCKET_SIZE=17
export MIMIR_IBUF_SIZE=$INBUFSIZE
export MIMIR_PAGE_SIZE=$PAGESIZE
export MIMIR_COMM_SIZE=$PAGESIZE
# export MIMIR_COMM_UNIT_SIZE=$COMM_UNIT_SIZE
export MIMIR_DBG_ALL=$MIMIR_DEBUG
# export MIMIR_DBG_VERBOSE=$MIMIR_DEBUG

export MPIR_CVAR_NOLOCAL=1

for i in `seq 1 $NTIMES`; do
    echo "Run number" $i
    if [ ! -d "$TMPDIR" ]; then mkdir -p "$TMPDIR"; fi
    mpiexec -n $NPROC \
        ./$EXE $PARAM $INDIR "$PREFIX" $OUTDIR "$TMPDIR" $i 2>&1 > $EXE.log
    rm -rf "$TMPDIR"
done

