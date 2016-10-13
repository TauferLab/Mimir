#! /bin/zsh

source retry.sh

if [ -z "$INPUT_BASEDIR" ]; then
    INPUT_BASEDIR=/projects/MPICH_MCS/yguo/mt-mrmpi/data
fi
if [ -z "$OUTPUT_BASEDIR" ]; then
    OUTPUT_BASEDIR=/projects/MPICH_MCS/yguo/mt-mrmpi/data
fi

VERSION=mtmrmpi

if [ $# -lt 3 ]; then
    echo "./exe [benchmark] [setting list] [datatypes] [test type] \
        [data list] [param list] [run times] [prev job]"
    exit -1
fi

SETTING=$1
BENCHMARK=$2
INDIR=$3
NTIMES=${4:-1}
NNODE_MIN=${5:-1}
NNODE_MAX=${6:-1}
INBUFSIZE=${7:-"512M"}
PAGESIZE=${8:-"64M"}
TIMEOUT=${9:-45}
PARAMS=${10:-""}

EXE=$BENCHMARK"_"$SETTING
if [ ! -x "$EXE" ]; then
    echo "$EXE" "is not found"
    exit 1
fi

export PPN=16

OUTDIR="$OUTPUT_BASEDIR/results"

#echo "$BENCHMARK"
#echo "$SETTING"
#echo "$INDIR"
#echo "$NTIMES"
#echo "$NNODE_MIN" "$NNODE_MAX"
#echo "$PARAMS"
#echo "$EXE"
#echo "$PREFIX"
#echo "$INPUTDIR"

#exit 0
for ((n_node = $NNODE_MIN; n_node <= $NNODE_MAX; n_node = n_node * 2)); do
    INPUTDIR="$INPUT_BASEDIR/$BENCHMARK/$INDIR/$n_node"
    if [ ! -d "$INPUTDIR" ]; then
        echo "Input dir " "$INPUTDIR" "is missing"
        exit 1
    fi

    PREFIX="$VERSION-$SETTING-$BENCHMARK-${INDIR//\//-}-$n_node"
    TMPDIR="$OUTPUT_BASEDIR/tmp/$PREFIX"
    retry 10m \
        qsub -A MPICH_MCS -t $TIMEOUT -n $n_node --mode script \
            ./job.sh $n_node $EXE $INPUTDIR $OUTDIR $TMPDIR $PREFIX $NTIMES \
                $INBUFSIZE $PAGESIZE $PARAMS

    echo "  =>" $n_node "nodes" $EXE $INPUTDIR $INBUFSIZE $PAGESIZE
done

exit 0
