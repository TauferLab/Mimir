#! /bin/zsh

export BASEDIR=/projects/MPICH_MCS/yguo/mt-mrmpi/data
VERSION=mtmrmpi
#SCRIPT="job.sh"

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
PARAMS=${8:-""}

EXE=$BENCHMARK"_"$SETTING
if [ ! -x "$EXE" ]; then
    echo "$EXE" "is not found"
    exit 1
fi
export EXE

export PREFIX=$VERSION-$SETTING-$BENCHMARK-${INDIR//\//-}

export PPN=16

INPUTDIR="$BASEDIR/$BENCHMARK/$INDIR"
if [ ! -d "$INPUTDIR" ]; then
    echo "Input dir " "$INPUTDIR" "is missing"
    exit 0
fi

OUTDIR="$BASEDIR/results"
if [ ! -d "$OUTDIR" ]; then mkdir -p "$OUTDIR"; fi

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
    TMPDIR="$BASEDIR/tmp/$PREFIX-$n_node"
    qsub --mode script -A MPICH_MCS -t 60 -n $n_node \
        ./job.sh $n_node $EXE $INPUTDIR/$n_node $OUTDIR $TMPDIR $PREFIX-$n_node $NTIMES $INBUFSIZE $PARAMS
done

exit 0
