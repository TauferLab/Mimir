#!/bin/zsh

source config.h

echo "node:"$NODE
echo "ppn:"$PPN
echo "program:"$EXE
echo "param:"$PARAM
echo "input dir:"$INDIR
echo "prefix":$PREFIX
echo "output dir:"$OUTDIR
echo "temp dir:"$TMPDIR
echo "thread num:"$THRS
echo "use tau:"$USETAU
echo "tau file:"$TAUFILE

export NODE=1
export PPN=16
export EXE=/home/yguo/proj/mt-mrmpi/multithreaded-mapreduce/scripts/execute/bgq/wordcount_basic
export INDIR=/home/yguo/proj/mt-mrmpi/data
export OUTDIR=/home/yguo/proj/mt-mrmpi/out
export TMPDIR=/home/yguo/proj/mt-mrmpi/tmp
export THRS=16
export USETAU=no
export PREFIX=words

let NPROC=PPN*NODE
export OMP_NUM_THRADS=$THRS

runjob --np $NRPOC -p $PPN --block $COBALT_PARTNAME --verbose=INFO : ./$EXE $PARAM $INDIR $PREFIX $OUTDIR $TMPDIR $1

# if [ $USETAU == "true" ]
# then
#   pprof | grep "  Heap Memory Used (KB)" | \
#     awk -v np=$PPN -v data=$DATASIZE '{print data","np","i++","$2}' \
#     >> $TAUFILE
#   rm profile.*
# fi
