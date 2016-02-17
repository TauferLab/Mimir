#!/bin/sh -l

DIR=../../examples
MPIRUN="mpiexec"
PHONE=3023335204

NPROCS=$1
NTHRS=$2
NOTIFY=$3
COMPILE=$4
TIMES=$5
FILE=$6
ARGS=$7

module load impi

if test "$#" -ne 6; then
  echo "usage: run_mpi.sh nprocs nthrs notify recompile program args"
fi

if test "$NOTIFY" -ne 0; then
  curl http://textbelt.com/text -d number=$PHONE -d "message=$FILE jobs start!"
fi

if test "$COMPILE" -ne 0; then
  CURDIR=$(pwd)
  cd ../../src && make clean && make && cd ../examples && rm $FILE && make $FILE && cd $CURDIR
fi

for((i=0; i<$TIMES; i++))
do
  MTMR_SHOW_BINDING=1 OMP_NUM_THREADS=$NTHRS $MPIRUN -n $NPROCS $DIR/$FILE $ARGS
done

if test "$NOTIFY" -ne 0; then
  curl http://textbelt.com/text -d number=$PHONE -d "message=$FILE jobs end!"
fi

