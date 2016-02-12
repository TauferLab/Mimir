#!/bin/sh -l

DIR=../../examples
MPIRUN="mpiexec"
PHONE=3022900216

NPROCS=$1
NTHRS=$2
NOTIFY=$3
COMPILE=$4
FILE=$5
ARGS=$6

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

OMP_NUM_THREADS=$NTHRS $MPIRUN -n $NPROCS $DIR/$FILE $ARGS

if test "$NOTIFY" -ne 0; then
  curl http://textbelt.com/text -d number=$PHONE -d "message=$FILE jobs end!"
fi

