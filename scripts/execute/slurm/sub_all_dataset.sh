#bin/bash

source config.h

SETLIST=""
DATALIST=""
NTIMES=10
SUB=""
PRE="none"

EXE=bfs
BASICEXE=$EXE

if [ $# == 5 ]
then
  SETLIST=$1
  DATALIST=$2
  NTIMES=$3
  SUB=$4
  PRE=$5
else
  echo "./exe [setting list] [data list] [run times] [scripts name] [prev job]"
fi

SLURMSTR="Submitted batch job "
PARAM=65536

for list in $DATALIST
do
  echo "datasize:"$list
  export DATASIZE=$list
  for setting in $SETLIST
  do
    export EXE=$BASICEXE"_"$setting
    echo "program:"$EXE
    export PARAM=$PARAM
    echo "param:"$PARAM
    export PREFIX=mtmrmpi-$setting-$BENCHMARK-$SETTING-$DATASET-$DATASIZE
    for((i=1; i<=$NTIMES; i++))
    do
      if [ $PRE == "none" ]
      then
        PRE=$(sbatch $SUB $i)
        PRE=${PRE#$SLURMSTR}        
      else
        PRE=$(sbatch --dependency=afterany:$PRE $SUB $i)
        PRE=${PRE#$SLURMSTR}
      fi
      echo "jobid:"$PRE
    done
  done
  let PARAM*=2
done
