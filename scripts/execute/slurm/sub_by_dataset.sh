#bin/bash

VERSION=mtmrmpi

BENCHMARK=""
SETLIST=""
DATATYPES=""
TESTTYPE=""
DATALIST=""
PARAMLIST=""
NTIMES=0
SCRIPT=""
PREJOB="none"

if [ $# == 9 ]
then
  BENCHMARK=$1
  SETLIST=$2
  DATATYPES=$3
  TESTTYPE=$4
  DATALIST=$5
  PARAMLIST=($6)
  NTIMES=$7
  SCRIPT=$8
  PREJOB=$9
else
  echo "./exe [benchmark] [setting list] [datatypes] [test type] [data list] [param list] [run times] [scripts name] [prev job]"
fi

source config.h

idx=0
for list in $DATALIST
do
  echo "datasize:"$list
  export DATASIZE=$list
  for DATATYPE in $DATATYPES
  do
    export INDIR=$BASEDIR/$BENCHMARK/$DATATYPE/$TESTTYPE/$list
    export PREFIX=$VERSION-$BENCHMARK-$DATATYPE-$TESTTYPE-$list
    for setting in $SETLIST
    do
      echo "setting:"$setting
      export EXE=$BENCHMARK"_"$setting
      export PARAM=${PARAMLIST[$idx]}
      export TAUFILE=$VERSION-$setting-$BENCHMARK-$DATATYPE-$TESTTYPE-tau.txt
      for((i=1; i<=$NTIMES; i++))
      do
        if [ $PREJOB == "none" ]
        then
          PREJOB=$(sbatch $SCRIPT $i | awk '{print $4}')
        else
          PREJOB=$(sbatch --dependency=afterany:$PREJOB $SCRIPT $i | awk '{print $4}')
        fi
        echo "jobid:"$PREJOB
      done
    done
  done
  let idx+=1
done
