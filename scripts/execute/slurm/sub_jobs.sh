#bin/bash

VERSION=mtmrmpi

BENCHMARK=""
SETLIST=""
DATATYPES=""
TESTTYPE=""
DATALIST=""
PARAMLIST=""
NTIMES=0
SCRIPT="run.sub"
PREJOB="none"

if [ $# == 8 ]
then
  BENCHMARK=$1
  SETLIST=$2
  DATATYPES=$3
  TESTTYPE=$4
  DATALIST=$5
  PARAMLIST=($6)
  NTIMES=$7
  PREJOB=$8
else
  echo "./exe [benchmark] [setting list] [datatypes] [test type] \
[data list] [param list] [run times] [prev job]"
fi

source config.h

export USETAU="no"

idx=0
nnode=1
for list in $DATALIST
do
  echo "datasize:"$list
  export DATASIZE=$list
  export NODE=$nnode
  for DATATYPE in $DATATYPES
  do
    echo $DATATYPE
    export INDIR=$BASEDIR/$BENCHMARK/$DATATYPE/$TESTTYPE/$list
    for setting in $SETLIST
    do
      echo "setting:"$setting
      export EXE=$BENCHMARK"_"$setting
      if [ $BENCHMARK != "wordcount" ]
      then
        export PARAM=${PARAMLIST[$idx]}
      fi
      export PREFIX=$VERSION-$setting-$BENCHMARK-$DATATYPE-$TESTTYPE-$list
      export TAUFILE=$VERSION-$setting-$BENCHMARK-$DATATYPE-$TESTTYPE-tau.txt
      for((i=1; i<=$NTIMES; i++))
      do
        export TESTINDEX=$i
        if [ $PREJOB == "none" ]
        then
          PREJOB=$(sbatch --nodes=$nnode $SCRIPT | awk '{print $4}')
        else
          PREJOB=$(sbatch --nodes=$nnode --dependency=afterany:$PREJOB $SCRIPT | awk '{print $4}')
        fi
        echo "jobid:"$PREJOB
      done
    done
  done
  if [ $TESTTYPE == "weekscale512M" ] || [ $TESTTYPE == "weekscale4G" ]
  then
    let nnode=nnode*2
  fi
  let idx+=1
done
