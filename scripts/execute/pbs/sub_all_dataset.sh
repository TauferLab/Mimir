#bin/bash

SETLIST=""
DATALIST=""
NTIMES=10
SUB=""
PRE="none"

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

PREFIX="Submitted batch job "

for list in $DATALIST
do
  echo "datasize:"$list  
  export DATASIZE=$list
  for setting in $SETLIST
  do
    export EXE=$(echo wordcount_$setting)
    echo "program:"$EXE
    for((i=1; i<=$NTIMES; i++))
    do
      if [ $PRE == "none" ]
      then
        PRE=$(qsub $SUB)
      else
        PRE=$(qsub -W depend=afterany:$PRE $SUB)
      fi
      echo "jobid:"$PRE
    done
  done
done
