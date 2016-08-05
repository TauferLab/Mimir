#bin/bash
SETLIST=""
DATALIST=""
NTIMES=10
EXE=""
PRE="none"

if [ $# == 5 ]
then
  SETLIST=$1
  DATALIST=$2
  NTIMES=$3
  EXE=$4
  PRE=$5
else
  echo "./exe [setting list] [data list] [run times] [scripts name] [prev job]"
fi

PREFIX="Submitted batch job "

for list in $DATALIST
do
  echo $list
  for setting in $SETLIST
  do
    echo $setting
    cd $setting
    for((i=1; i<=$NTIMES; i++))
    do
      if [ $PRE == "none" ]
      then
        PRE=$(sbatch $EXE $list)
        PRE=${PRE#$PREFIX}        
      else
        PRE=$(sbatch --dependency=afterany:$PRE $EXE $list)
        PRE=${PRE#$PREFIX}
      fi
      echo $PRE
    done
    cd ..
  done
done
