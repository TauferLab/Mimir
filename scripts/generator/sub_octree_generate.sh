#bin/bash
DATALIST=""
EXE=""
PRE="none"
DIST="normal"
STDIV=0.5
NFILE=24
COUNT=302080
DEPEN="no"
ADDCNT="no"

#BASEDIR=/oasis/scratch/comet/taogao/temp_project/octree/1S/onenode
BASEDIR=/oasis/scratch/comet/taogao/temp_project/octree/1S/onenode/1T_basic

if [ $# == 3 ]
then
  DATALIST=$1
  EXE=$2
  PRE=$3
else
  echo "./exe [data list] [scripts name] [prev job]"
fi

PREFIX="Submitted batch job "

for((i=0;i<64;i++))
do
  list=16G.$i
  cnt=$(ls $BASEDIR/$list/ | wc -l)
  echo $cnt
  sbatch $EXE $cnt $NFILE $COUNT $DIST $STDIV $BASEDIR/$list/ $BASEDIR/$list.points/
done

#for list in $DATALIST
#do
#  echo $list
#  if [ $PRE == "none" -o $DEPEN=="no" ]
#  then
    #echo $list
    #echo $list.points
#    PRE=$(sbatch $EXE 0 $NFILE $COUNT $DIST $STDIV $BASEDIR/$list/ $BASEDIR/$list.points/)
#    PRE=${PRE#$PREFIX} 
#  else
#    PRE=$(sbatch --dependency=afterany:$PRE $EXE 0 $NFILE $COUNT $DIST $STDIV $BASEDIR/$list/ $BASEDIR/$list.points/)
#    PRE=${PRE#$PREFIX}
#  fi
#  if [ $ADDCNT == "yes" ]
#  then
#    let "COUNT=COUNT*2"
#  fi
#  echo $COUNT
#  echo $PRE
#done
