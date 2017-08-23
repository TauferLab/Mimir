#!/bin/bash
#----------------------------------------------------
# submit one SLURM job
#
# Notes:
#
#   -- Launch this script by executing
#      "run.job.sh config jobname label N n job params statdir"
#
#----------------------------------------------------

config=$1
jobname=$2
label=$3
N=$4
n=$5
job=$6
params=$7
statdir=$8
prejob=$9

source $config

subscript=$SUBSCRIPT
ntimes=$TESTTIMES
partition=$PARTITION
timelimit=$TIMELIMIT
installdir=$INSTALLDIR/bin

# output stat files
#export MIMIR_OUTPUT_STAT=1
export MIMIR_STAT_FILE=$statdir/$jobname-$label-$partition-$N-$n
export MIMIR_RECORD_PEAKMEM=1
#export MIMIR_DBG_ALL=0         # always output debug message

#echo $N,$n,$job,$params

#for((i=0; i<$ntimes; i++))
#do

#echo $prejob

if [ -z "$prejob" ];
then
    sbatch --job-name=$jobname --output=$jobname.o%j.out --error=$jobname.e%j.out \
    --partition=$partition -N $N -n $n --time=$timelimit --export=all             \
    $subscript $N $n $installdir/$job "$params" | awk '{print $4}'
else
    sbatch --job-name=$jobname --output=$jobname.o%j.out --error=$jobname.e%j.out \
    --partition=$partition -N $N -n $n --time=$timelimit                          \
    --export=all --dependency=afterany:$prejob                                    \
    $subscript $N $n $installdir/$job "$params" | awk '{print $4}'
fi
#sleep 1
#done
