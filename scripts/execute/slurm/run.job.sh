#!/bin/bash
#----------------------------------------------------
# submit one SLURM job
#
# Notes:
#
#   -- Launch this script by executing
#      "run.job.sh config subscript jobname N n job params prev"
#
#----------------------------------------------------

config=$1
subscript=$2
jobname=$3
N=$4
n=$5
job=$6
params=$7
prev=$8

source $config
statdir=$STATDIR
label=$STATLABEL
ntimes=$TESTTIMES
partition=$PARTITION
timelimit=$TIMELIMIT
installdir=$INSTALLDIR/bin

# output stat files
export MIMIR_OUTUT_STAT=1
export MIMIR_STAT_FILE=$statdir/$jobname-$label-$partition-$N-$n
export MIMIR_RECORD_PEAKMEM=1
export MIMIR_DBG_ALL=1         # always output debug mesage

echo $job,$params

for((i=0; i<$ntimes; i++))
do
    sbatch --job-name=$jobname --output=$jobname.o%j.out --error=$jobname.e%j.out\
    --partition=$partition -N $N -n $n --time=$timelimit                       \
    --dependency=afterany:$prev $subscript $n $installdir/$job "$params"       \
    | awk '{print $4}'
done
