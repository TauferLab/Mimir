#!/bin/bash
benchmark=oc
datatype=synthetic
scratchdir=/HOME/nudt_tgao_1/BIGDATA/
homedir=/HOME/nudt_tgao_1/

output=$scratchdir/mimir_results/oc/oc
statout=$scratchdir/mimir_lb_results/tianhe2-oc/
scriptdir=/HOME/nudt_tgao_1/Mimir/scripts/execute/slurm/

export MIMIR_OUTPUT_STAT=1
export MIMIR_BALANCE_LOAD=0
export MIMIR_BALANCE_ALG=proc
export MIMIR_BALANCE_FREQ=5
export MIMIR_BALANCE_FACTOR=1.2
export MIMIR_DBG_ALL=1
export MIMIR_DBG_REPAR=1
export MIMIR_BIN_COUNT=1000
export MIMIR_READ_TYPE=mpiio
export MIMIR_DIRECT_READ=1
export MIMIR_WRITE_TYPE=mpiio

#jobstr=mimir-d0.0001-lb-proc-f1.2-b1000
jobstr=mimir-d0.0001
exe=oc

density=0.0001

prejob=$1

dataset=UN-s29
input=$scratchdir/points/$dataset/
jobname=$jobstr
label=$benchmark-$datatype-$dataset
params="$density $output.$alpha.1 $input"
prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
echo $prejob

#dataset=1S-0.2-s29
#input=$scratchdir/points/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="$density $output.$alpha.2 $input"
#prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
#echo $prejob

#dataset=1S-0.3-s29
#input=$scratchdir/points/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="$density $output.$alpha.4 $input"
#prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
#echo $prejob

#dataset=1S-0.4-s29
#input=$scratchdir/points/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="$density $output.$alpha.8 $input"
#prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
#echo $prejob

#dataset=1S-0.5-s29
#input=$scratchdir/points/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="$density $output.$alpha.16 $input"
#prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
#echo $prejob

#dataset=1S-0.6-s29
#input=$scratchdir/points/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="$density $output.$alpha.32 $input"
#prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
#echo $prejob

#dataset=1S-0.7-s29
#input=$scratchdir/points/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="$density $output.$alpha.64 $input"
#prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
#echo $prejob

#dataset=1S-0.8-s29
#input=$scratchdir/points/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="$density $output.$alpha.128 $input"
#prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
#echo $prejob

#dataset=1S-0.9-s29
#input=$scratchdir/points/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="$density $output.$alpha.256 $input"
#prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
#echo $prejob

#dataset=1S-1.0-s29
#input=$scratchdir/points/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="$density $output.$alpha.512 $input"
#prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
#echo $prejob
