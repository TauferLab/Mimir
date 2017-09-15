#!/bin/bash
benchmark=join
datatype=synthetic
scratchdir=/HOME/nudt_tgao_1/BIGDATA/
homedir=/HOME/nudt_tgao_1/

output=$scratchdir/mimir_results/join/join
statout=$scratchdir/mimir_lb_results/tianhe2-join/
scriptdir=/HOME/nudt_tgao_1/Mimir/scripts/execute/slurm/

export MIMIR_OUTPUT_STAT=1
export MIMIR_BALANCE_LOAD=0
export MIMIR_BALANCE_ALG=proc
export MIMIR_BALANCE_FREQ=1
export MIMIR_BALANCE_FACTOR=1.5
export MIMIR_DBG_ALL=1
export MIMIR_DBG_REPAR=1
export MIMIR_BIN_COUNT=1000
export MIMIR_READ_TYPE=mpiio
export MIMIR_DIRECT_READ=1
export MIMIR_WRITE_TYPE=mpiio

#jobstr=mimir-lb-proc-fq1-f1.5-b1000
jobstr=mimir
exe=join

prejob=$1

dataset=0.8b-100m-10-0.0
input1=$scratchdir/join/$dataset/
input2=$scratchdir/join/$dataset-stat/
jobname=$jobstr
label=$benchmark-$datatype-$dataset
params="$output.0.0 $input1 $input2"
prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
echo $prejob

dataset=0.8b-100m-10-0.1
input1=$scratchdir/join/$dataset/
input2=$scratchdir/join/$dataset-stat/
jobname=$jobstr
label=$benchmark-$datatype-$dataset
params="$output.0.1 $input1 $input2"
prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
echo $prejob

dataset=0.8b-100m-10-0.2
input1=$scratchdir/join/$dataset/
input2=$scratchdir/join/$dataset-stat/
jobname=$jobstr
label=$benchmark-$datatype-$dataset
params="$output.0.2 $input1 $input2"
prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
echo $prejob

dataset=0.8b-100m-10-0.3
input1=$scratchdir/join/$dataset/
input2=$scratchdir/join/$dataset-stat/
jobname=$jobstr
label=$benchmark-$datatype-$dataset
params="$output.0.3 $input1 $input2"
prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
echo $prejob

dataset=0.8b-100m-10-0.4
input1=$scratchdir/join/$dataset/
input2=$scratchdir/join/$dataset-stat/
jobname=$jobstr
label=$benchmark-$datatype-$dataset
params="$output.0.4 $input1 $input2"
prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
echo $prejob

dataset=0.8b-100m-10-0.5
input1=$scratchdir/join/$dataset/
input2=$scratchdir/join/$dataset-stat/
jobname=$jobstr
label=$benchmark-$datatype-$dataset
params="$output.0.5 $input1 $input2"
prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
echo $prejob

dataset=0.8b-100m-10-0.6
input1=$scratchdir/join/$dataset/
input2=$scratchdir/join/$dataset-stat/
jobname=$jobstr
label=$benchmark-$datatype-$dataset
params="$output.0.6 $input1 $input2"
prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
echo $prejob

dataset=0.8b-100m-10-0.7
input1=$scratchdir/join/$dataset/
input2=$scratchdir/join/$dataset-stat/
jobname=$jobstr
label=$benchmark-$datatype-$dataset
params="$output.0.7 $input1 $input2"
prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
echo $prejob

dataset=0.8b-100m-10-0.8
input1=$scratchdir/join/$dataset/
input2=$scratchdir/join/$dataset-stat/
jobname=$jobstr
label=$benchmark-$datatype-$dataset
params="$output.0.8 $input1 $input2"
prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
echo $prejob

dataset=0.8b-100m-10-0.9
input1=$scratchdir/join/$dataset/
input2=$scratchdir/join/$dataset-stat/
jobname=$jobstr
label=$benchmark-$datatype-$dataset
params="$output.0.9 $input1 $input2"
prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
echo $prejob

dataset=0.8b-100m-10-1.0
input1=$scratchdir/join/$dataset/
input2=$scratchdir/join/$dataset-stat/
jobname=$jobstr
label=$benchmark-$datatype-$dataset
params="$output.1.0 $input1 $input2"
prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
echo $prejob
