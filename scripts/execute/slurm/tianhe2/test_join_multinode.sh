#!/bin/bash
benchmark=join
datatype=synthetic
scratchdir=/HOME/nudt_tgao_1/BIGDATA/
homedir=/HOME/nudt_tgao_1/

output=$scratchdir/mimir_results/join/join
statout=$scratchdir/mimir_lb_results/tianhe2-join/
scriptdir=/HOME/nudt_tgao_1/Mimir/scripts/execute/slurm/

export MIMIR_OUTPUT_STAT=1
export MIMIR_BALANCE_LOAD=1
export MIMIR_BALANCE_ALG=proc
export MIMIR_BALANCE_FREQ=1
export MIMIR_BALANCE_FACTOR=1.5
export MIMIR_DBG_ALL=0
export MIMIR_DBG_REPAR=0
export MIMIR_BIN_COUNT=1000
export MIMIR_READ_TYPE=mpiio
export MIMIR_DIRECT_READ=1
export MIMIR_WRITE_TYPE=mpiio

jobstr=mimir-lb-proc-fq1-f1.5-b1000-split
#jobstr=mimir
exe=join_split

#alpha=1.0

prejob=$1

#dataset=0.8b-100m-10-1.0
#input1=$scratchdir/join/$dataset-stat/
#input2=$scratchdir/join/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="$output.$alpha.0 $input1 $input2"
#prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
#echo $prejob

#dataset=1.6b-200m-10-1.0
#input1=$scratchdir/join/$dataset-stat/
#input2=$scratchdir/join/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="$output.$alpha.1 $input1 $input2"
#prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 2 48 $exe "$params" $statout $prejob)
#echo $prejob

#dataset=3.2b-400m-10-1.0
#input1=$scratchdir/join/$dataset-stat/
#input2=$scratchdir/join/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="$output.$alpha.2 $input1 $input2"
#prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 4 96 $exe "$params" $statout $prejob)
#echo $prejob

#dataset=6.4b-800m-10-1.0
#input1=$scratchdir/join/$dataset-stat/
#input2=$scratchdir/join/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="$output.$alpha.3 $input1 $input2"
#prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 8 192 $exe "$params" $statout $prejob)
#echo $prejob

#dataset=12.8b-1.6b-10-1.0
#input1=$scratchdir/join/$dataset-stat/
#input2=$scratchdir/join/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="$output.$alpha.4 $input1 $input2"
#prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 16 384 $exe "$params" $statout $prejob)
#echo $prejob

#dataset=25.6b-3.2b-10-1.0
#input1=$scratchdir/join/$dataset-stat/
#input2=$scratchdir/join/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="$output.$alpha.5 $input1 $input2"
#prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 32 768 $exe "$params" $statout $prejob)
#echo $prejob

dataset=51.2b-6.4b-10-1.0
input1=$scratchdir/join/$dataset-stat/
input2=$scratchdir/join/$dataset/
jobname=$jobstr
label=$benchmark-$datatype-$dataset
params="$output.$alpha.6 $input1 $input2"
prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 64 1536 $exe "$params" $statout $prejob)
echo $prejob

#dataset=102.4b-12.8b-10-1.0
#input1=$scratchdir/join/$dataset-stat/
#input2=$scratchdir/join/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="$output.$alpha.6 $input1 $input2"
#prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 128 3072 $exe "$params" $statout $prejob)
#echo $prejob

#dataset=102.4b-12.8b-10-1.0
#input1=$scratchdir/join/$dataset-stat/
#input2=$scratchdir/join/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="$output.$alpha.7 $input1 $input2"
#prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 128 3072 $exe "$params" $statout $prejob)
#echo $prejob

#dataset=1b-64m-10-0.8
#input1=$scratchdir/join/$dataset-stat/
#input2=$scratchdir/join/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="$output.$alpha.8 $input1 $input2"
#prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
#echo $prejob

#dataset=1b-64m-10-0.9
#input1=$scratchdir/join/$dataset-stat/
#input2=$scratchdir/join/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="$output.$alpha.9 $input1 $input2"
#prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
#echo $prejob

#dataset=1b-64m-10-1.0
#input1=$scratchdir/join/$dataset-stat/
#input2=$scratchdir/join/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="$output.$alpha.10 $input1 $input2"
#prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
#echo $prejob

#dataset=51.2b-51.2m-10-$alpha
#input1=$scratchdir/db/$dataset-stat/
#input2=$scratchdir/db/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="$output.$alpha.64 $input1 $input2"
#prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 64 1536 $exe "$params" $statout $prejob)
#echo $prejob
