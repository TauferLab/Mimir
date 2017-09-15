#!/bin/bash
benchmark=wc
datatype=wikipedia
scratchdir=/HOME/nudt_tgao_1/BIGDATA/
homedir=/HOME/nudt_tgao_1/

output=$scratchdir/mimir_results/wc/wc
statout=$scratchdir/mimir_lb_results/tianhe2-wc/
scriptdir=/HOME/nudt_tgao_1/Mimir/scripts/execute/slurm/

export MIMIR_OUTPUT_STAT=1
export MIMIR_BALANCE_LOAD=0
export MIMIR_BALANCE_ALG=proc
export MIMIR_BALANCE_FREQ=5
export MIMIR_BALANCE_FACTOR=1.2
export MIMIR_BIN_COUNT=100
export MIMIR_DBG_ALL=1
export MIMIR_READ_TYPE=mpiio
export MIMIR_DIRECT_READ=1
export MIMIR_WRITE_TYPE=mpiio

jobstr=mimir
exe=wc

prejob=$1

jobname=$jobstr
#dataset=16G-1m-0.0
dataset=16G
label=$benchmark-$datatype-$dataset
#input=$scratchdir/words/$dataset
input=$scratchdir/Wikipedia/wikipedia_300GB
filelist=$(../get_file_list.sh $input 17179869184 "")
#filelist=$input
params="$output.0 $filelist"
prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout $prejob)
echo $prejob

jobname=$jobstr
#dataset=16G-1m-0.1
dataset=32G
label=$benchmark-$datatype-$dataset
#input=$scratchdir/words/$dataset
input=$scratchdir/Wikipedia/wikipedia_300GB
filelist=$(../get_file_list.sh $input 34359738368 "")
#filelist=$input
params="$output.1 $filelist"
prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 2 48 $exe "$params" $statout $prejob)
echo $prejob

jobname=$jobstr
#dataset=16G-1m-0.2
dataset=64G
label=$benchmark-$datatype-$dataset
#input=$scratchdir/words/$dataset
input=$scratchdir/Wikipedia/wikipedia_300GB
filelist=$(../get_file_list.sh $input 68719476736 "")
#filelist=$input
params="$output.2 $filelist"
prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 4 96 $exe "$params" $statout $prejob)
echo $prejob

jobname=$jobstr
#dataset=16G-1m-0.3
dataset=128G
label=$benchmark-$datatype-$dataset
#input=$scratchdir/words/$dataset
input=$scratchdir/Wikipedia/wikipedia_300GB
filelist=$(../get_file_list.sh $input 137438953472 "")
#filelist=$input
params="$output.3 $filelist"
prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 8 192 $exe "$params" $statout $prejob)
echo $prejob

jobname=$jobstr
#dataset=16G-1m-0.4
dataset=256G
label=$benchmark-$datatype-$dataset
#input=$scratchdir/words/$dataset
input=$scratchdir/Wikipedia/wikipedia_300GB
filelist=$(../get_file_list.sh $input 274877906944 "")
#filelist=$input
params="$output.4 $filelist"
prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 16 384 $exe "$params" $statout $prejob)
echo $prejob

jobname=$jobstr
#dataset=16G-1m-0.5
dataset=512G
label=$benchmark-$datatype-$dataset
#input=$scratchdir/words/$dataset
input=$scratchdir/Wikipedia/
filelist=$(../get_file_list.sh $input 549755813888 "")
#filelist=$input
params="$output.5 $filelist"
prejob=$($scriptdir/run.job.sh config.bigdata.h $jobname $label 32 768 $exe "$params" $statout $prejob)
echo $prejob
