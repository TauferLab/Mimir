#!/bin/bash
benchmark=bfs
datatype=graph500
scratchdir=/HOME/nudt_tgao_1/BIGDATA/
homedir=/HOME/nudt_tgao_1/

output=$scratchdir/mimir_results/bfs/bfs
statout=$scratchdir/mimir_lb_results/bfs/tianhe2-bfs-singlenode/
scriptdir=/HOME/nudt_tgao_1/Mimir/scripts/execute/slurm/

export MIMIR_OUTPUT_STAT=1
export MIMIR_BALANCE_LOAD=0
export MIMIR_BALANCE_ALG=proc
export MIMIR_DBG_ALL=1
export MIMIR_READ_TYPE=mpiio
export MIMIR_WRITE_TYPE=mpiio

jobstr=mimir
exe=bfs_join

#dataset=s24
#input=$scratchdir/graph500/$dataset/
#jobname=$jobstr
#label=$benchmark-$datatype-$dataset
#params="0 16777216 $output.s24 $input"
#$scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout

dataset=s25
input=$scratchdir/graph500/$dataset/
jobname=$jobstr
label=$benchmark-$datatype-$dataset
params="0 33554432 $output.s25 $input"
$scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout
