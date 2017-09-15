#!/bin/bash
benchmark=kmer
datatype=1000genomes
scratchdir=/HOME/nudt_tgao_1/BIGDATA/
homedir=/HOME/nudt_tgao_1/

input=$scratchdir/1000genomes/
output=$scratchdir/mimir_results/kmer/1000genomes
statout=$scratchdir/mimir_lb_results/kmer/tianhe2-kmer-singlenode/
scriptdir=/HOME/nudt_tgao_1/Mimir/scripts/execute/slurm/

export MIMIR_OUTPUT_STAT=1
export MIMIR_BALANCE_LOAD=0
export MIMIR_BALANCE_ALG=proc
export MIMIR_DBG_ALL=1
export MIMIR_READ_TYPE=mpiio
export MIMIR_WRITE_TYPE=mpiio

jobstr=mimir
exe=jellyfish

jobname=$jobstr
dataset=24G
label=$benchmark-$datatype-$dataset
filelist=$(../get_file_list.sh $input 25769803776 "fastq")
params="mcount -t 1 -o $output -s 256M -m 22 --text $filelist"
$scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout

jobname=$jobstr
dataset=48G
label=$benchmark-$datatype-$dataset
filelist=$(../get_file_list.sh $input 51539607552 "fastq")
params="mcount -t 1 -o $output -s 256M -m 22 --text $filelist"
$scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout

jobname=$jobstr
dataset=96G
label=$benchmark-$datatype-$dataset
filelist=$(../get_file_list.sh $input 103079215104 "fastq")
params="mcount -t 1 -o $output -s 256M -m 22 --text $filelist"
$scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout

jobname=$jobstr
dataset=192G
label=$benchmark-$datatype-$dataset
filelist=$(../get_file_list.sh $input 206158430208 "fastq")
params="mcount -t 1 -o $output -s 256M -m 22 --text $filelist"
$scriptdir/run.job.sh config.bigdata.h $jobname $label 1 24 $exe "$params" $statout
