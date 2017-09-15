#!/bin/bash
scratchdir=/HOME/nudt_tgao_1/BIGDATA/
homedir=/HOME/nudt_tgao_1/

n=102400000000
m=12800000000
a=1.0
dirname=102.4b-12.8b-10-$a

node=128
proc=3072

output=$scratchdir/join/$dirname/
statfile=$scratchdir/join/$dirname-stat/

echo $statfile

export MIMIR_DBG_ALL=1
export MIMIR_OUTPUT_STAT=0

jobname=gen-join
label=empty
exe=gen_words
params="$n $output                  \
        --stat-file $statfile       \
        --zipf-n $m --zipf-alpha $a \
        --len-mean 8 --len-std 0    \
        -withvalue --val-len 1      \
        -disorder -exchange -idxunique"

statout=empty

scriptdir=/HOME/nudt_tgao_1/Mimir/scripts/execute/slurm/
$scriptdir/run.job.sh config.bigdata.h $jobname $label $node $proc $exe "$params" $statout $1
