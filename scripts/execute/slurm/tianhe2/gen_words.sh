#!/bin/bash
scratchdir=/HOME/nudt_tgao_1/BIGDATA/
homedir=/HOME/nudt_tgao_1/

n=1908874354
m=200000000
a=0.0
dirname=16G-200m-$a

node=1
proc=24

output=$scratchdir/words/$dirname/
statfile=$scratchdir/words/$dirname-stat/

echo $statfile

export MIMIR_OUTPUT_STAT=0

jobname=gen-join
label=empty
exe=gen_words
params="$n $output                  \
        --stat-file $statfile       \
        --zipf-n $m --zipf-alpha $a \
        --len-mean 8 --len-std 0    \
        -idxunique -disorder -exchange"

statout=empty

scriptdir=/HOME/nudt_tgao_1/Mimir/scripts/execute/slurm/
$scriptdir/run.job.sh config.bigdata.h $jobname $label $node $proc $exe "$params" $statout
