#!/bin/bash
benchmark=
datatype=
scratchdir=
homedir=

# output dir of the benchmark
output=
# output dir of the stat file
statout=

# Configure Mimir here
export MIMIR_OUTPUT_STAT=1

jobstr=mimir
exe=

prejob=$1

dataset=
input=
jobname=$jobstr
label=$benchmark-$datatype-$dataset
params="$output $input"
run.job.sh config.example.h $jobname $label N n $exe "$params" $statout
