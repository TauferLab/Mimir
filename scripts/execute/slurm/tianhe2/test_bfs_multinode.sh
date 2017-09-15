#/bin/hash
benchmark=bfs-join
datatype=graph500
scratchdir=/HOME/nudt_tgao_1/BIGDATA/
homedir=/HOME/nudt_tgao_1/

output=$scratchdir/mimir_results/bfs/bfs
exe=bfs_join
statout=$scratchdir/mimir_lb_results/bfs/tianhe2-bfs-multinode/
scriptdir=/HOME/nudt_tgao_1/Mimir/scripts/execute/slurm/

root=0
jobname=mimir
export MIMIR_OUTPUT_STAT=1
export MIMIR_BALANCE_LOAD=0
export MIMIR_BALANCE_ALG=proc
export MIMIR_DBG_ALL=1
export MIMIR_READ_TYPE=mpiio
export MIMIR_WRITE_TYPE=mpiio

dataset=s25
input=$scratchdir/graph500/s25/
label=$benchmark-$datatype-$dataset
params="$root 33554432  $output $input"
$scriptdir/run.job.sh config.bigdata.h $jobname $label 2 48 $exe "$params" $statout

dataset=s26
input=$scratchdir/graph500/s26/
label=$benchmark-$datatype-$dataset
params="$root 67108864 $output $input"
$scriptdir/run.job.sh config.bigdata.h $jobname $label 4 96 $exe "$params" $statout

dataset=s27
input=$scratchdir/graph500/s27/
label=$benchmark-$datatype-$dataset
params="$root 134217728 $output $input"
$scriptdir/run.job.sh config.bigdata.h $jobname $label 8 192 $exe "$params" $statout

dataset=s28
input=$scratchdir/graph500/s28/
label=$benchmark-$datatype-$dataset
params="$root 268435456 $output $input"
$scriptdir/run.job.sh config.bigdata.h $jobname $label 16 384 $exe "$params" $statout

#dataset=s29
#input=$scratchdir/graph500/s29/
#label=$benchmark-$datatype-$dataset
#params="$root 536870912 $output $input"
#$scriptdir/run.job.sh config.bigdata.h $jobname $label 16 384 $exe "$params" $statout

#dataset=s30
#input=$scratchdir/graph500/s30/
#label=$benchmark-$datatype-$dataset
#params="$root 1073741824 $output.s30 $input"
#$scriptdir/run.job.sh config.bigdata.h $jobname $label 64 1536 $exe "$params" $statout

#dataset=s31
#input=$scratchdir/graph500/s31/
#label=$benchmark-$datatype-$dataset
#params="$root 2147483648 $output $input"
#$scriptdir/run.job.sh config.bigdata.h $jobname $label 64 1536 $exe "$params" $statout

#dataset=s32
#input=$scratchdir/graph500/s32/
#label=$benchmark-$datatype-$dataset
#params="$root 4294967296 $output $input"
#$scriptdir/run.job.sh config.bigdata.h $jobname $label 128 3072 $exe "$params" $statout

#dataset=s33
#input=$scratchdir/graph500/s33/
#label=$benchmark-$datatype-$dataset
#params="$root 8589934592 $output $input"
#$scriptdir/run.job.sh config.bigdata.h $jobname $label 256 6144 $exe "$params" $statout
