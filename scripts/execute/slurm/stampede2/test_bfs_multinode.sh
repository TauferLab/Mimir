#/bin/hash
scratchdir=/scratch/05007/gwdtvjyu
homedir=/home1/05007/gwdtvjyu

benchmark=bfs
datatype=graph500

#export MIMIR_DISK_SIZE=16M
#export MIMIR_COMM_SIZE=16M
#export MIMIR_PAGE_SIZE=16M
#export MIMIR_USE_MCDRAM=1

export  I_MPI_DEBUG=5
export I_MPI_ADJUST_ALLTOALLV=2

output=$scratchdir/output/tmp
exe=bfs
statout=$homedir/results/bfs/stampede2-bfs-multinode/
jobname=mimir

root=0

export MIMIR_COMM_SIZE=64M
jobname=mimir
dataset=s25
input=$scratchdir/datasets/graph500/25.16/
label=$benchmark-$datatype-$dataset
params="$root 33554432 $output $input"
../run.job.sh config.normal.h $jobname $label 1 136 $exe "$params" $statout

export MIMIR_COMM_SIZE=16M
jobname=mimir-c16M
dataset=s26
input=$scratchdir/datasets/graph500/26.16/
label=$benchmark-$datatype-$dataset
params="$root 67108864 $output $input"
../run.job.sh config.normal.h $jobname $label 2 272 $exe "$params" $statout

export MIMIR_COMM_SIZE=32M
jobname=mimir-c32M
dataset=s27
input=$scratchdir/datasets/graph500/27.16/
label=$benchmark-$datatype-$dataset
params="$root 134217728 $output $input"
../run.job.sh config.normal.h $jobname $label 4 544 $exe "$params" $statout

export MIMIR_COMM_SIZE=64M
jobname=mimir
dataset=s28
input=$scratchdir/datasets/graph500/28.16/
label=$benchmark-$datatype-$dataset
params="$root 268435456 $output $input"
../run.job.sh config.normal.h $jobname $label 8 1088 $exe "$params" $statout

export MIMIR_COMM_SIZE=64M
jobname=mimir
dataset=s29
input=$scratchdir/datasets/graph500/29.16/
label=$benchmark-$datatype-$dataset
params="$root 536870912 $output $input"
../run.job.sh config.normal.h $jobname $label 16 2176 $exe "$params" $statout

export MIMIR_COMM_SIZE=64M
jobname=mimir
dataset=s30
input=$scratchdir/datasets/graph500/30.16/
label=$benchmark-$datatype-$dataset
params="$root 1073741824 $output $input"
../run.job.sh config.normal.h $jobname $label 32 4352 $exe "$params" $statout

#../run.job.sh config.normal.h $jobname $label 1 68 $exe "$params" $statout 
#../run.job.sh config.normal.h $jobname $label 1 136 $exe "$params" $statout
#../run.job.sh config.normal.h $jobname $label 1 272 $exe "$params" $statout

#../run.job.sh config.flat-quadrant.h $jobname $label 1 34 $exe "$params" $statout
#../run.job.sh config.flat-quadrant.h $jobname $label 1 68 $exe "$params" $statout
#../run.job.sh config.flat-quadrant.h $jobname $label 1 136 $exe "$params" $statout
#../run.job.sh config.flat-quadrant.h $jobname $label 1 272 $exe "$params" $statout

#../run.job.sh config.flat-snc4.h $jobname $label 1 34 $exe "$params" $statout
#../run.job.sh config.flat-snc4.h $jobname $label 1 68 $exe "$params" $statout
#../run.job.sh config.flat-snc4.h $jobname $label 1 136 $exe "$params" $statout
#../run.job.sh config.flat-snc4.h $jobname $label 1 272 $exe "$params" $statout
