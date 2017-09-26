#/bin/hash
scratchdir=/scratch/05007/gwdtvjyu
homedir=/home1/05007/gwdtvjyu

benchmark=bfs
datatype=graph500

export  I_MPI_DEBUG=5
export I_MPI_ADJUST_ALLTOALLV=2

#export MIMIR_READ_TYPE=mpiio
#export MIMIR_WRITE_TYPE=mpiio
export MIMIR_COMM_UNIT_SIZE=1K
#export MIMIR_SHUFFLE_TYPE=ia2av

output=$scratchdir/output/tmp
exe=bfs
statout=$scratchdir/results/bfs/stampede2-bfs-multinode/
prefixname=mimir

root=0

#export MIMIR_COMM_SIZE=64M
#jobname=$prefixname
#dataset=s25
#input=$scratchdir/datasets/graph500/25.16/
#label=$benchmark-$datatype-$dataset
#params="$root 33554432 $output $input"
#../run.job.sh config.normal.h $jobname $label 1 34 $exe "$params" $statout

#export MIMIR_COMM_SIZE=64M
#jobname=$prefixname
#dataset=s26
#input=$scratchdir/datasets/graph500/26.16/
#label=$benchmark-$datatype-$dataset
#params="$root 67108864 $output $input"
#../run.job.sh config.normal.h $jobname $label 2 68 $exe "$params" $statout

#export MIMIR_COMM_SIZE=64M
#jobname=$prefixname
#dataset=s27
#input=$scratchdir/datasets/graph500/27.16/
#label=$benchmark-$datatype-$dataset
#params="$root 134217728 $output $input"
#../run.job.sh config.normal.h $jobname $label 4 136 $exe "$params" $statout

#export MIMIR_COMM_SIZE=16M
#jobname=$prefixname-c16M
#dataset=s28
#input=$scratchdir/datasets/graph500/28.16/
#label=$benchmark-$datatype-$dataset
#params="$root 268435456 $output $input"
#../run.job.sh config.normal.h $jobname $label 8 272 $exe "$params" $statout

#export MIMIR_COMM_SIZE=32M
#jobname=$prefixname-c32M
#dataset=s29
#input=$scratchdir/datasets/graph500/29.16/
#label=$benchmark-$datatype-$dataset
#params="$root 536870912 $output $input"
#../run.job.sh config.normal.h $jobname $label 16 544 $exe "$params" $statout

#export MIMIR_COMM_SIZE=64M
#jobname=$prefixname
#dataset=s30
#input=$scratchdir/datasets/graph500/30.16/
#label=$benchmark-$datatype-$dataset
#params="$root 1073741824 $output $input"
#../run.job.sh config.normal.h $jobname $label 32 1088 $exe "$params" $statout

export MIMIR_COMM_SIZE=64M
jobname=$prefixname
dataset=s31
input=$scratchdir/datasets/graph500/31.16/
label=$benchmark-$datatype-$dataset
params="$root 2147483648 $output $input"
../run.job.sh config.normal.h $jobname $label 64 2176 $exe "$params" $statout

export MIMIR_COMM_SIZE=64M
jobname=$prefixname
dataset=s32
input=$scratchdir/datasets/graph500/32.16/
label=$benchmark-$datatype-$dataset
params="$root 4294967296 $output $input"
../run.job.sh config.normal.h $jobname $label 128 4352 $exe "$params" $statout

#export MIMIR_COMM_SIZE=64M
#jobname=$prefixname
#dataset=s33
#input=$scratchdir/datasets/graph500/33.16/
#label=$benchmark-$datatype-$dataset
#params="$root 8589934592 $output $input"
#../run.job.sh config.normal.h $jobname $label 256 8704 $exe "$params" $statout
