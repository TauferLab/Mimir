#/bin/hash
scratchdir=/scratch/05007/gwdtvjyu
homedir=/home1/05007/gwdtvjyu

benchmark=bfs
datatype=graph500
dataset=s25

input=$scratchdir/datasets/graph500/25.16/
output=$scratchdir/output/tmp

export MIMIR_DISK_SIZE=32M
export MIMIR_COMM_SIZE=32M
export MIMIR_PAGE_SIZE=32M

jobname=mimir-d32M-c32M-p32M
label=$benchmark-$datatype-$dataset
exe=bfs
params="0 33554432 $output $input"
statout=$homedir/results/bfs/stampede2-singlenode/

export  I_MPI_DEBUG=5

#../run.job.sh config.normal.h $jobname $label 1 34 $exe "$params" $statout
#../run.job.sh config.normal.h $jobname $label 1 68 $exe "$params" $statout 
#../run.job.sh config.normal.h $jobname $label 1 136 $exe "$params" $statout
#../run.job.sh config.normal.h $jobname $label 1 272 $exe "$params" $statout

#../run.job.sh config.flat-quadrant.h $jobname $label 1 34 $exe "$params" $statout
#../run.job.sh config.flat-quadrant.h $jobname $label 1 68 $exe "$params" $statout
#../run.job.sh config.flat-quadrant.h $jobname $label 1 136 $exe "$params" $statout
#../run.job.sh config.flat-quadrant.h $jobname $label 1 272 $exe "$params" $statout

#../run.job.sh config.flat-snc4.h $jobname $label 1 34 $exe "$params" $statout
#../run.job.sh config.flat-snc4.h $jobname $label 1 68 $exe "$params" $statout
../run.job.sh config.flat-snc4.h $jobname $label 1 136 $exe "$params" $statout
#../run.job.sh config.flat-snc4.h $jobname $label 1 272 $exe "$params" $statout
