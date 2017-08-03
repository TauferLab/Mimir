#/bin/hash
benchmark=wc
datatype=wikipedia
dataset=weakscale20GB
scratchdir=/scratch/05007/gwdtvjyu
homedir=/home1/05007/gwdtvjyu

input=$scratchdir/datasets/wikipedia/
#input=$scratchdir/datasets/words/10M.16G/
output=$scratchdir/output/tmp

#export MIMIR_DISK_SIZE=32M
#export MIMIR_COMM_SIZE=16M
#export MIMIR_PAGE_SIZE=32M
#export MIMIR_USE_MCDRAM=0

jobname=mimir
#jobname=mimir-nomcdram
#jobname=mimir-d32M-c16M-p16M
label=$benchmark-$datatype-$dataset
exe=wc_cb
statout=$homedir/results/wc/stampede2-wc-multinode/

export  I_MPI_DEBUG=5
export I_MPI_ADJUST_ALLTOALLV=2

export MIMIR_COMM_SIZE=64M
jobname=mimir
filelist=$(../get_file_list.sh $input 21474836480 "*")
echo $filelist
params="$output $filelist"
../run.job.sh config.normal.h $jobname $label 1 136 $exe "$params" $statout

export MIMIR_COMM_SIZE=16M
jobname=mimir-c16M
filelist=$(../get_file_list.sh $input 42949672960 "*")
echo $filelist
params="$output $filelist"
../run.job.sh config.normal.h $jobname $label 2 272 $exe "$params" $statout

export MIMIR_COMM_SIZE=32M
jobname=mimir-c32M
filelist=$(../get_file_list.sh $input 85899345920 "*")
echo $filelist
params="$output $filelist"
../run.job.sh config.normal.h $jobname $label 4 544 $exe "$params" $statout

export MIMIR_COMM_SIZE=64M
jobname=mimir
filelist=$(../get_file_list.sh $input 171798691840 "*")
echo $filelist
params="$output $filelist"
../run.job.sh config.normal.h $jobname $label 8 1088 $exe "$params" $statout

export MIMIR_COMM_SIZE=64M
jobname=mimir
filelist=$(../get_file_list.sh $input 343597383680 "*")
echo $filelist
params="$output $filelist"
../run.job.sh config.normal.h $jobname $label 16 2176 $exe "$params" $statout

export MIMIR_COMM_SIZE=64M
jobname=mimir
params="$output $input"
../run.job.sh config.normal.h $jobname $label 32 4352 $exe "$params" $statout

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
#../run.job.sh config.flat-snc4.h $jobname $label 1 136 $exe "$params" $statout
#../run.job.sh config.flat-snc4.h $jobname $label 1 272 $exe "$params" $statout
