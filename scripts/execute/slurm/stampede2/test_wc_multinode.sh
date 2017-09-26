#/bin/hash
benchmark=wc
datatype=wikipedia
dataset=weakscale2GB
scratchdir=/scratch/05007/gwdtvjyu
homedir=/home1/05007/gwdtvjyu

input=$scratchdir/datasets/wikipedia/
#input=$scratchdir/datasets/words/10M.16G/
output=$scratchdir/output/tmp

export MIMIR_COMM_UNIT_SIZE=1K

prefixname=mimir
label=$benchmark-$datatype-$dataset
exe=wc_cb
statout=$scratchdir/results/wc/stampede2-wc-multinode/

export  I_MPI_DEBUG=5
export I_MPI_ADJUST_ALLTOALLV=2

export MIMIR_COMM_SIZE=64M
jobname=mimir
filelist=$(../get_file_list.sh $input 2147483648 "*")
echo $filelist
params="$output $filelist"
../run.job.sh config.normal.h $jobname $label 1 34 $exe "$params" $statout

export MIMIR_COMM_SIZE=64M
jobname=mimir
filelist=$(../get_file_list.sh $input 4294967296 "*")
echo $filelist
params="$output $filelist"
../run.job.sh config.normal.h $jobname $label 2 68 $exe "$params" $statout

export MIMIR_COMM_SIZE=16M
jobname=mimir-c16M
filelist=$(../get_file_list.sh $input 8589934592 "*")
echo $filelist
params="$output $filelist"
../run.job.sh config.normal.h $jobname $label 4 136 $exe "$params" $statout

export MIMIR_COMM_SIZE=32M
jobname=mimir-c32M
filelist=$(../get_file_list.sh $input 17179869184 "*")
echo $filelist
params="$output $filelist"
../run.job.sh config.normal.h $jobname $label 8 272 $exe "$params" $statout

export MIMIR_COMM_SIZE=64M
jobname=mimir
filelist=$(../get_file_list.sh $input 34359738368 "*")
echo $filelist
params="$output $filelist"
../run.job.sh config.normal.h $jobname $label 16 544 $exe "$params" $statout

export MIMIR_COMM_SIZE=64M
jobname=mimir
filelist=$(../get_file_list.sh $input 68719476736 "*")
echo $filelist
params="$output $filelist"
../run.job.sh config.normal.h $jobname $label 32 1088 $exe "$params" $statout

export MIMIR_COMM_SIZE=64M
jobname=mimir
filelist=$(../get_file_list.sh $input 137438953472 "*")
echo $filelist
params="$output $filelist"
../run.job.sh config.normal.h $jobname $label 64 2176 $exe "$params" $statout

export MIMIR_COMM_SIZE=64M
jobname=mimir
filelist=$(../get_file_list.sh $input 274877906944 "*")
echo $filelist
params="$output $filelist"
../run.job.sh config.normal.h $jobname $label 128 4352 $exe "$params" $statout

#export MIMIR_COMM_SIZE=64M
#jobname=mimir
#filelist=$(../get_file_list.sh $input 549755813888 "*")
#echo $filelist
#params="$output $filelist"
#../run.job.sh config.normal.h $jobname $label 256 34816 $exe "$params" $statout

