#/bin/hash
benchmark=wc
#datatype=synthetic
datatype=wikipedia
scratchdir=/scratch/05007/gwdtvjyu
homedir=/home1/05007/gwdtvjyu

output=$scratchdir/output/tmp
statout=$scratchdir/results/wc/stampede2-wc-singlenode/

export  I_MPI_DEBUG=5

datasets="16G-1m-1.0"
partition=normal

#for dataset in $datasets
#do
#    jobname=mimir-nomcdram
#    exe=wc
#    echo $dataset
#    label=$benchmark-$datatype-$dataset
#    input=$scratchdir/datasets/words/$dataset/
#    params="$output $input"
#    ../run.job.sh config.$partition.h $jobname $label 1 68 $exe "$params" $statout

#    jobname=mimir-cb-nomcdram
#    exe=wc_cb
#    label=$benchmark-$datatype-$dataset
#    input=$scratchdir/datasets/words/$dataset/
#    params="$output $input"
#    ../run.job.sh config.$partition.h $jobname $label 1 68 $exe "$params" $statout
#done

#export MIMIR_DISK_SIZE=32M
#export MIMIR_COMM_SIZE=32M
#export MIMIR_PAGE_SIZE=32M
export MIMIR_USE_MCDRAM=0

jobname=mimir
#jobname=mimir-nomcdram
#jobname=mimir-d32M-c32M-p32M
exe=wc_cb
dataset=34G
echo $dataset
label=$benchmark-$datatype-$dataset
input=$scratchdir/datasets/wikipedia/wikipedia_50GB/
filelist=$(../get_file_list.sh $input 36507222016 "*")
#filelist=$input
params="$output $filelist"
../run.job.sh config.$partition.h $jobname $label 1 68 $exe "$params" $statout

#jobname=mimir-cb-nomcdram
#exe=wc_cb
#dataset=16G
#echo $dataset
#label=$benchmark-$datatype-$dataset
#input=$scratchdir/datasets/wikipedia/
#filelist=$(../get_file_list.sh $input 17179869184 "*")
#params="$output $filelist"
#../run.job.sh config.$partition.h $jobname $label 1 34 $exe "$params" $statout

#jobname=mimir
#dataset=16G
#label=$benchmark-$datatype-$dataset
#input=$scratchdir/datasets/words/1M.16G/
#exe=wc_cb
#params="$output $input"
#../run.job.sh config.normal.h $jobname $label 1 68 $exe "$params" $statout

#../run.job.sh config.normal.h $jobname $label 1 68 $exe "$params" $statout 
#../run.job.sh config.development.h $jobname $label 1 68 $exe "$params" $statout
#../run.job.sh config.normal.h $jobname $label 1 272 $exe "$params" $statout

#../run.job.sh config.flat-quadrant.h $jobname $label 1 34 $exe "$params" $statout
#../run.job.sh config.flat-quadrant.h $jobname $label 1 68 $exe "$params" $statout
#../run.job.sh config.flat-quadrant.h $jobname $label 1 136 $exe "$params" $statout
#../run.job.sh config.flat-quadrant.h $jobname $label 1 272 $exe "$params" $statout

#../run.job.sh config.flat-snc4.h $jobname $label 1 34 $exe "$params" $statout
#../run.job.sh config.flat-snc4.h $jobname $label 1 68 $exe "$params" $statout
#../run.job.sh config.flat-snc4.h $jobname $label 1 136 $exe "$params" $statout
#../run.job.sh config.flat-snc4.h $jobname $label 1 272 $exe "$params" $statout
