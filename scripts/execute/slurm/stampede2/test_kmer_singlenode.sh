#/bin/hash
benchmark=kmer
datatype=1000genomes
scratchdir=/scratch/05007/gwdtvjyu
homedir=/home1/05007/gwdtvjyu

input=$scratchdir/datasets/1000genomes/
output=$scratchdir/output/tmp
statout=$homedir/results/kmer/stampede2-kmer-singlenode/

jobname=mimir
#jobname=mimir-nomcdram
#jobname=mimir-d32M-c32M-p32M
#jobname=mimir-nomcdram-d32M-c32M-p32M
#label=$benchmark-$datatype-$dataset
#params="mcount -t 1 -o $output -s 16M -m 22 --text $input"
export  I_MPI_DEBUG=5

export MIMIR_DISK_SIZE=16M
export MIMIR_COMM_SIZE=16M
export MIMIR_PAGE_SIZE=16M
export MIMIR_USE_MCDRAM=0
#partition=flat-quadrant
partition=normal

jobname=mimir-d16M-c16M-p16M
#jobname=mimir
exe=jellyfish
#dataset=16G
dataset=HG00096
label=$benchmark-$datatype-$dataset
input=$scratchdir/datasets/1000genomes/HG00096/sequence_read/
echo $input
#filelist=$(../get_file_list.sh $input 17179869184 "fastq")
filelist=$input
echo $filelist
params="mcount -t 1 -o $output -s 16M -m 22 --text $filelist"
../run.job.sh config.$partition.h $jobname $label 1 272 $exe "$params" $statout

#params="$output $filelist"


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
