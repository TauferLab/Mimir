#/bin/hash
benchmark=kmer
datatype=1000genomes
dataset=weakscale8G
scratchdir=/scratch/05007/gwdtvjyu
homedir=/home1/05007/gwdtvjyu

export MIMIR_COMM_UNIT_SIZE=1K

input=$scratchdir/datasets/1000genomes/
output=$scratchdir/output/tmp

jobname=mimir
label=$benchmark-$datatype-$dataset
exe=jellyfish
statout=$scratchdir/results/kmer/stampede2-kmer-multinode/

export  I_MPI_DEBUG=5
export I_MPI_ADJUST_ALLTOALLV=2

partition=normal

label=$benchmark-$datatype-$dataset
input=$scratchdir/datasets/1000genomes/

export MIMIR_COMM_SIZE=64M
jobname=mimir
filelist=$(../get_file_list.sh $input 8589934592 "fastq")
params="mcount -t 1 -o $output -s 256M -m 22 --text $filelist"
../run.job.sh config.$partition.h $jobname $label 1 68 $exe "$params" $statout

export MIMIR_COMM_SIZE=64M
jobname=mimir
filelist=$(../get_file_list.sh $input 17179869184 "fastq")
params="mcount -t 1 -o $output -s 256M -m 22 --text $filelist"
../run.job.sh config.$partition.h $jobname $label 2 136 $exe "$params" $statout

export MIMIR_COMM_SIZE=16M
jobname=mimir-c16M
filelist=$(../get_file_list.sh $input 34359738368 "fastq")
params="mcount -t 1 -o $output -s 256M -m 22 --text $filelist"
../run.job.sh config.$partition.h $jobname $label 4 272 $exe "$params" $statout

export MIMIR_COMM_SIZE=32M
jobname=mimir-c32M
filelist=$(../get_file_list.sh $input 68719476736 "fastq")
params="mcount -t 1 -o $output -s 256M -m 22 --text $filelist"
../run.job.sh config.$partition.h $jobname $label 8 544 $exe "$params" $statout

export MIMIR_COMM_SIZE=64M
jobname=mimir
filelist=$(../get_file_list.sh $input 137438953472 "fastq")
params="mcount -t 1 -o $output -s 256M -m 22 --text $filelist"
../run.job.sh config.$partition.h $jobname $label 16 1088 $exe "$params" $statout

export MIMIR_COMM_SIZE=64M
jobname=mimir
filelist=$(../get_file_list.sh $input 274877906944 "fastq")
params="mcount -t 1 -o $output -s 256M -m 22 --text $filelist"
../run.job.sh config.$partition.h $jobname $label 32 2176 $exe "$params" $statout

export MIMIR_COMM_SIZE=64M
jobname=mimir
filelist=$(../get_file_list.sh $input 549755813888 "fastq")
params="mcount -t 1 -o $output -s 256M -m 22 --text $filelist"
../run.job.sh config.$partition.h $jobname $label 64 4352 $exe "$params" $statout

export MIMIR_COMM_SIZE=64M
jobname=mimir
filelist=$(../get_file_list.sh $input 1099511627776 "fastq")
params="mcount -t 1 -o $output -s 256M -m 22 --text $filelist"
../run.job.sh config.$partition.h $jobname $label 128 8704 $exe "$params" $statout
