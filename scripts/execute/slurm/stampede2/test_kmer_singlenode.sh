#/bin/hash
benchmark=kmer
datatype=1000genomes
dataset=HG00096
scratchdir=/scratch/05007/gwdtvjyu
homedir=/home1/05007/gwdtvjyu

input=$scratchdir/datasets/1000genomes/ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/data/HG00096/sequence_read/
output=$scratchdir/output/tmp

#export MIMIR_DISK_SIZE=32M
#export MIMIR_COMM_SIZE=32M
#export MIMIR_PAGE_SIZE=32M
export MIMIR_USE_MCDRAM=1

jobname=mimir
#jobname=mimir-d32M-c32M-p32M
#jobname=mimir-nomcdram-d32M-c32M-p32M
label=$benchmark-$datatype-$dataset
exe=jellyfish
params="mcount -t 1 -o $output -s 16M -m 22 --text $input"
statout=$homedir/results/kmer/stampede2-kmer-singlenode/

export  I_MPI_DEBUG=5

#../run.job.sh config.normal.h $jobname $label 1 34 $exe "$params" $statout
#../run.job.sh config.normal.h $jobname $label 1 68 $exe "$params" $statout 
#../run.job.sh config.normal.h $jobname $label 1 136 $exe "$params" $statout
#../run.job.sh config.normal.h $jobname $label 1 272 $exe "$params" $statout

#../run.job.sh config.flat-quadrant.h $jobname $label 1 34 $exe "$params" $statout
#../run.job.sh config.flat-quadrant.h $jobname $label 1 68 $exe "$params" $statout
../run.job.sh config.flat-quadrant.h $jobname $label 1 136 $exe "$params" $statout
#../run.job.sh config.flat-quadrant.h $jobname $label 1 272 $exe "$params" $statout

#../run.job.sh config.flat-snc4.h $jobname $label 1 34 $exe "$params" $statout
#../run.job.sh config.flat-snc4.h $jobname $label 1 68 $exe "$params" $statout
#../run.job.sh config.flat-snc4.h $jobname $label 1 136 $exe "$params" $statout
#../run.job.sh config.flat-snc4.h $jobname $label 1 272 $exe "$params" $statout
