#/bin/hash
scratchdir=/scratch/05007/gwdtvjyu
homedir=/home1/05007/gwdtvjyu

dirname=16G-1b-1.0
itemcount=2454267026

output=$scratchdir/datasets/words/$dirname/

jobname=gen-words
label=empty
exe=gen_words
params="$itemcount $output --zipf-n 1000000000 --zipf-alpha 1.0 -disorder -exchange"
statout=empty

export  I_MPI_DEBUG=5
export I_MPI_ADJUST_ALLTOALLV=2

echo $params
../run.job.sh config.flat-quadrant.h $jobname $label 2 128 $exe "$params" $statout
