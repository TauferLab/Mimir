#/bin/hash
scratchdir=/scratch/05007/gwdtvjyu
homedir=/home1/05007/gwdtvjyu

dirname=33.16
scale=33

output=$scratchdir/datasets/graph500/$dirname/

export GRAPH500_GENERATOR_OUTDIR=$output

jobname=gen-graphs
label=empty
exe=graph500_generator
params="$scale"
statout=empty

export  I_MPI_DEBUG=5
export I_MPI_ADJUST_ALLTOALLV=2

echo $params
../run.job.sh config.normal.h $jobname $label 64 4096 $exe "$params" $statout
