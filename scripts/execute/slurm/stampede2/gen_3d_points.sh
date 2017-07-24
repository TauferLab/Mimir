#/bin/hash
scratchdir=/scratch/05007/gwdtvjyu
homedir=/home1/05007/gwdtvjyu

output=$scratchdir/datasets/points/s10

jobname=gen-points
label=empty
exe=gen_3d_points
params="test $output 0 16 0.5 1 0 0 0 16 normal"
statout=empty

export  I_MPI_DEBUG=5

../run.job.sh config.normal.h $jobname $label 1 64 $exe "$params" $statout
