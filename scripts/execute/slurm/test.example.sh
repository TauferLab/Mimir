#/bin/hash

scratchdir=/scratch/dir
homedir=/home/dir
benchmark=
datatype=
dataset=

jobname=mimir
nnodes=
nprocs=
label=$benchmark-$datatype-$dataset
exe=wc
params=
statdir=

../run.job.sh config.example.h $jobname $label $nnodes $nprocs $exe $params $statdir
