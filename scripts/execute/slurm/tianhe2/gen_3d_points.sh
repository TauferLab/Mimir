#!/bin/bash
scratchdir=/HOME/nudt_tgao_1/BIGDATA/
homedir=/HOME/nudt_tgao_1/

nitems=22369621
#nitems=1024
stdiv=0.9
#output=$scratchdir/points/1S-$stdiv-s29
output=$scratchdir/points/UN-s29
nnode=1
nproc=24

jobname=gen-points
label=empty
exe=gen_3d_points

params="test $output 0 $nitems $stdiv 1 0 0 0 $nitems uniform"
statout=empty

export  I_MPI_DEBUG=5

scriptdir=/HOME/nudt_tgao_1/Mimir/scripts/execute/slurm/
$scriptdir/run.job.sh config.bigdata.h $jobname $label $nnode $nproc $exe "$params" $statout
