#/bin/hash
scratchdir=/HOME/nudt_tgao_1/BIGDATA/
homedir=/HOME/nudt_tgao_1/

output=$scratchdir/graph500/s33
export GRAPH500_GENERATOR_OUTDIR=$output

jobname=gen-graphs
label=empty
exe=graph500_generator
params="33"
statout=empty

scriptdir=/HOME/nudt_tgao_1/Mimir/scripts/execute/slurm/
$scriptdir/run.job.sh config.bigdata.h $jobname $label 86 2048 $exe "$params" $statout
