#settings=('io_c_process' 'io_c_thread' 'io_mpi_blocking' 'io_mpi_nonblocking')
settings=('io_mpi_nonblocking')

TIMES=1
START=1
END=1

PRE=-1

for setting in ${settings[@]}
do
  echo 'begin setting:'$setting'...'
  for((k=$START; k<=$END; k*=2))
  do
    for((i=1; i<=$TIMES; i++))
    do
      PRE=$(sbatch --dependency=afterok:$PRE $setting/run_wc.$k.sub)
    done
  done
  echo 'end setting:'$setting'.'
done
