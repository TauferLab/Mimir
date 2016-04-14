settings=('io_c_process' 'io_c_thread' 'io_mpi_blocking' 'io_mpi_nonblocking')
#settings=('io_mpi_nonblocking')

TIMES=20
START=2
END=2

PRE="none"

for((k=$START; k<=$END; k*=2))
do
  for setting in ${settings[@]}
  do
    echo $setting
    cd $setting
    for((i=1; i<=$TIMES; i++))
    do
      if [ $PRE == "none" ]
      then
        PRE=$(qsub run_wc.$k.sub)
      else
        PRE=$(qsub -W depend=afterany:$PRE run_wc.$k.sub)
      fi
      echo $PRE
    done
    cd ..
  done
done
