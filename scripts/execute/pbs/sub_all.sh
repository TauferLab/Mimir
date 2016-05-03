#settings=('io_c_process' 'io_c_thread' 'io_mpi_blocking' 'io_mpi_nonblocking')
#settings=('io_mpi_nonblocking')
settings=('io_c_thread')

TIMES=10
START=64
END=64

PRE=$1

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
        PRE=$(qsub run.$k.sub)
      else
        PRE=$(qsub -W depend=afterany:$PRE run.$k.sub)
      fi
      echo $PRE
    done
    cd ..
  done
done
