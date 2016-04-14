settings=('io_c_process' 'io_c_thread' 'io_mpi_blocking' 'io_mpi_nonblocking')

for setting in ${settings[@]}
do
  echo $setting
  cd $setting
  rm mtmr-mpi-wc.*
  cd ..
done

