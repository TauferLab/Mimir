settings=('io_c_process' 'io_c_thread' 'io_mpi_blocking' 'io_mpi_nonblocking')
#settings=('io_mpi_nonblocking')

module load impi
for setting in ${settings[@]}
do
  echo $setting
  cd $setting
  ./make_wc.sh
  cd ..
done

