DIR=$(pwd)
echo $DIR

module load impi 

cd ../../../../src
make clean
make CFLAGS="-DUSE_MPI_IO -DUSE_MPI_ASYN_IO"

cd ../examples
make clean
make wordcount

cp ./wordcount $DIR

cd $DIR
