DIR=$(pwd)
echo $DIR

cd ../../../../src
make clean
make CFLAGS+="-DUSE_MPI_IO"

cd ../examples
make clean
make wordcount

cp ./wordcount $DIR
cd $DIR
