DIR=$(pwd)
echo $DIR

module load impi

cd ../../../../src
make clean
make CFLAGS="-DUSE_MT_IO"

cd ../examples
make clean
make wordcount
#make bfs

cp ./wordcount $DIR
#cp ./bfs $DIR
cd $DIR
