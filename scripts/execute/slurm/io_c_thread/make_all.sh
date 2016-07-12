DIR=$(pwd)
echo $DIR

module load impi

cd ../../../../src
make clean
make CFLAGS="-DUSE_MT_IO" CC=tau

cd ../examples
make clean
make wordcount CC=tau
#make bfs

cp ./wordcount $DIR
#cp ./bfs $DIR
cd $DIR
