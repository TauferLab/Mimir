DIR=$(pwd)
echo $DIR

CC=openmp

#module load impi

which mpicxx

cd ../../../../src
make clean
make CC=$CC CFLAGS="-DUSE_MT_IO -DMTMR_MULTITHREAD -DMTMR_ZERO_COPY"

cd ../examples
make clean
make wordcount CC=$CC
#make bfs CC=$CC

cp ./wordcount $DIR
#cp ./bfs $DIR
cd $DIR
