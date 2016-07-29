DIR=$(pwd)
echo $DIR

CC=

#module load impi

which mpicxx

cd ../../../../src
make clean
make CC=$CC CFLAGS="-DMTMR_ZERO_COPY"

cd ../examples
make clean
make wordcount CC=$CC
#make bfs CC=$CC

cp ./wordcount $DIR
#cp ./bfs $DIR
cd $DIR
