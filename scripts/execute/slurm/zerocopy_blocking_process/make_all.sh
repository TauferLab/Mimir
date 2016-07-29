DIR=$(pwd)
echo $DIR

CC=

which mpicxx

cd ../../../../src
make clean
make CC=$CC CFLAGS="-DMTMR_COMM_BLOCKING -DMTMR_ZERO_COPY"

cd ../examples
make clean
make wordcount CC=$CC
#make bfs CC=$CC

cp ./wordcount $DIR
#cp ./bfs $DIR
cd $DIR
