DIR=$(pwd)
echo $DIR

CC=

cd ../../../../src
make clean
make CC=$CC CFLAGS="-DMTMR_COMM_BLOCKING"

cd ../examples
make clean
make wordcount CC=$CC

cp ./wordcount $DIR
cd $DIR
