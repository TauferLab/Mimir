DIR=$(pwd)
echo $DIR
cd ../../../../src
make clean && make
cd ../examples
make clean
make wordcount
cp ./wordcount $DIR
cd $DIR
