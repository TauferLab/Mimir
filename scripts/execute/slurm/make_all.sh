DIR=$(pwd)
echo $DIR

FLAGS=$1

cd ../../../src
make clean
make CC=$FALGS CFLAGS="-DMTMR_COMM_BLOCKING"

cd ../examples
make clean
make wordcount CC=$FLAGS
make octree_move_lg CC=$FALGS
cp ./wordcount $DIR/wordcount_basic
cp ./octree_move_lg $DIR/octree_move_lg_basic

make clean
make wordcount CC=$FLAGS CFLAGS="-DPART_REDUCE"
make octree_move_lg CC=$FLAGS CFLAGS="-DPART_REDUCE"
cp ./wordcount $DIR/wordcount_partreduce
cp ./octree_move_lg $DIR/octree_move_lg_partreduce

make clean
make wordcount CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT"
make octree_move_lg CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT"
cp ./wordcount $DIR/wordcount_kvhint
cp ./octree_move_lg $DIR/octree_move_lg_kvhint

make clean
cd ../src
make clean

cd $DIR
