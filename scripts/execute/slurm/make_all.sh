DIR=$(pwd)
echo $DIR

FLAGS=$1

cd ../../../src
make clean
make CC=$FLAGS CFLAGS="-DMTMR_COMM_BLOCKING"

cd ../examples
make clean
make wordcount CC=$FLAGS
make octree CC=$FLAGS
make bfs CC=$FLAGS CFLAGS="-DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_basic
cp ./octree $DIR/octree_basic
cp ./bfs $DIR/bfs_basic

make clean
make wordcount CC=$FLAGS CFLAGS="-DPART_REDUCE"
make octree CC=$FLAGS CFLAGS="-DPART_REDUCE"
make bfs CC=$FLAGS CFLAGS="-DPART_REDUCE -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_partreduce
cp ./octree $DIR/octree_partreduce
cp ./bfs $DIR/bfs_partreduce

make clean
make wordcount CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT"
make octree CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT"
make bfs CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_kvhint
cp ./octree $DIR/octree_kvhint
cp ./bfs $DIR/bfs_kvhint

make clean
cd ../src
make clean

cd $DIR
