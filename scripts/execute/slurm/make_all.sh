DIR=$(pwd)
echo $DIR

FLAGS=$1

cd ../../../src
make clean
make CC=$FLAGS

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
cp ./wordcount $DIR/wordcount_pr
cp ./octree $DIR/octree_pr
cp ./bfs $DIR/bfs_pr

make clean
make wordcount CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT"
make octree CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT"
make bfs CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_prkvhint
cp ./octree $DIR/octree_prkvhint
cp ./bfs $DIR/bfs_prkvhint

make clean
make wordcount CC=$FLAGS CFLAGS="-DCOMPRESS"
make octree CC=$FLAGS CFLAGS="-DCOMPRESS -DKV_HINT"
make bfs CC=$FLAGS CFLAGS="-DCOMPRESS -DKV_HINT -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_cps
cp ./octree $DIR/octree_cps
cp ./bfs $DIR/bfs_cps

make clean
make wordcount CC=$FLAGS CFLAGS="-DCOMPRESS -DKV_HINT"
make octree CC=$FLAGS CFLAGS="-DCOMPRESS -DKV_HINT"
make bfs CC=$FLAGS CFLAGS="-DCOMPRESS -DKV_HINT -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_cpskvhint
cp ./octree $DIR/octree_cpskvhint
cp ./bfs $DIR/bfs_cpskvhint

make clean
cd ../src
make clean

cd $DIR
