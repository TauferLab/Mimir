DIR=$(pwd)
echo $DIR

#FLAGS=$1

cd ../../../src
make clean
make

cd ../examples
make clean
make wordcount
#make octree CC=$FLAGS
#make bfs CC=$FLAGS CFLAGS="-DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_basic
#cp ./octree $DIR/octree_basic
#cp ./bfs $DIR/bfs_basic

make clean
make wordcount CFLAGS="-DCOMBINE"
#make octree CC=$FLAGS CFLAGS="-DPART_REDUCE"
#make bfs CC=$FLAGS CFLAGS="-DPART_REDUCE -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_cb
#cp ./octree $DIR/octree_pr
#cp ./bfs $DIR/bfs_pr

make clean
make wordcount CFLAGS="-DKHINT"
#make octree CC=$FLAGS CFLAGS="-DCOMPRESS"
#make bfs CC=$FLAGS CFLAGS="-DCOMPRESS -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_khint
#cp ./octree $DIR/octree_cps
#cp ./bfs $DIR/bfs_cps

make clean
make wordcount CFLAGS="-DVHINT"
#make octree CC=$FLAGS CFLAGS="-DKV_HINT"
#make bfs CC=$FLAGS CFLAGS="-DKV_HINT"
cp ./wordcount $DIR/wordcount_vhint
#cp ./octree $DIR/octree_kvhint
#cp ./bfs $DIR/bfs_kvhint

make clean
make wordcount CFLAGS="-DKHINT -DVHINT"
#make octree CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT"
#make bfs CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_kvhint
#cp ./octree $DIR/octree_prkvhint
#cp ./bfs $DIR/bfs_prkvhint

#make clean
#make wordcount CFLAGS="-DCOMPRESS -DKV_HINT"
#make octree CC=$FLAGS CFLAGS="-DCOMPRESS -DKV_HINT"
#make bfs CC=$FLAGS CFLAGS="-DCOMPRESS -DKV_HINT -DOUTPUT_RESULT"
#cp ./wordcount $DIR/wordcount_cpskvhint
#cp ./octree $DIR/octree_cpskvhint
#cp ./bfs $DIR/bfs_cpskvhint

#make clean
#make wordcount CC=$FLAGS CFLAGS="-DPART_REDUCE -DCOMPRESS -DKV_HINT"
#make octree CC=$FLAGS CFLAGS="-DPART_REDUCE -DCOMPRESS -DKV_HINT"
#make bfs CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT -DOUTPUT_RESULT"
#cp ./wordcount $DIR/wordcount_prcpskvhint
#cp ./octree $DIR/octree_prcpskvhint
#cp ./bfs $DIR/bfs_prkvhint

make clean
cd ../src
make clean

cd $DIR
