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
make wordcount CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT -DOUTPUT_KV"
make octree CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT"
make bfs CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_partreducekvhint
cp ./octree $DIR/octree_partreducekvhint
cp ./bfs $DIR/bfs_partreducekvhint

make clean
make wordcount CC=$FLAGS CFLAGS="-DCOMPRESS"
#make octree CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT"
#make bfs CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_cps

make clean
make wordcount CC=$FLAGS CFLAGS="-DCOMPRESS -DPART_REDUCE"
#make octree CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT"
#make bfs CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_cpspr

make clean
make wordcount CC=$FLAGS CFLAGS="-DKV_HINT -DPART_REDUCE -DCOMPRESS"
#make octree CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT"
#make bfs CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_cpsprkvhint
#cp ./octree $DIR/octree_partreducekvhint
#cp ./bfs $DIR/bfs_partreducekvhint

#cd ../src
#make clean
#make CC=
#cd ../examples
#make clean
#make wordcount CC= CFLAGS="-DPART_REDUCE -DKV_HINT" 
#cp ./wordcount $DIR/wordcount_ptkvnb

#cd ../src
#make clean
#make CC=mp CFLAGS="-DMTMR_MULTITHREAD"
#cd ../examples
#make clean
#make wordcount CC=mp CFLAGS="-DPART_REDUCE -DKV_HINT -DMTMR_MULTITHREAD"
#cp ./wordcount $DIR/wordcount_ptkvnbmt

make clean
cd ../src
make clean

cd $DIR
