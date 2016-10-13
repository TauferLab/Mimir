#! /bin/zsh

DIR=$(pwd)
echo $DIR

FLAGS=mpicc

cd ../../../src
make -f Makefile.bgq clean
make -f Makefile.bgq CC=$FLAGS

cd ../examples
make -f Makefile.bgq clean
make -f Makefile.bgq wordcount CC=$FLAGS
make -f Makefile.bgq octree CC=$FLAGS
make -f Makefile.bgq bfs CC=$FLAGS CFLAGS="-DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_basic
cp ./octree $DIR/octree_basic
cp ./bfs $DIR/bfs_basic

make -f Makefile.bgq clean
make -f Makefile.bgq wordcount CC=$FLAGS CFLAGS="-DPART_REDUCE"
make -f Makefile.bgq octree CC=$FLAGS CFLAGS="-DPART_REDUCE"
make -f Makefile.bgq bfs CC=$FLAGS CFLAGS="-DPART_REDUCE -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_pr
cp ./octree $DIR/octree_pr
cp ./bfs $DIR/bfs_pr

make -f Makefile.bgq clean
make -f Makefile.bgq wordcount CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT"
make -f Makefile.bgq octree CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT"
make -f Makefile.bgq bfs CC=$FLAGS CFLAGS="-DPART_REDUCE -DKV_HINT -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_prkvhint
cp ./octree $DIR/octree_prkvhint
cp ./bfs $DIR/bfs_prkvhint

make -f Makefile.bgq clean
make -f Makefile.bgq wordcount CC=$FLAGS CFLAGS="-DCOMPRESS"
make -f Makefile.bgq octree CC=$FLAGS CFLAGS="-DCOMPRESS -DKV_HINT"
make -f Makefile.bgq bfs CC=$FLAGS CFLAGS="-DCOMPRESS -DKV_HINT -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_cps
cp ./octree $DIR/octree_cps
cp ./bfs $DIR/bfs_cps

make -f Makefile.bgq clean
make -f Makefile.bgq wordcount CC=$FLAGS CFLAGS="-DCOMPRESS -DKV_HINT"
make -f Makefile.bgq octree CC=$FLAGS CFLAGS="-DCOMPRESS -DKV_HINT"
make -f Makefile.bgq bfs CC=$FLAGS CFLAGS="-DCOMPRESS -DKV_HINT -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_cpskvhint
cp ./octree $DIR/octree_cpskvhint
cp ./bfs $DIR/bfs_cpskvhint

make -f Makefile.bgq clean
make -f Makefile.bgq wordcount CC=$FLAGS CFLAGS="-DKV_HINT"
make -f Makefile.bgq octree CC=$FLAGS CFLAGS="-DKV_HINT"
make -f Makefile.bgq bfs CC=$FLAGS CFLAGS="-DKV_HINT -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_kvhint
cp ./octree $DIR/octree_kvhint
cp ./bfs $DIR/bfs_kvhint

make -f Makefile.bgq clean
make -f Makefile.bgq wordcount CC=$FLAGS CFLAGS="-DPART_REDUCE -DCOMPRESS -DKV_HINT"
make -f Makefile.bgq octree CC=$FLAGS CFLAGS="-DPART_REDUCE -DCOMPRESS -DKV_HINT"
make -f Makefile.bgq bfs CC=$FLAGS CFLAGS="-DPART_REDUCE -DCOMPRESS -DKV_HINT -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_cpsprkvhint
cp ./octree $DIR/octree_cpsprkvhint
cp ./bfs $DIR/bfs_cpsprkvhint

make -f Makefile.bgq clean
cd ../src
make -f Makefile.bgq clean

cd $DIR
