#! /bin/zsh

DIR=$(pwd)
echo $DIR

cd ../../../src
make -f Makefile.bgq clean
make -f Makefile.bgq

cd ../examples
make -f Makefile.bgq clean
make -f Makefile.bgq wordcount
make -f Makefile.bgq octree
make -f Makefile.bgq bfs CFLAGS="-DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_basic
cp ./octree $DIR/octree_basic
cp ./bfs $DIR/bfs_basic

make -f Makefile.bgq clean
make -f Makefile.bgq wordcount CFLAGS="-DCOMBINE"
make -f Makefile.bgq octree CFLAGS="-DCOMBINE"
make -f Makefile.bgq bfs CFLAGS="-DCOMBINE -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_cb
cp ./octree $DIR/octree_cb
cp ./bfs $DIR/bfs_cb

make -f Makefile.bgq clean
make -f Makefile.bgq wordcount CFLAGS="-DKHINT -DVHINT"
make -f Makefile.bgq octree CFLAGS="-DKHINT -DVHINT"
make -f Makefile.bgq bfs CFLAGS="-DKHINT -DVHINT -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_kvhint
cp ./octree $DIR/octree_kvhint
cp ./bfs $DIR/bfs_kvhint

make -f Makefile.bgq clean
make -f Makefile.bgq wordcount CFLAGS="-DCOMBINE -DKHINT -DVHINT"
make -f Makefile.bgq octree CFLAGS="-DCOMBINE -DKHINT -DVHINT"
make -f Makefile.bgq bfs CFLAGS="-DCOMBINE -DKHINT -DVHINT -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_cbkvhint
cp ./octree $DIR/octree_cbkvhint
cp ./bfs $DIR/bfs_cbkvhint

make -f Makefile.bgq clean
cd ../src
make -f Makefile.bgq clean

cd $DIR
