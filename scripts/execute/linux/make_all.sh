#! /bin/zsh

DIR=$(pwd)
echo $DIR

cd ../../../src
make -f Makefile.comet clean
make -f Makefile.comet

cd ../examples
make -f Makefile.linux clean
make -f Makefile.linux wordcount
make -f Makefile.linux octree
make -f Makefile.linux bfs CFLAGS="-DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_basic
cp ./octree $DIR/octree_basic
cp ./bfs $DIR/bfs_basic

make -f Makefile.linux clean
make -f Makefile.linux wordcount CFLAGS="-DCOMBINE"
make -f Makefile.linux octree CFLAGS="-DCOMBINE"
make -f Makefile.linux bfs CFLAGS="-DCOMBINE -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_cb
cp ./octree $DIR/octree_cb
cp ./bfs $DIR/bfs_cb

make -f Makefile.linux clean
make -f Makefile.linux wordcount CFLAGS="-DKHINT -DVHINT"
make -f Makefile.linux octree CFLAGS="-DKHINT -DVHINT"
make -f Makefile.linux bfs CFLAGS="-DKHINT -DVHINT -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_kvhint
cp ./octree $DIR/octree_kvhint
cp ./bfs $DIR/bfs_kvhint

make -f Makefile.linux clean
make -f Makefile.linux wordcount CFLAGS="-DCOMBINE -DKHINT -DVHINT"
make -f Makefile.linux octree CFLAGS="-DCOMBINE -DKHINT -DVHINT"
make -f Makefile.linux bfs CFLAGS="-DCOMBINE -DKHINT -DVHINT -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_cbkvhint
cp ./octree $DIR/octree_cbkvhint
cp ./bfs $DIR/bfs_cbkvhint

make -f Makefile.comet clean
cd ../src
make -f Makefile.comet clean

cd $DIR
