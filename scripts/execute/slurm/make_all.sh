DIR=$(pwd)
echo $DIR

cd ../../../src
make clean
make

cd ../examples
make clean
make wordcount
make octree
make bfs CPPFLAGS="-DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_basic
cp ./octree $DIR/octree_basic
cp ./bfs $DIR/bfs_basic

make clean
make wordcount CPPFLAGS="-DCOMBINE"
make octree CPPFLAGS="-DCOMBINE"
make bfs CPPFLAGS="-DCOMBINE -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_cb
cp ./octree $DIR/octree_cb
cp ./bfs $DIR/bfs_cb

make clean
make wordcount CPPFLAGS="-DKHINT -DVHINT"
make octree CPPFLAGS="-DKHINT -DVHINT"
make bfs CPPFLAGS="-DKHINT -DVHINT -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_kvhint
cp ./octree $DIR/octree_kvhint
cp ./bfs $DIR/bfs_kvhint

make clean
make wordcount CPPFLAGS="-DCOMBINE -DKHINT -DVHINT"
make octree CPPFLAGS="-DCOMBINE -DKHINT -DVHINT"
make bfs CPPFLAGS="-DCOMBINE -DKHINT -DVHINT -DOUTPUT_RESULT"
cp ./wordcount $DIR/wordcount_cbkvhint
cp ./octree $DIR/octree_cbkvhint
cp ./bfs $DIR/bfs_cbkvhint

make clean
cd ../src
make clean

cd $DIR
