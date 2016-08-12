DIR=$(pwd)
echo $DIR

FLAGS=$1

cd ../../src
make clean-all
make -f Makefile linux CC=$FLAGS

cd $DIR

cd ../../examples
make -f Makefile.linux clean
make -f Makefile.linux wordcount CFLAGS="-DGATHER_STAT=1 -DPAGESIZE=64" CC=$FLAGS
make -f Makefile.linux octree_move_lg CFLAGS="-DGATHER_STAT=1 -DPAGESIZE=64" CC=$FLAGS

cp ./wordcount $DIR/wordcount_p64
cp ./octree_move_lg $DIR/octree_move_lg_p64

cd ../../examples
make -f Makefile.linux clean
make -f Makefile.linux wordcount CFLAGS="-DGATHER_STAT=1 -DPAGESIZE=128" CC=$FLAGS
make -f Makefile.linux octree_move_lg CFLAGS="-DGATHER_STAT=1 -DPAGESIZE=128" CC=$FLAGS

cp ./wordcount $DIR/wordcount_p128
cp ./octree_move_lg $DIR/octree_move_lg_p128

cd ../../examples
make -f Makefile.linux clean
make -f Makefile.linux wordcount CFLAGS="-DGATHER_STAT=1 -DPAGESIZE=256" CC=$FLAGS
make -f Makefile.linux octree_move_lg CFLAGS="-DGATHER_STAT=1 -DPAGESIZE=256" CC=$FLAGS

cp ./wordcount $DIR/wordcount_p256
cp ./octree_move_lg $DIR/octree_move_lg_p256

make -f Makefile.linux clean
make -f Makefile.linux wordcount CFLAGS="-DGATHER_STAT=1 -DPAGESIZE=512" CC=$FLAGS
make -f Makefile.linux octree_move_lg CFLAGS="-DGATHER_STAT=1 -DPAGESIZE=512" CC=$FLAGS

cp ./wordcount $DIR/wordcount_p512
cp ./octree_move_lg $DIR/octree_move_lg_p512

cd $DIR
