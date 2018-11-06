#!/bin/bash -e
#
# (c) 2018 by The University of Tennessee Knoxville, Argonne National
#     Laboratory, San Diego Supercomputer Center, National University of
#     Defense Technology, National Supercomputer Center in Guangzhou,
#     and Sun Yat-sen University.
#
#     See COPYRIGHT in top-level directory.
#

nproc=4
testdir=bfs-test
infile=../datasets/graph_1024v
exedir=../../examples
root=0
N=1024

mkdir $testdir
cd $testdir

python ../bfs.py $root $N $infile bfs.out1

mkdir results
mpiexec -n $nproc $exedir/bfs $root $N ./results/ $infile
if [ $? -ne 0 ]
then
    echo "Error in bfs."
    cd ..
    rm -rf $testdir
    exit 1
fi

python ../bfs-p2l.py $root $N results bfs.out2
diff bfs.out1 bfs.out2
if [ $? -ne 0 ]
then
    echo "Error in bfs."
    cd ..
    rm -rf $testdir
    exit 1
fi


cd ..
rm -rf $testdir

echo "No Error"

exit 0
