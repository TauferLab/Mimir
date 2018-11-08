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
testdir=join-test
infile=./dataset
exedir=../../examples
gendir=../../generator
itemcnt=10240

mkdir $testdir
cd $testdir

mkdir dataset1
mkdir dataset2
mpiexec -n $nproc $gendir/gen_3d_points test ./dataset1 0 $pointcnt 0.5 1 0 0 0 $pointcnt normal > /dev/null
mpiexec -n $nproc $gendir/gen_words $itemcnt dataset1/infile1         \
    --zipf-n 1000 --zipf-alpha 0.5 --stat-file dataset2/infile2       \
    -withvalue -disorder -exchange > /dev/null
if [ $? -ne 0 ]
then
    echo "Error in generator."
    cd ..
    rm -rf $testdir
    exit 1
fi

mkdir results
mpiexec -n $nproc $exedir/join ./results/join.out ./dataset1 ./dataset2 > /dev/null
if [ $? -ne 0 ]
then
    echo "Error in join."
    cd ..
    rm -rf $testdir
    exit 1
fi

python ../join.py dataset1 dataset2 ./results.txt
cat ./results.txt | sort -k1 -k2 -k3 -n > join.sort1.out
cat ./results/* | sort -k1 -k2 -k3 -n > join.sort2.out
diff join.sort1.out join.sort2.out > /dev/null
if [ $? -ne 0 ]
then
    echo "Error in join."
    cd ..
    #rm -rf $testdir
    exit 1
fi

cd ..
rm -rf $testdir

echo "No Error"

exit 0
