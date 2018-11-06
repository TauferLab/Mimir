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
testdir=wc-test
infile=./dataset
exedir=../../examples
gendir=../../generator
wordcnt=1048576

mkdir $testdir
cd $testdir

mkdir dataset
mpiexec -n $nproc $gendir/gen_words $wordcnt dataset/infile   \
    --zipf-n 1000 --zipf-alpha 0.5 -disorder -exchange > /dev/null
if [ $? -ne 0 ]
then
    echo "Error in generator."
    cd ..
    rm -rf $testdir
    exit 1
fi

python ../wordcount.py $infile wc.out > /dev/null
cat wc.out | sort -k1 -k2 -n > wc.sort1.out

mpiexec -n $nproc $exedir/wc wc.out $infile > /dev/null
cat wc.out$nproc.* | sort -k1 -k2 -n > wc.sort2.out
diff wc.sort1.out wc.sort2.out > /dev/null
if [ $? -ne 0 ]
then
    echo "Error in wc."
    cd ..
    rm -rf $testdir
    exit 1
fi

cd ..
rm -rf $testdir

echo "No Error"

exit 0
