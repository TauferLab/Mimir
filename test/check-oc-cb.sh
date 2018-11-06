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
testdir=oc-test
infile=./dataset
exedir=../../examples
gendir=../../generator
pointcnt=8192

mkdir $testdir
cd $testdir

mkdir dataset
mpiexec -n $nproc $gendir/gen_3d_points test ./dataset 0 $pointcnt 0.5 1 0 0 0 $pointcnt normal > /dev/null
if [ $? -ne 0 ]
then
    echo "Error in generator."
    cd ..
    rm -rf $testdir
    exit 1
fi

mkdir results
mpiexec -n $nproc $exedir/oc_cb 0.01 ./results/oc.out $infile > /dev/null
if [ $? -ne 0 ]
then
    echo "Error in oc."
    cd ..
    rm -rf $testdir
    exit 1
fi
python ../verify-oc-results.py ./results $infile
if [ $? -ne 0 ]
then
    echo "Error in oc."
    cd ..
    rm -rf $testdir
    exit 1
fi

mpiexec -n $nproc $exedir/oc_cb 0.001 ./results/oc.out $infile > /dev/null
if [ $? -ne 0 ]
then
    echo "Error in oc."
    cd ..
    rm -rf $testdir
    exit 1
fi
python ../verify-oc-results.py ./results $infile
if [ $? -ne 0 ]
then
    echo "Error in oc."
    cd ..
    rm -rf $testdir
    exit 1
fi

cd ..
rm -rf $testdir

echo "No Error"

exit 0
