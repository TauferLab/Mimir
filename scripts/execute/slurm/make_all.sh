#!/bin/bash
DIR=$(pwd)
echo $DIR

cd ../../../

./configure --prefix=$1
make clean && make && make install

cd $DIR
