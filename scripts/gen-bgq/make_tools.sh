#! /bin/zsh -e

soft delete +bgqtoolchain-gcc484
soft delete +mpiwrapper-mpich3-gcc
soft add +mpiwrapper-bgclang-mpi3

mpiclang++11 -o gen_key_normal gen_key_normal.cc
mpiclang++11 -o gen_wordcount_uniform gen_wordcount_uniform.cc
mpicc -std=c99 -o split-half split-half.c
mpiclang++11 -o split_text_files split_text_files.cpp
