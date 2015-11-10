# README #

This repository contains the source code (src), benchmarks (benchmarks), documentations (doc), and relevant publications (publications) of the multithreaded MapReduce project.

The goal of the project is to enable more efficient big data processing on traditional HPC platforms.

## How do I Compile and Run ##
### Supported Platform ###
GNU/Linux

### Required Software ###
* A C++ compiler must be installed
* MPI library must be installed, MPICH, MVAPICH, etc
* OpenMP support
 
### How to Compile ###
```
$ cd /path/to/src
$ make
```

### How to Run ###
To run the wordcount benchmark:
```
$ cd /path/to/examples
$ make wordcount
$ mpiexec -n 2 -ppn 1 ./wordcount ./input_path
```

To run the bfs benchmark:
```
$ cd /path/to/examples
$ make bfs
$ mpiexec -n 2 -ppn 1 ./bfs N ./input_path
```

To run the octree_lg benchmark:
```
$ cd /path/to/examples
$ make ochre_move_lg
$ mpiexec -n 2 -ppn 1 ./octree_move_lg input_path 500(density)
```

# Related Projects #
MR-MPI [link](http://mapreduce.sandia.gov/)

KMR [link](http://mt.aics.riken.jp/kmr/)

# Reference #