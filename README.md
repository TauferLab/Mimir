# Overview
Mimir is a memory-efficient and scalable MapReduce implementation over MPI for 
supercomputing systems. The inefficient memory use of 
[MapReduce-MPI](http://mapreduce.sandia.gov/) drives the design of this new 
library. The work is summarized in IPDPS 2017 paper:

T. Gao, Y. Guo, B. Zhang, P. Cicotti, Y. Lu, P. Balaji, and M. Taufer.
Mimir: Memory-Efficient and Scalable MapReduce for Large Supercomputing Systems.
The 31st IEEE International Parallel and Distributed Processing Symposium (IPDPS)
2017.

The code is still under development. If you find any problems, please contact us
at taogao@udel.edu.

# Requirement
* A C++ compiler: supports C++ 11
* MPI implementation: supports MPI 3.0
* Automake tools

# Getting Started
* git clone https://github.com/TauferLab/Mimir.git
* cd Mimir
* autoreconf -i
* ./configure --perfix=/mimir/install/directory
* make
* make install

# Programming API
Come soon

# Configuration Parameters
Mimir provides environment variables to tune the library parameters.

## Buffers
* MIMIR_COMM_SIZE (default: 64M) --- communication bufer size
* MIMIR_PAGE_SIZE (default: 64M) --- data buffer unit size
* MIMIR_DISK_SIZE (default: 64M) --- disk I/O buffer size
* MIMIR_BUCKET_SIZE (default: 1M) --- hash bucket size used by reduce and
combine phases
* MIMIR_MAX_RECORD_SIZE (default: 1M) --- the maximum length of any
<key,value> pair

## Settings
* MIMIR_SHUFFLE_TYPE (default: a2av) --- a2av: MPI_Alltoallv; ia2av:
MPI_Ialltoallv
* MIMIR_COMM_UNIT_SIZE (default: 4K) --- the unit size to divide the
communication buffer
* MIMIR_MIN_COMM_BUF (default: 2) --- if the shuffle type is ia2av, it sets
the min communication buffer count
* MIMIR_MAX_COMM_BUF (default: 5) --- if the suffle type is ia2av, it sets
the amx communication buffer count
* MIMIR_READ_TYPE (default: posix) --- read type
* MIMIR_WRITE_TYPE (default: posix) --- write type

## Features
* MIMIR_WORK_STEAL (default: off) --- enable/disable work stealing
* MIMIR_MAKE_PROGRESS (default: off) --- enable/disable aggressive
progress pushing during nonblocking communication
* MIMIR_BALANCE_LOAD (default: off) --- enable/disable load balancing
* MIMIR_BIN_COUNT (default: 100) --- number of bins per process
* MIMIR_BALANCE_FACTOR (default: 1.5) --- the balance factor

## Stat & Debug
* MIMIR_OUTUT_STAT (default: off) --- output stat file
* MIMIR_STAT_FILE (default: NULL) --- stat file name
* MIMIR_RECORD_PEAKMEM --- enable/disable record peak memory usage
* MIMIR_DBG_ALL (default: off) --- enable/disable debug message
