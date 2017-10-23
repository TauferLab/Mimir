# Overview
Mimir is a memory-efficient and scalable MapReduce implementation over MPI for 
supercomputing systems. The inefficient memory use of 
[MapReduce-MPI](http://mapreduce.sandia.gov/) drives the design of this new 
library.

The code is still under development. If you find any problems, please contact us
at taogao@udel.edu.

# Publications
Tao Gao, Yanfei Guo, Boyu Zhang, Pietro Cicotti, Yutong Lu, Pavan Balaji, and
Michela Taufer. Mimir: Memory-Efficient and Scalable MapReduce for Large
Supercomputing Systems. The 31st IEEE International Parallel and Distributed
Processing Symposium (IPDPS) 2017.

Tao Gao, Yanfei Guo, Yanjie Wei, Bingqiang Wang, Yutong Lu, Pietro Cicotti,
Pavan Balaji, and Michela Taufer. Bloomfish: A Highly Scalable Distributed K-mer
Counting Framework. IEEE International Conference on Parallel and Distributed
Systems (ICPADS) 2017.

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
* MIMIR_MAX_RECORD_SIZE (default: 1M) --- maximum length of any
<key,value> pair

## Settings
* MIMIR_SHUFFLE_TYPE (default: a2av) --- a2av: MPI_Alltoallv; ia2av:
MPI_Ialltoallv
* MIMIR_MIN_COMM_BUF (default: 2) --- if the shuffle type is ia2av, it sets
the min communication buffer count
* MIMIR_MAX_COMM_BUF (default: 5) --- if the suffle type is ia2av, it sets
the max communication buffer count
* MIMIR_READ_TYPE (default: posix) --- read type (posix; mpiio)
* MIMIR_WRITE_TYPE (default: posix) --- write type (posix; mpiio)
* MIMIR_DIRECT_READ (default: off) --- direct read
* MIMIR_DIRECT_WRITE (default: off) --- direct write

## Features
* MIMIR_WORK_STEAL (default: off) --- enable/disable work stealing
* MIMIR_MAKE_PROGRESS (default: off) --- enable/disable aggressive
progress pushing during nonblocking communication
* MIMIR_BALANCE_LOAD (default: off) --- enable/disable load balancing
* MIMIR_BIN_COUNT (default: 1000) --- number of bins per process
* MIMIR_BALANCE_FACTOR (default: 1.5) --- the balance factor
* MIMIR_BALANCE_FREQ (default: 1) --- load balancing frequency
* MIMIR_USE_MCDRAM (default: off) --- if use MCDRAM when there is MCDRAM
* MIMIR_LIMIT_POWER (default: off) --- enable/disable power capping
* MIMIR_LIMIT_SCALE (default: 1.0) --- power capping percentage (e.g. 1.0 means no power capping)

## Stat & Debug
* MIMIR_OUTPUT_STAT (default: off) --- output stat file
* MIMIR_OUTPUT_TRACE (default: off) --- output trace file
* MIMIR_STAT_FILE (default: NULL) --- stat file name
* MIMIR_DBG_ALL (default: off) --- enable/disable debug message
