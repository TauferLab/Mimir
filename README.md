# Overview
Mimir is a memory-efficient and scalable MapReduce implementation over MPI for 
supercomputing systems. The inefficient memory use of 
[MapReduce-MPI](http://mapreduce.sandia.gov/) drives the design of this new 
library.

The code is still under development. If you find any problems, please contact us
at taufer@utk.edu.

# Publications
1. Tao Gao, Yanfei Guo, Boyu Zhang, Pietro Cicotti, Yutong Lu, Pavan Balaji, and Michela
Taufer. On the Power of Combiner Optimizations in MapReduce over MPI Workflows. IEEE
International Conference on Parallel and Distributed Systems (ICPADS). Sentosa, Singapore, December 2018.

2. Tao Gao, Yanfei Guo, Boyu Zhang, Pietro Cicotti, Yutong Lu, Pavan Balaji, and
Michela Taufer. Mimir: Memory-Efficient and Scalable MapReduce for Large
Supercomputing Systems. The 31st IEEE International Parallel and Distributed
Processing Symposium (IPDPS) 2017.

3. Tao Gao, Yanfei Guo, Yanjie Wei, Bingqiang Wang, Yutong Lu, Pietro Cicotti,
Pavan Balaji, and Michela Taufer. Bloomfish: A Highly Scalable Distributed K-mer
Counting Framework. IEEE International Conference on Parallel and Distributed
Systems (ICPADS) 2017.

# Requirement
* A C++ compiler: supports C++ 11
* MPI implementation: supports MPI 3.0

# Getting Started
Mimir can be build and install from release tarball as follow.

```sh
./configure
make
make install
```

# Programming with Mimir
Mimir implements the MapReduce programming model. Here we use wordcount as an
example to show how to program with Mimir.

The MapReduce algorithm as the `map` and the `reduce` functions. 

```c++
void map(Readable<char *, void> *input, Writable<char *, uint64_t> *output, void *ptr)
{
    char *line = NULL;
    while (input->read(&line, NULL) == true) {
        char *saveptr = NULL;
        char *word = strtok_r(line, " ", &saveptr);
        while (word != NULL) {
            if (strlen(word) < 1024) {
                uint64_t one = 1;
                output->write(&word, &one);
                nwords += 1;
            }
            word = strtok_r(NULL, " ", &saveptr);
        }
    }
}

void reduce(Readable<char *, uint64_t> *input, Writable<char *, uint64_t> *output, void *ptr);
{
    char *key = NULL;
    uint64_t val = 0;
    uint64_t count = 0;
    while (input->read(&key, &val) == true) {
        count += val;
    }
    output->write(&key, &count);
}
```

After defining the `map` and `reduce` function, we can start creating the
runtime environment.  Since Mimir is built on top of MPI, the first step is
setup the MPI environment.

```c++
MPI_Init(NULL, NULL);
```

Next, the user need to create a `MimirContext`. The `MimirContext` is the
runtime abstraction of the MapReduce job.

```c++
MimirContext<char *, uint64_t, char *, void> *ctx
    = new MimirContext<char *, uint64_t, char *, void>(input, output,
                                                       MPI_COMM_WORLD, NULL)
```

The `MimirContext` template has four template parameters. These parameters
describes the types of the keys and values. In the example above, the input and
output keys are C string type and the output value is `uint64_t` type. Note that
the types here must match the ones used in the `map` and the `reduce` function.
Next the constructor takes four arguments which represents the lists of the
input and output files, the group of processes involved in the Mimir job
(represented as MPI communicator) and the optional pointer to the combiner
function.

In this example, we take the first command line arguments as the output file and
the rest arguments as input files.

```c++
std::string output = argv[1];
std::vector<std::string> input;()
for (int i = 2; i < argc; i++) {
input.push_back(argv[i]);
}
```

Once the Mimir context is created, we can start processing the input by calling
the `map` and the `reduce` function was follow. Note the user-defined `map` and
`reduce` functions are passed here as function pointers.

```c++
ctx->map(map);
nunique = ctx->reduce(reduce, NULL, true, "text");
```

After executing the MapReduce job, the runtime can be cleaned up by simply
destroying the mimir context. The application would also need to destroy the MPI
runtime at the end of it.

```c++
delete context;
MPI_Finalize();
```

# Running a Mimir Application
The user can run a Mimir application in the same way as they do with any regular
MPI application. That is launching the application using the launcher provided
by the MPI library or the cluster. The example above can be launched simply as
follow.

```
mpiexec -n 16 ./wordcount output.txt input0 input1 ...
```

This will start the wordcount example with 16 processes.

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

# Notes for Developers

For developers working on the project, please follow these steps to
build the library.

```sh
git clone https://github.com/TauferLab/Mimir.git
cd Mimir

./autogen.sh
./configure
make
make install
```
