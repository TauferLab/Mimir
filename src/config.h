#ifndef MTMR_CONFIG_H
#define MTMR_CONFIG_H

//#define USE_MT_IO
//#define USE_MPI_IO
//#define USE_MPI_ASYN_IO

//#define MTMR_MULTITHREAD
//#define MTMR_COMM_BLOCKING

#define UNIT_1G_SIZE  (1024*1024*1024)
#define MAX_COMM_SIZE       0x40000000

#define MAX_STR_SIZE             8192
#define INPUT_BUF_COUNT             2

#define INPUT_SIZE                  8
#define BLOCK_SIZE                 64  // 16M 

#define LOCAL_BUF_SIZE            32  // 1K
#define GLOBAL_BUF_SIZE           64  // 1M
#define MAXMEM_SIZE                4  // 1G  
#define MAX_BLOCKS              1024  // 1024 blocks

#define PCS_PER_NODE              2
#define THS_PER_PROC             10

//#define SET_COUNT                16
//#define KEY_COUNT           (4194304)

// convert
#define BUCKET_SIZE               17

// type 
#define KV_TYPE                    0

// out of core 
#define OUT_OF_CORE                1
#define TMP_PATH                  "."

// others
#define MAXLINE                  2048

//#define GATHER_STAT                 1
#define SAFE_CHECK                  1
#define SHOW_BINDING                0

#define ENV_BIND_THREADS       "MTMR_BIND_THREADS"
#define ENV_SHOW_BINGDING      "MTMR_SHOW_BINDING"
#define ENV_PROCS_PER_NODE     "MTMR_NODE_PROCS"
#define ENV_THRS_PER_PROC      "MTMR_PROC_THRS"

#endif
