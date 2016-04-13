#ifndef MTMR_CONFIG_H
#define MTMR_CONFIG_H

//#define USE_MT_IO
#define USE_MPI_IO
#define USE_MPI_ASYN_IO

#define UNIT_1K_SIZE            (1024)
#define UNIT_1M_SIZE       (1024*1024)
#define UNIT_1G_SIZE  (1024*1024*1024)

#define MAX_COMM_SIZE           1024

#define MAX_STR_SIZE            8192
#define INPUT_BUF_COUNT            2

#define INPUT_SIZE                 8
#define BLOCK_SIZE                64  // 16M 

#define LOCAL_BUF_SIZE            32  // 1K
#define GLOBAL_BUF_SIZE           64  // 1M
#define MAXMEM_SIZE                4  // 1G  
#define MAX_BLOCKS              1024  // 1024 blocks

#define PCS_PER_NODE              2
#define THS_PER_PROC             10

//#define SET_COUNT                16
//#define KEY_COUNT           (4194304)

// convert
#define BUCKET_SIZE               22

// type 
#define KV_TYPE                    0

// out of core 
#define OUT_OF_CORE                1
#define TMP_PATH                  "."

// others
#define MAXLINE                  2048

#define GATHER_STAT                 1
#define SAFE_CHECK                  1
#define SHOW_BINDING                0

#define ENV_BIND_THREADS       "MTMR_BIND_THREADS"
#define ENV_SHOW_BINGDING      "MTMR_SHOW_BINDING"
#define ENV_PROCS_PER_NODE     "MTMR_NODE_PROCS"
#define ENV_THRS_PER_PROC      "MTMR_PROC_THRS"


#define TIMER_MAP_PARALLEL                  0
#define TIMER_MAP_WAIT                      1
#define TIMER_MAP_USER                      2
#define TIMER_MAP_SENDKV                    3
#define TIMER_MAP_TWAIT                     4
#define TIMER_MAP_COMMSYN                   5
#define TIMER_MAP_COMM                      6
#define TIMER_MAP_LOCK                      7
#define TIMER_MAP_EXCHANGE                  8
#define TIMER_MAP_LASTEXCH                  9
#define TIMER_MAP_ALLTOALL                 10
#define TIMER_MAP_IALLTOALL                11
#define TIMER_MAP_SAVEDATA                 12
#define TIMER_MAP_ALLREDUCE                13
#define TIMER_MAP_WAITDATA                 14
#define TIMER_MAP_COPYDATA                 15
#define TIMER_MAP_IO                       16
#define TIMER_MAP_STAT                     17
#define TIMER_MAP_OPEN                     18          
#define TIMER_MAP_CLOSE                    19
#define TIMER_MAP_SEEK                     20
#define TIMER_REDUCE_CVT                   21
#define TIMER_REDUCE_KV2U                  22
#define TIMER_REDUCE_U2KMV                 23
#define TIMER_REDUCE_MERGE                 24
#define TIMER_REDUCE_LCVT                  25
#define TIMER_REDUCE_USER                  26
#define TIMER_REDUCE_STAGE1                27
#define TIMER_REDUCE_STAGE2                28
#define TIMER_COMPRESS_TOTAL               29
#define TIMER_COMPRESS_SCAN                30
#define TIMER_COMPRESS_GATHER              31
#define TIMER_COMPRESS_REDUCE              32
#define TIMER_NUM                          33

#define COUNTER_FILE_COUNT                  0
#define COUNTER_KV_NUMS                     1
#define COUNTER_SEND_BYTES                  2
#define COUNTER_RECV_BYTES                  3
#define COUNTER_BLOCK_BYTES                 4
#define COUNTER_COMPRESS_KVS                5
#define COUNTER_COMPRESS_OUTKVS             6
#define COUNTER_COMPRESS_BYTES              7
#define COUNTER_COMPRESS_RESULTS            8
#define COUNTER_BUCKET_BYTES                9  
#define COUNTER_UNIQUE_BYTES               10
#define COUNTER_SET_BYTES                  11
#define COUNTER_MV_BYTES                   12
#define COUNTER_RESULT_BYTES               13
#define COUNTER_NUM                        14

#endif
