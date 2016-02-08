#ifndef MTMR_CONFIG_H
#define MTMR_CONFIG_H

// memory
#define UNIT_SIZE                 32  // 1K

#define BLOCK_SIZE                 1  // 16M 

#define LOCAL_BUF_SIZE             1 // 1K
#define GLOBAL_BUF_SIZE            1  // 1M
#define MAXMEM_SIZE     (4*1024*1024)  // 1G  
#define MAX_BLOCKS              1024  // 1024 blocks

#define PCS_PER_NODE              2
#define THS_PER_PROC             10

//#define SET_COUNT                16
//#define KEY_COUNT           (4194304)

// convert
#define BUCKET_SIZE               4

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
#define TIMER_MAP_SENDKV                    2
#define TIMER_MAP_SERIAL                    3
#define TIMER_MAP_TWAIT                     4
#define TIMER_MAP_LOCK                      5

#define TIMER_REDUCE_KV2U                   6
#define TIMER_REDUCE_LCVT                   7
#define TIMER_REDUCE_MERGE                  8

#define TIMER_MAP_ADD                       9

#define TIMER_NUM                          10

#endif
