#ifndef CONFIG_H
#define CONFIG_H

// memory
#define UNIT_SIZE               1024  // 1K

#define BLOCK_SIZE         (64*1024)  // 16M 

#define LOCAL_BUF_SIZE             1 // 1K
#define GLOBAL_BUF_SIZE         1024  // 1M
#define MAXMEM_SIZE     (4*1024*1024)  // 1G  
#define MAX_BLOCKS              1024  // 1024 blocks

#define PCS_PER_NODE            8
#define THS_PER_PROC            6

//#define TMP_BLOCK_SIZE     BLOCK_SIZE // 128M
// README
// LOCAL_BUF_SIZE <= GLOBAL_BUF_SIZE <= BLOCK_SIZE


#define UNIQUE_SIZE         (32*1024)

#define SET_COUNT           (1048576)
#define KEY_COUNT           (1048576)

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

#endif
