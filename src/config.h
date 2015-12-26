#ifndef CONFIG_H
#define CONFIG_H

// memory
#define UNIT_SIZE               1024  // 1K

#define BLOCK_SIZE                 4  // 16M 

#define LOCAL_BUF_SIZE             4  // 1K
#define GLOBAL_BUF_SIZE            4  // 1M
#define MAXMEM_SIZE      2*1024*1024  // 1G  
#define MAX_BLOCKS              1024  // 1024 blocks


#define TMP_BLOCK_SIZE       256*1024 // 128M
// README
// LOCAL_BUF_SIZE <= GLOBAL_BUF_SIZE <= BLOCK_SIZE


#define UNIQUE_SIZE             1024

#define BLOCK_COUNT             8192
#define KEY_COUNT            1048576

// convert
#define BUCKET_SIZE               22

// type 
#define KV_TYPE                    0

// out of core 
#define OUT_OF_CORE                0
#define TMP_PATH                  "."

// others
#define MAXLINE                  2048

#define GATHER_STAT                 1
#define SAFE_CHECK                  1

#endif
