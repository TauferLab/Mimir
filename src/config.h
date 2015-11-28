#ifndef CONFIG_H
#define CONFIG_H

// memory
#define UNIT_SIZE               1024  // 1K

#define BLOCK_SIZE              1024  // 16M 

#define LOCAL_BUF_SIZE             1  // 1K
#define GLOBAL_BUF_SIZE         1024  // 1M
#define MAXMEM_SIZE        1024*1024  // 1G  
#define MAX_BLOCKS              1024  // 1024 blocks


#define UNIQUE_POOL_SIZE     32*1024  // 32M
// README
// LOCAL_BUF_SIZE <= GLOBAL_BUF_SIZE <= BLOCK_SIZE

// convert
#define BUCKET_SIZE               22

// type 
#define KV_TYPE                    0

#define GATHER_STAT                1

// out of core 
#define OUT_OF_CORE           0
#define TMP_PATH            "."

// others
#define MAXLINE            2048

#endif
