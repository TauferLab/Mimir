#ifndef CONFIG_H
#define CONFIG_H

// memory
#define UNIT_SIZE         64

#define BLOCK_SIZE         1
#define MAX_BLOCKS      1024
#define MAXMEM_SIZE      256 
#define LOCAL_BUF_SIZE     1
#define GLOBAL_BUF_SIZE    1

#define UNIQUE_POOL_SIZE  32

// README
// LOCAL_BUF_SIZE <= GLOBAL_BUF_SIZE <= BLOCK_SIZE

// convert
#define BUCKET_SIZE        3

// type 
#define KV_TYPE            0


// out of core 
#define OUT_OF_CORE        0
#define TMP_PATH          "."

// others
#define MAXLINE         2048

#endif
