#ifndef CONFIG_H
#define CONFIG_H

// memory
#define UNIT_SIZE   1024*1024

#define BLOCK_SIZE        64
#define MAX_BLOCKS        16
#define MAXMEM_SIZE     1024
#define LOCAL_BUF_SIZE     1
#define GLOBAL_BUF_SIZE    4

// README
// LOCAL_BUF_SIZE <= GLOBAL_BUF_SIZE <= BLOCK_SIZE

// type 
#define KV_TYPE            0

// out of core 
#define OUT_OF_CORE        0
#define TMP_PATH          "."

// others
#define MAXLINE         2048

#endif
