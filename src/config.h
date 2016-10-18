#ifndef MTMR_CONFIG_H
#define MTMR_CONFIG_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

extern int COMM_UNIT_SIZE;
extern int MAX_KEY_SIZE;
extern int MAX_VALUE_SIZE;

extern int BUCKET_SIZE;
extern int MAX_BUCKET_SIZE;

/// if support out-of-core
extern int OUT_OF_CORE;

extern int MAX_BLOCKS;

extern int64_t GLOBAL_BUF_SIZE;
extern int64_t INPUT_SIZE;
extern int64_t BLOCK_SIZE;

extern int64_t INPUT_UNIT_SIZE;
extern int64_t UNIQUE_UNIT_SIZE;
extern int64_t SET_UNIT_SIZE;

extern int64_t MAXMEM_SIZE;

extern int MAX_STR_SIZE;

extern int DBG_LEVEL;

//#define MIMIR_COMM_NONBLOCKING

//#define MIMIR_DYNAMIC_BUCKET_SIZE
//#define MIMIR_USE_MPI_IO

//#define SAFE_CHECK

#endif
