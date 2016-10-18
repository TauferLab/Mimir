#include "config.h"

/// unit size for comm buffer
int COMM_UNIT_SIZE=4096;

/// max key size
int MAX_KEY_SIZE=8192;

/// max value size
int MAX_VALUE_SIZE=8;

/// bucket with 2^x
int BUCKET_SIZE=20;
int MAX_BUCKET_SIZE=27;

int MAX_STR_SIZE=8192;

/// if support out-of-core
int OUT_OF_CORE=0;

int MAX_BLOCKS=1024;

int64_t GLOBAL_BUF_SIZE=64*1024*1024;
int64_t INPUT_SIZE=512*1024*1024;
int64_t BLOCK_SIZE=64*1024*1024;

int64_t INPUT_UNIT_SIZE=64*1024*1024;
int64_t UNIQUE_UNIT_SIZE=64*1024*1024;
int64_t SET_UNIT_SIZE=64*1024*1024;

int64_t MAXMEM_SIZE=16;

int DBG_LEVEL=0;
