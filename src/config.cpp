/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#include "config.h"

// Buffer settings
int64_t COMM_BUF_SIZE = 64 * 1024 * 1024;
int64_t DATA_PAGE_SIZE = 64 * 1024 * 1024;
int64_t INPUT_BUF_SIZE = 64 * 1024 * 1024;
int BUCKET_COUNT = 1024 * 1024;
int MAX_RECORD_SIZE = 1024 * 1024;

// Settings
int SHUFFLE_TYPE = 0;
int MIN_SBUF_COUNT = 2;
int MAX_SBUF_COUNT = 5;
int COMM_UNIT_SIZE = 4096;
int READ_TYPE = 0;
int WRITE_TYPE = 0;

// Features
int WORK_STEAL = 0;
int MAKE_PROGRESS = 0;
int BIN_COUNT = 100;
int BALANCE_LOAD = 0;
double BALANCE_FACTOR = 1.5;
int BALANCE_ALG = 0;
int USE_MCDRAM = 1;

// Profile & Debug
int DBG_LEVEL = 0;
int OUTPUT_STAT = 0;
const char *STAT_FILE = NULL;
int RECORD_PEAKMEM = 0;
