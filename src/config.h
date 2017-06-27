/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MTMR_CONFIG_H
#define MTMR_CONFIG_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

//#define MPI_FETCH_AND_OP

// Buffer settings
extern int BUCKET_COUNT;
extern int64_t COMM_BUF_SIZE;
extern int64_t DATA_PAGE_SIZE;
extern int64_t INPUT_BUF_SIZE;
extern int MAX_RECORD_SIZE;
extern int MIN_SBUF_COUNT;
extern int MAX_SBUF_COUNT;
extern int COMM_UNIT_SIZE;

// Settings
extern int WORK_STEAL;
extern int READER_TYPE;
extern int WRITER_TYPE;
extern int SHUFFLE_TYPE;
extern int MAKE_PROGRESS;
extern int BALANCE_LOAD;
extern int BIN_COUNT;
extern float BALANCE_FACTOR;
//extern int BIN_CONTAINER;

// Profile & Debug
extern int DBG_LEVEL;
extern int OUTPUT_STAT;
extern int RECORD_PEAKMEM;

#endif
