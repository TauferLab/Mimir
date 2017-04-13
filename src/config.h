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

extern int KV_EXCH_COMM;
extern int MAX_PAGE_COUNT;
extern int BUCKET_COUNT;
extern int SET_COUNT;
extern int MAX_RECORD_SIZE;

extern int64_t COMM_BUF_SIZE;
extern int64_t DATA_PAGE_SIZE;
extern int64_t INPUT_BUF_SIZE;

//extern int64_t FILE_SPLIT_UNIT;
extern int DISK_IO_TYPE;

extern int WORK_STEAL;

extern int DBG_LEVEL;
extern int COMM_UNIT_SIZE;
extern int RECORD_PEAKMEM;

extern int KTYPE, VTYPE;

extern int READER_TYPE;
extern int WRITER_TYPE;
extern int SHUFFLE_TYPE;

extern int OUTPUT_STAT;

#endif
