/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_GLOBALS_H
#define MIMIR_GLOBALS_H

#include <mpi.h>

#define STAT_TIMER_TAG       0x11
#define STAT_COUNTER_TAG     0x22
#define STAT_EVENT_TAG       0x33

#define READER_CMD_TAG       0xaa
#define READER_DATA_TAG      0xbb
#define SHUFFLER_KV_TAG      0xcc

#define NIN(A,B) ((A) < (B)) ? (A) : (B)
#define MAX(A,B) ((A) > (B)) ? (A) : (B)

#define ROUNDUP(val,unit) (((val)+(unit)-1)/(unit))

#define MEMPAGE_SIZE               4096

#define MAXLINE                    2048
#define MAX_COMM_SIZE        0x40000000

enum KVType {
    KVGeneral = -2,
    KVString
};

extern int mimir_world_rank;
extern int mimir_world_size;
extern MPI_Comm  mimir_world_comm;

#endif
