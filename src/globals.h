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

#define _LARGEFILE_SOURCE       1
#define _LARGEFILE64_SOURCE     1
#define _FILE_OFFSET_BITS      64

#define STAT_TIMER_TAG       0x11
#define STAT_COUNTER_TAG     0x22
#define STAT_EVENT_TAG       0x33

#define SHUFFLER_KV_TAG      0xaa
#define CHUNK_TAIL_TAG       0xbb

#define NIN(A,B) ((A) < (B)) ? (A) : (B)
#define MAX(A,B) ((A) > (B)) ? (A) : (B)

#define ROUNDUP(val,unit) (((val)+(unit)-1)/(unit))
#define ROUNDDOWN(val,unit) (((val))/(unit))

#define MEMPAGE_SIZE               4096
#define DISKPAGE_SIZE              4096

#define MAXLINE                    2048
#define MAX_COMM_SIZE        0x40000000

enum KVType {
    KVGeneral = -3,
    KVVARINT = -2,
    KVString = -1,
};

extern int mimir_world_rank;
extern int mimir_world_size;
extern MPI_Comm  mimir_world_comm;

#endif
