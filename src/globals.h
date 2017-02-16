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

#define COUNT_TAG     0xaa
#define  DATA_TAG     0xbb

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
