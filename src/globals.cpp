/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#include "log.h"
#include <mpi.h>

MPI_Comm  mimir_world_comm;
int mimir_world_rank = -1;
int mimir_world_size = 0;

char dump_buffer[MIMIR_MAX_LOG_LEN] = "";

