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

int mimir_ctx_count = 0;

char dump_buffer[MIMIR_MAX_LOG_LEN] = "";

void handle_error(int errcode, const char *str)
{
    char name[MPI_MAX_PROCESSOR_NAME];
    char msg[MPI_MAX_ERROR_STRING];
    int resultlen;

    MPI_Get_processor_name(name, &resultlen);
    MPI_Error_string(errcode, msg, &resultlen);
    fprintf(stderr, "%d[%d](host=%s) %s: %s\n",
            mimir_world_rank, mimir_world_size, name, str, msg);
    MPI_Abort(MPI_COMM_WORLD, 1);
}
