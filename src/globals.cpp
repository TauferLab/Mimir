#include "log.h"
#include <mpi.h>

MPI_Comm  mimir_world_comm;
int mimir_world_rank = -1;
int mimir_world_size = 0;

char dump_buffer[MIMIR_MAX_LOG_LEN] = "";

