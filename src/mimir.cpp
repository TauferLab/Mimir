#include "mimir.h"
#include "globals.h"


void Mimir_Init(int *argc, char **argv[], MPI_Comm comm){
    mimir_world_comm = comm;
    MPI_Comm_rank(mimir_world_comm, &mimir_world_rank);
    MPI_Comm_size(mimir_world_comm, &mimir_world_size);
}

void Mimir_Finalize(){
}


