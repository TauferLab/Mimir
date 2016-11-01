#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

#include "mapreduce.h"
#include "common.h"

using namespace MIMIR_NS;

void output()
{
    MapReduce::output_stat("test");
    MPI_Barrier(MPI_COMM_WORLD);
    if(rank==0){
    }
}
