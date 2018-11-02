/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

#include "common.h"
#include "mimir.h"

using namespace MIMIR_NS;

const char *commsize = NULL;
const char *pagesize = NULL;
const char *ibufsize = NULL;
int bucketsize;

void check_envars(int rank, int size)
{
    if (getenv("MIMIR_BUCKET_SIZE") == NULL || getenv("MIMIR_COMM_SIZE") == NULL
        || getenv("MIMIR_PAGE_SIZE") == NULL
        || getenv("MIMIR_IBUF_SIZE") == NULL) {
        if (rank == 0)
            printf(
                "Please set MIMIR_BUCKET_SIZE, MIMIR_COMM_SIZE, "
                "MIMIR_PAGE_SIZE "
                "and MIMIR_IBUF_SIZE environments!\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

void output(int rank, int size, const char *prefix, const char *outdir)
{
    MPI_Barrier(MPI_COMM_WORLD);

    commsize = getenv("MIMIR_COMM_SIZE");
    pagesize = getenv("MIMIR_PAGE_SIZE");
    ibufsize = getenv("MIMIR_IBUF_SIZE");
    bucketsize = atoi(getenv("MIMIR_BUCKET_SIZE"));

    char filename[1024];
    sprintf(filename, "%s/%s-%d_c%s-p%s-i%s-h%d", outdir, prefix, size,
            commsize, pagesize, ibufsize, bucketsize);

    Mimir_stat(filename);

    MPI_Barrier(MPI_COMM_WORLD);
}
