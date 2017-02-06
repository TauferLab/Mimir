#include "mimir.h"
#include "globals.h"


void Mimir_Init(int *argc, char **argv[], MPI_Comm comm){
    mimir_world_comm = comm;
    MPI_Comm_rank(mimir_world_comm, &mimir_world_rank);
    MPI_Comm_size(mimir_world_comm, &mimir_world_size);
}

void Mimir_Finalize(){
}

#if 0
void MapReduce::_get_default_values()
{
    /// Initialize member of MapReduce
    phase = NonePhase;

    kv = NULL;
    c = NULL;

    myhash = NULL;
    mycombiner = NULL;

    //kvtype = GeneralKV;
    ksize = vsize = KVGeneral;

    /// Configure main parameters
    char *env = NULL;
    env = getenv("MIMIR_BUCKET_SIZE");
    if (env) {
        BUCKET_COUNT = atoi(env);
        if (BUCKET_COUNT <= 0)
            LOG_ERROR
                ("Error: set bucket size error, please set MIMIR_BUCKET_SIZE (%s) correctly!\n",
                 env);
    }
    env = getenv("MIMIR_COMM_SIZE");
    if (env) {
        COMM_BUF_SIZE = _convert_to_int64(env);
        if (COMM_BUF_SIZE <= 0)
            LOG_ERROR
                ("Error: set communication buffer size error, please set MIMIR_COMM_SIZE (%s) correctly!\n",
                 env);
    }
    env = getenv("MIMIR_PAGE_SIZE");
    if (env) {
        DATA_PAGE_SIZE = _convert_to_int64(env);
        if (DATA_PAGE_SIZE <= 0)
            LOG_ERROR("Error: set page size error, please set DATA_PAGE_SIZE (%s) correctly!\n",
                      env);
    }
    env = getenv("MIMIR_IBUF_SIZE");
    if (env) {
        INPUT_BUF_SIZE = _convert_to_int64(env);
        if (INPUT_BUF_SIZE <= 0)
            LOG_ERROR
                ("Error: set input buffer size error, please set INPUT_BUF_SIZE (%s) correctly!\n",
                 env);
    }

    /// Configure unit size for communication buffer
    env = NULL;
#endif
