#include "log.h"
#include "stat.h"
#include "mimir.h"
#include "globals.h"

void get_default_values();

void Mimir_Init(int *argc, char **argv[], MPI_Comm comm){
    mimir_world_comm = comm;
    MPI_Comm_rank(mimir_world_comm, &mimir_world_rank);
    MPI_Comm_size(mimir_world_comm, &mimir_world_size);
    INIT_STAT();
    get_default_values();
}

void Mimir_Finalize()
{
    UNINIT_STAT;
}

int64_t convert_to_int64(const char *_str)
{
    std::string str = _str;
    int64_t num = 0;
    if (str[str.size() - 1] == 'b' || str[str.size() - 1] == 'B') {
        str = str.substr(0, str.size() - 1);
        num = atoi(str.c_str());
        num *= 1;
    }
    else if (str[str.size() - 1] == 'k' || str[str.size() - 1] == 'K' ||
             (str[str.size() - 1] == 'b' && str[str.size() - 2] == 'k') ||
             (str[str.size() - 1] == 'B' && str[str.size() - 2] == 'K')) {
        if (str[str.size() - 1] == 'b' || str[str.size() - 1] == 'B') {
            str = str.substr(0, str.size() - 2);
        }
        else {
            str = str.substr(0, str.size() - 1);
        }
        num = atoi(str.c_str());
        num *= 1024;
    }
    else if (str[str.size() - 1] == 'm' || str[str.size() - 1] == 'M' ||
             (str[str.size() - 1] == 'b' && str[str.size() - 2] == 'm') ||
             (str[str.size() - 1] == 'B' && str[str.size() - 2] == 'M')) {
        if (str[str.size() - 1] == 'b' || str[str.size() - 1] == 'B') {
            str = str.substr(0, str.size() - 2);
        }
        else {
            str = str.substr(0, str.size() - 1);
        }
        num = atoi(str.c_str());
        num *= 1024 * 1024;
    }
    else if (str[str.size() - 1] == 'g' || str[str.size() - 1] == 'G' ||
             (str[str.size() - 1] == 'b' && str[str.size() - 2] == 'g') ||
             (str[str.size() - 1] == 'B' && str[str.size() - 2] == 'G')) {
        if (str[str.size() - 1] == 'b' || str[str.size() - 1] == 'B') {
            str = str.substr(0, str.size() - 2);
        }
        else {
            str = str.substr(0, str.size() - 1);
        }
        num = atoi(str.c_str());
        num *= 1024 * 1024 * 1024;
    }
    else {
        LOG_ERROR("Error: set buffer size %s error! \
                  The buffer size should end with b,B,k,K,kb,KB,m,M,mb,MB,g,G,gb,GB", _str);
    }
    if (num == 0) {
        LOG_ERROR("Error: buffer size %s should not be zero!", _str);
    }

    return num;
}


void get_default_values()
{
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
        COMM_BUF_SIZE = convert_to_int64(env);
        if (COMM_BUF_SIZE <= 0)
            LOG_ERROR
                ("Error: set communication buffer size error, please set MIMIR_COMM_SIZE (%s) correctly!\n",
                 env);
    }
    env = getenv("MIMIR_PAGE_SIZE");
    if (env) {
        DATA_PAGE_SIZE = convert_to_int64(env);
        if (DATA_PAGE_SIZE <= 0)
            LOG_ERROR("Error: set page size error, please set DATA_PAGE_SIZE (%s) correctly!\n",
                      env);
    }
    env = getenv("MIMIR_IBUF_SIZE");
    if (env) {
        INPUT_BUF_SIZE = convert_to_int64(env);
        if (INPUT_BUF_SIZE <= 0)
            LOG_ERROR
                ("Error: set input buffer size error, please set INPUT_BUF_SIZE (%s) correctly!\n",
                 env);
    }

    /// Configure unit size for communication buffer
    env = NULL;
    env = getenv("MIMIR_COMM_UNIT_SIZE");
    if (env) {
        COMM_UNIT_SIZE = (int) convert_to_int64(env);
        if (COMM_UNIT_SIZE <= 0 || COMM_UNIT_SIZE > 1024 * 1024 * 1024)
            LOG_ERROR("Error: COMM_UNIT_SIZE (%d) should be > 0 and <1G!\n", COMM_UNIT_SIZE);
    }

    // Configure debug level
    env = getenv("MIMIR_DBG_ALL");
    if (env) {
        int flag = atoi(env);
        if (flag != 0) {
            DBG_LEVEL |= (DBG_GEN | DBG_DATA | DBG_COMM | DBG_IO | DBG_MEM);
        }
    }
    env = getenv("MIMIR_DBG_GEN");
    if (env) {
        int flag = atoi(env);
        if (flag != 0) {
            DBG_LEVEL |= (DBG_GEN);
        }
    }
    env = getenv("MIMIR_DBG_DATA");
    if (env) {
        int flag = atoi(env);
        if (flag != 0) {
            DBG_LEVEL |= (DBG_DATA);
        }
    }
    env = getenv("MIMIR_DBG_COMM");
    if (env) {
        int flag = atoi(env);
        if (flag != 0) {
            DBG_LEVEL |= (DBG_COMM);
        }
    }
    env = getenv("MIMIR_DBG_IO");
    if (env) {
        int flag = atoi(env);
        if (flag != 0) {
            DBG_LEVEL |= (DBG_IO);
        }
    }
    env = getenv("MIMIR_DBG_MEM");
    if (env) {
        int flag = atoi(env);
        if (flag != 0) {
            DBG_LEVEL |= (DBG_MEM);
        }
    }
    env = getenv("MIMIR_DBG_VERBOSE");
    if (env) {
        int flag = atoi(env);
        if (flag != 0) {
            DBG_LEVEL |= (DBG_VERBOSE);
        }
    }

    env = getenv("MIMIR_RECORD_PEAKMEM");
    if (env) {
        int flag = atoi(env);
        if (flag == 0) {
            RECORD_PEAKMEM = 0;
        }
    }

    if (mimir_world_rank == 0) {
        fprintf(stdout, "bucket size(2^x)=%d, comm_buf_size=%ld, \
                data_page_size=%ld, input_buf_size=%ld, DBG_LEVEL=%x\n", BUCKET_COUNT, COMM_BUF_SIZE, DATA_PAGE_SIZE, INPUT_BUF_SIZE, DBG_LEVEL);
    }
    fflush(stdout);

    PROFILER_RECORD_COUNT(COUNTER_BUCKET_SIZE, (uint64_t) BUCKET_COUNT, OPMAX);
    PROFILER_RECORD_COUNT(COUNTER_INBUF_SIZE, (uint64_t) INPUT_BUF_SIZE, OPMAX);
    PROFILER_RECORD_COUNT(COUNTER_COMM_SIZE, (uint64_t) COMM_BUF_SIZE, OPMAX);
    PROFILER_RECORD_COUNT(COUNTER_PAGE_SIZE, (uint64_t) DATA_PAGE_SIZE, OPMAX);
}


