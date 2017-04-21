/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
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
    if (OUTPUT_STAT) {
        const char *env = getenv("MIMIR_STAT_FILE");
        if (env) {
            Mimir_stat(env);
        }
        else Mimir_stat("Mimir");
    }

    UNINIT_STAT;
}

void Mimir_stat(const char* filename)
{
    GET_CUR_TIME;
    TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);
    if (RECORD_PEAKMEM == 1) {
        int64_t vmsize = get_mem_usage();
        if (vmsize > peakmem) peakmem = vmsize;
    }
    PROFILER_PRINT(filename);
    TRACKER_PRINT(filename);
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
    KTYPE = VTYPE = KVGeneral;

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
    env = getenv("MIMIR_WORK_STEAL");
    if (env) {
        WORK_STEAL = atoi(env);
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
            DBG_LEVEL |= (DBG_GEN | DBG_DATA | DBG_COMM | DBG_IO | DBG_MEM | DBG_CHUNK);
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

    env = getenv("MIMIR_READER_TYPE");
    if (env) {
        if (strcmp(env, "posix") == 0) {
            READER_TYPE = 0;
        }else if (strcmp(env, "direct") == 0) {
            READER_TYPE = 1;
        }else if (strcmp(env, "mpiio") == 0) {
            READER_TYPE = 2;
        }
    }

    env = getenv("MIMIR_WRITER_TYPE");
    if (env) {
        if (strcmp(env, "posix") == 0) {
            WRITER_TYPE = 0;
        } else if (strcmp(env, "direct") == 0) {
            WRITER_TYPE = 1;
        }else if (strcmp(env, "mpiio") == 0) {
            WRITER_TYPE = 2;
        }
    }

    env = getenv("MIMIR_SHUFFLE_TYPE");
    if (env) {
        if (strcmp(env, "a2av") == 0) {
            SHUFFLE_TYPE = 0;
        }else if (strcmp(env, "ia2av") == 0) {
            SHUFFLE_TYPE = 1;
        }else if (strcmp(env, "isend") == 0) {
            SHUFFLE_TYPE = 2;
        }
    }

    env = getenv("MIMIR_MIN_COMM_BUF");
    if (env) {
        MIN_SBUF_COUNT = atoi(env);
    }

    env = getenv("MIMIR_MAX_COMM_BUF");
    if (env) {
        MAX_SBUF_COUNT = atoi(env);
    }

    env = getenv("MIMIR_OUTPUT_STAT");
    if (env) {
        int flag = atoi(env);
        if (flag == 0) {
            OUTPUT_STAT = 0;
        } else {
            OUTPUT_STAT = 1;
        }
    }

    //env = getenv("MIMIR_FILE_ALIGN");
    //if (env) {
    //    FILE_SPLIT_UNIT = convert_to_int64(env);
    //    if (FILE_SPLIT_UNIT <= 0)
    //        LOG_ERROR("Error: set file alignment error\n");
    //}

    if (mimir_world_rank == 0) {
        printf(\
"**********************************************************************\n\
******** Welcome to use Mimir (MapReduce framework over MPI) *********\n\
**********************************************************************\n\
Refer IPDPS17 paper \"Mimir: Memory-Efficient and Scalable MapReduce\n\
for Large Supercomputing Systems\" for the design idea.\n\
Library configuration:\n\
\tcomm buffer size: %ld\n\
\tpage buffer size: %ld\n\
\tdisk buffer size: %ld\n\
\tbucket size (2^x): %d\n\
\twork stealing: %d\n\
\tshuffle type: %d (0 - MPI_Alltoallv; 1 - MPI_Ialltoallv; 2 - MPI_Isend)\n\
\treader type: %d (0 - POSIX; 1 - DIRECT; 2 - MPIIO)\n\
\twriter type: %d (0 - POSIX; 1 - DIRECT; 2 - MPIIO)\n\
\tcomm buffer: min=%d, max=%d\n\
\tdebug level: %x\n\
***********************************************************************\n",
        COMM_BUF_SIZE, DATA_PAGE_SIZE, INPUT_BUF_SIZE, BUCKET_COUNT, WORK_STEAL, 
        SHUFFLE_TYPE, READER_TYPE, WRITER_TYPE, 
        MIN_SBUF_COUNT, MAX_SBUF_COUNT, 
        DBG_LEVEL);
        fflush(stdout);
    }

    //PROFILER_RECORD_COUNT(COUNTER_BUCKET_SIZE, (uint64_t) BUCKET_COUNT, OPMAX);
    //PROFILER_RECORD_COUNT(COUNTER_INBUF_SIZE, (uint64_t) INPUT_BUF_SIZE, OPMAX);
    //PROFILER_RECORD_COUNT(COUNTER_COMM_SIZE, (uint64_t) COMM_BUF_SIZE, OPMAX);
    //PROFILER_RECORD_COUNT(COUNTER_PAGE_SIZE, (uint64_t) DATA_PAGE_SIZE, OPMAX);
}


