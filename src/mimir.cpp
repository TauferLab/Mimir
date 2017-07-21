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
#include "ac_config.h"

#if HAVE_LIBMEMKIND 
#include "hbwmalloc.h"
#endif

void get_default_values();

void mimir_init(){
    //MPI_Comm_dup(comm, &mimir_world_comm);
    mimir_world_comm = MPI_COMM_WORLD;
    MPI_Comm_rank(MPI_COMM_WORLD, &mimir_world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mimir_world_size);
    char hostname[1024];
    gethostname(hostname, 1024);
    printf("%d[%d] Mimir Initialize... (pid=%d,host=%s)\n",
           mimir_world_rank, mimir_world_size, getpid(), hostname);
    INIT_STAT();
    get_default_values();
#if HAVE_LIBMEMKIND
    printf("%d[%d] set hbw policy\n",
           mimir_world_rank, mimir_world_size);
    hbw_set_policy(HBW_POLICY_BIND);   
#endif
}

void mimir_finalize()
{
    if (OUTPUT_STAT) {
        //const char *env = getenv("MIMIR_STAT_FILE");
        if (STAT_FILE) {
            mimir_stat(STAT_FILE);
        }
        else mimir_stat("Mimir");
    }
    UNINIT_STAT;
    //MPI_Comm_free(&mimir_world_comm);
    //printf("%d[%d] Mimir Finalize.\n",
    //       mimir_world_rank, mimir_world_size);
}

void mimir_stat(const char* filename)
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
    ///// Buffes
    char *env = NULL;
    // hash bucket size
    env = getenv("MIMIR_BUCKET_SIZE");
    if (env) {
        BUCKET_COUNT = (int)convert_to_int64(env);
        if (BUCKET_COUNT <= 0)
            LOG_ERROR
                ("Error: set bucket size error, please set MIMIR_BUCKET_SIZE (%s) correctly!\n",
                 env);
    }
    // communication buffer size
    env = getenv("MIMIR_COMM_SIZE");
    if (env) {
        COMM_BUF_SIZE = convert_to_int64(env);
        if (COMM_BUF_SIZE <= 0)
            LOG_ERROR
                ("Error: set communication buffer size error, please set MIMIR_COMM_SIZE (%s) correctly!\n",
                 env);
    }
    // data page buffer size
    env = getenv("MIMIR_PAGE_SIZE");
    if (env) {
        DATA_PAGE_SIZE = convert_to_int64(env);
        if (DATA_PAGE_SIZE <= 0)
            LOG_ERROR("Error: set page size error, please set DATA_PAGE_SIZE (%s) correctly!\n",
                      env);
    }
    // disk I/O buffer size
    env = getenv("MIMIR_DISK_SIZE");
    if (env) {
        INPUT_BUF_SIZE = convert_to_int64(env);
        if (INPUT_BUF_SIZE <= 0)
            LOG_ERROR
                ("Error: set input buffer size error, please set INPUT_BUF_SIZE (%s) correctly!\n",
                 env);
    }
    // max record size
    env = getenv("MIMIR_MAX_RECORD_SIZE");
    if (env) {
        MAX_RECORD_SIZE = (int)convert_to_int64(env);
        if (MAX_RECORD_SIZE <= 0) {
            LOG_ERROR
                ("Error: set max record size error, please set MAX_RECORD_SIZE (%s) correctly!\n",
                 env);
        }
    }

    /// Settings
    // shuffle type
    env = getenv("MIMIR_SHUFFLE_TYPE");
    if (env) {
        if (strcmp(env, "a2av") == 0) {
            SHUFFLE_TYPE = 0;
        }else if (strcmp(env, "ia2av") == 0) {
            SHUFFLE_TYPE = 1;
        }
    }
    // unit size to divide communication buffer
    env = NULL;
    env = getenv("MIMIR_COMM_UNIT_SIZE");
    if (env) {
        COMM_UNIT_SIZE = (int) convert_to_int64(env);
        if (COMM_UNIT_SIZE <= 0 || COMM_UNIT_SIZE > 1024 * 1024 * 1024)
            LOG_ERROR("Error: COMM_UNIT_SIZE (%d) should be > 0 and <1G!\n", COMM_UNIT_SIZE);
    }
    // if shuffle type is ia2av, min count of communication buffer
    env = getenv("MIMIR_MIN_COMM_BUF");
    if (env) {
        MIN_SBUF_COUNT = atoi(env);
    }
    // if shuffle type is ia2av, max count of commmunication buffer
    env = getenv("MIMIR_MAX_COMM_BUF");
    if (env) {
        MAX_SBUF_COUNT = atoi(env);
    }
    // read type
    env = getenv("MIMIR_READE_TYPE");
    if (env) {
        if (strcmp(env, "posix") == 0) {
            READ_TYPE = 0;
        }else if (strcmp(env, "direct") == 0) {
            READ_TYPE = 1;
        }else if (strcmp(env, "mpiio") == 0) {
            READ_TYPE = 2;
        }
    }
    // write type
    env = getenv("MIMIR_WRITE_TYPE");
    if (env) {
        if (strcmp(env, "posix") == 0) {
            WRITE_TYPE = 0;
        } else if (strcmp(env, "direct") == 0) {
            WRITE_TYPE = 1;
        }else if (strcmp(env, "mpiio") == 0) {
            WRITE_TYPE = 2;
        }
    }

    /// Features
    // work steal or not
    env = getenv("MIMIR_WORK_STEAL");
    if (env) {
        WORK_STEAL = atoi(env);
    }
    // aggressive make progress
    env = getenv("MIMIR_MAKE_PROGRESS");
    if (env) {
        MAKE_PROGRESS = atoi(env);
    }
    // balance load
    env = getenv("MIMIR_BALANCE_LOAD");
    if (env) {
        int flag = atoi(env);
        if (flag == 0) {
            BALANCE_LOAD = 0;
        } else {
            BALANCE_LOAD = 1;
        }
    }
    // number of bins per process
    env = getenv("MIMIR_BIN_COUNT");
    if (env) {
        BIN_COUNT = atoi(env);
    }
    // balance factor
    env = getenv("MIMIR_BALANCE_FACTOR");
    if (env) {
        BALANCE_FACTOR = atof(env);
    }
    // balance memory among nodes
    env = getenv("MIMIR_BALANCE_NODE");
    if (env) {
        BALANCE_NODE = atoi(env);
    }
    // balance memory among nodes
    env = getenv("MIMIR_USE_MCDRAM");
    if (env) {
        USE_MCDRAM = atoi(env);
    }

    /// Profile & Debug
    // output stat file
    env = getenv("MIMIR_OUTPUT_STAT");
    if (env) {
        int flag = atoi(env);
        if (flag == 0) {
            OUTPUT_STAT = 0;
        } else {
            OUTPUT_STAT = 1;
        }
    }
    // stat file name
    env = getenv("MIMIR_STAT_FILE");
    if (env) {
        STAT_FILE = env;
    }
    // record peak memory usage
    env = getenv("MIMIR_RECORD_PEAKMEM");
    if (env) {
        int flag = atoi(env);
        if (flag == 0) {
            RECORD_PEAKMEM = 0;
        } else {
            RECORD_PEAKMEM = 1;
        }
    }
    // Ccnfigure debug level
    env = getenv("MIMIR_DBG_ALL");
    if (env) {
        int flag = atoi(env);
        if (flag != 0) {
            DBG_LEVEL |= (DBG_GEN | DBG_DATA | DBG_COMM 
                          | DBG_IO | DBG_MEM | DBG_CHUNK | DBG_REPAR);
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
\thash bucket size: %d\n\
\tmax record size: %d\n\
\tshuffle type: %d (unit size: %d)(0 - MPI_Alltoallv; 1 - MPI_Ialltoallv [%d,%d])\n\
\treader type: %d (0 - POSIX; 1 - DIRECT; 2 - MPIIO)\n\
\twriter type: %d (0 - POSIX; 1 - DIRECT; 2 - MPIIO)\n\
\twork stealing: %d (make progress=%d)\n\
\tload balance: balance=%d, balance factor=%.2lf, bin count=%d, balance node=%d\n\
\tMCDRAM: use_mcdram=%d\n\
\tstat & debug: output stat=%d, stat file=%s, output peak mem=%d, debug level=%x\n\
***********************************************************************\n",
        COMM_BUF_SIZE, DATA_PAGE_SIZE, INPUT_BUF_SIZE, BUCKET_COUNT, MAX_RECORD_SIZE,
        SHUFFLE_TYPE, COMM_UNIT_SIZE, MIN_SBUF_COUNT, MAX_SBUF_COUNT, READ_TYPE, WRITE_TYPE, 
        WORK_STEAL, MAKE_PROGRESS,
        BALANCE_LOAD, BALANCE_FACTOR, BIN_COUNT, BALANCE_NODE,
        USE_MCDRAM,
	OUTPUT_STAT, STAT_FILE, RECORD_PEAKMEM, DBG_LEVEL);
        fflush(stdout);
    }
}
