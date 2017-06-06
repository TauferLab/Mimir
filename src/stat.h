/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef STAT_H
#define STAT_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <mpi.h>

#include <map>
#include <vector>
#include <string>

#include "memory.h"

enum OpType { OPSUM, OPMAX };

typedef struct _profiler_info {
    double prev_wtime;
} Profiler_info;

typedef struct _tracker_info {
    double prev_wtime;
} Tracker_info;

extern double init_wtime;

extern Profiler_info profiler_info;
extern double *profiler_timer;
extern uint64_t *profiler_counter;

extern Tracker_info tracker_info;
extern std::vector<std::pair<std::string, double> > *tracker_event;

extern const char *timer_str[];
extern const char *counter_str[];

extern char timestr[];

#define MR_GET_WTIME() MPI_Wtime()

// Timers
#define TIMER_TOTAL                0    // Total time
#define TIMER_PFS_INPUT            1    // PFS input time
#define TIMER_PFS_OUTPUT           2    // PFS output time
#define TIMER_COMM_A2A             3    // MPI_Alltoall
#define TIMER_COMM_A2AV            4    // MPI_Alltoallv
#define TIMER_COMM_RDC             5    // MPI_Allreduce
#define TIMER_COMM_BLOCK           6    // blocking time
#define TIMER_COMM_ALLGATHER       7    // MPI_Allgather
#define TIMER_MEM_ALLOCATE         8    // memory allocation
#define TIMER_SERIALIZER           9    // serializer
#define TIMER_NUM                 10


// Counters
#define COUNTER_COMM_BUFS           0   // comm buffer count
#define COUNTER_SHUFFLE_TIMES       1   // shuffle times
#define COUNTER_SEND_BYTES          2   // send bytes
#define COUNTER_RECV_BYTES          3   // recv bytes
#define COUNTER_FILE_COUNT          4   // file count
#define COUNTER_FILE_SIZE           5   // file size
#define COUNTER_MAX_FILE            6   // max size
#define COUNTER_SEND_TAIL           7   // send tail
#define COUNTER_RECV_TAIL           8   // recv tail
#define COUNTER_MAX_PAGES           9   // max pages
#define COUNTER_REDUCE_BUCKET      10   // max reduce bucket
#define COUNTER_COMBINE_BUCKET     11   // max combine bucket
#define COUNTER_PEAKMEM_USE        12   // peak memory usage
#define COUNTER_UNIQUE_KEY         13   // unique words
#define COUNTER_NUM                14

/// Events
#define EVENT_COMPUTE_APP          "event_compute_app"          // application computation

//  Computation
#define EVENT_COMPUTE_MAP          "event_compute_map"          // map computation
#define EVENT_COMPUTE_CVT          "event_compute_convert"      // convert computation
#define EVENT_COMPUTE_RDC          "event_compute_reduce"       // reduce computation

// Synchnization
#define EVENT_SYN_COMM             "event_syn_comm"             // make progress

// Communication
#define EVENT_COMM_ALLTOALL        "event_comm_alltoall"        // MPI_Alltoall
#define EVENT_COMM_ALLTOALLV       "event_comm_alltoallv"       // MPI_Alltoallv
#define EVENT_COMM_ALLREDUCE       "event_comm_allreduce"       // MPI_Allreduce
#define EVENT_COMM_IALLTOALL       "event_comm_ialltoall"       // MPI_Ialltoall
#define EVENT_COMM_IALLTOALLV      "event_comm_ialltoallv"      // MPI_Ialltoallv
#define EVENT_COMM_IREDUCE         "event_comm_ireduce"         // MPI_Ireduce
#define EVENT_COMM_SEND            "event_comm_send"            // MPI_Send
#define EVENT_COMM_RECV            "event_comm_recv"            // MPI_Recv
#define EVENT_COMM_ISEND           "event_comm_isend"           // MPI_Isend
#define EVENT_COMM_IRECV           "event_comm_irecv"           // MPI_Irecv
#define EVENT_COMM_ALLGATHER       "event_comm_allgather"       // MPI_Allgather

// Disk IO
#define EVENT_DISK_FOPEN           "event_disk_open"            // posix open
#define EVENT_DISK_FREADAT         "event_disk_readat"          // posix seek+read
#define EVENT_DISK_FWRITE          "event_disk_write"           // posx write
#define EVENT_DISK_FCLOSE          "event_disk_close"           // posix close
#define EVENT_DISK_MPIOPEN         "event_disk_mpiopen"         // MPI_File_open
#define EVENT_DISK_MPIREADATALL    "event_disk_mpireadatall"    // MPI_File_read_at_all
#define EVENT_DISK_MPIWRITEATALL   "event_disk_mpiwriteatall"   // MPI_File_write_at_all
#define EVENT_DISK_MPICLOSE        "event_disk_mpiclose"        // MPI_File_close

#define INIT_STAT()                                                            \
{                                                                              \
    PROFILER_START;                                                            \
    TRACKER_START;                                                             \
    init_wtime=MR_GET_WTIME();                                                 \
}

#define UNINIT_STAT                                                            \
{                                                                              \
    PROFILER_END;                                                              \
    TRACKER_END;                                                               \
}

#define GET_CUR_TIME                                                           \
{                                                                              \
    if (mimir_world_rank == 0) {                                               \
        time_t t = time(NULL);                                                 \
        struct tm tm = *localtime(&t);                                         \
        sprintf(timestr, "%d-%d-%d-%d-%d-%d",                                  \
                tm.tm_year + 1900,                                             \
                tm.tm_mon + 1,                                                 \
                tm.tm_mday,                                                    \
                tm.tm_hour,                                                    \
                tm.tm_min,                                                     \
                tm.tm_sec);                                                    \
    }                                                                          \
}

#ifndef ENABLE_PROFILER

#define PROFILER_START
#define PROFILER_END
#define PROFILER_RECORD_TIME_START
#define PROFILER_RECORD_TIME_END(timer_type)
#define PROFILER_RECORD_COUNT(counter_type, count, op)
#define PROFILER_PRINT(filename)

#else

#define PROFILER_START                                                         \
{                                                                              \
    profiler_timer = new double[TIMER_NUM];                                    \
    for (int i = 0; i < TIMER_NUM; i++) profiler_timer[i] = 0.0;               \
    profiler_counter = new uint64_t[COUNTER_NUM];                              \
    for (int i = 0; i < COUNTER_NUM; i++) profiler_counter[i] = 0.0;           \
}

#define PROFILER_END                                                           \
{                                                                              \
    delete [] profiler_timer;                                                  \
    delete [] profiler_counter;                                                \
}

#define PROFILER_RECORD_TIME_START                                             \
    profiler_info.prev_wtime = MR_GET_WTIME();

#define PROFILER_RECORD_TIME_END(timer_type)                                   \
    profiler_timer[timer_type] +=                                              \
        (MR_GET_WTIME() - profiler_info.prev_wtime);

#define PROFILER_RECORD_COUNT(counter_type, count, op)                         \
{                                                                              \
    if (op == OPSUM) {                                                         \
        profiler_counter[counter_type] += count;                               \
    } else if (op==OPMAX) {                                                    \
        if (profiler_counter[counter_type] < count) {                          \
            profiler_counter[counter_type] = count;                            \
        }                                                                      \
    }                                                                          \
}

#define PROFILER_PRINT(filename)                                               \
{                                                                              \
    profiler_timer[TIMER_TOTAL] = MR_GET_WTIME() - init_wtime;                 \
    profiler_counter[COUNTER_PEAKMEM_USE] = peakmem;                           \
    char fullname[1024];                                                       \
    FILE *fp = NULL;                                                           \
    if (mimir_world_rank == 0) {                                               \
        sprintf(fullname, "%s_%s_profile.txt", filename, timestr);             \
        printf("filename=%s\n", fullname);                                     \
        fp = fopen(fullname, "w+");                                            \
        if (!fp) LOG_ERROR("Create file %s error!\n", fullname);               \
        fprintf(fp, "testtime,rank,size");                                     \
        for(int i=0; i<TIMER_NUM; i++) fprintf(fp, ",%s", timer_str[i]);       \
        for(int i=0; i<COUNTER_NUM; i++) fprintf(fp, ",%s", counter_str[i]);   \
        fprintf(fp, "\n%s,0,%d", timestr, mimir_world_size);                   \
        for(int i=0; i<TIMER_NUM; i++) fprintf(fp, ",%g", profiler_timer[i]);  \
        for(int i=0; i<COUNTER_NUM; i++)                                       \
            fprintf(fp, ",%ld", profiler_counter[i]);                          \
    }                                                                          \
    if (mimir_world_rank == 0) {                                               \
        MPI_Status st;                                                         \
        for (int i = 1; i < mimir_world_size; i++) {                           \
            fprintf(fp, "\n%s,%d,%d", timestr, i, mimir_world_size);           \
            MPI_Recv(profiler_timer, TIMER_NUM, MPI_DOUBLE,                    \
                     i, STAT_TIMER_TAG, mimir_world_comm, &st);                \
            for (int i = 0; i < TIMER_NUM; i++)                                \
                fprintf(fp, ",%g", profiler_timer[i]);                         \
            MPI_Recv(profiler_counter, COUNTER_NUM, MPI_UINT64_T,              \
                     i, STAT_COUNTER_TAG, mimir_world_comm, &st);              \
            for (int i = 0; i < COUNTER_NUM; i++)                              \
                fprintf(fp, ",%ld", profiler_counter[i]);                      \
        }                                                                      \
    } else {                                                                   \
        MPI_Send(profiler_timer, TIMER_NUM, MPI_DOUBLE,                        \
                 0, STAT_TIMER_TAG, mimir_world_comm);                         \
        MPI_Send(profiler_counter, COUNTER_NUM, MPI_UINT64_T,                  \
                 0, STAT_COUNTER_TAG, mimir_world_comm);                       \
    }                                                                          \
    if (mimir_world_rank == 0) fclose(fp);                                     \
    MPI_Barrier(mimir_world_comm);                                             \
}

#endif

#ifndef ENABLE_TRACKER

#define TRACKER_START
#define TRACKER_END
#define TRACKER_RECORD_EVENT(event_type)
#define TRACKER_PRINT(filename)

#else

#define TRACKER_START                                                          \
{                                                                              \
    if (mimir_world_rank == 0) {                                               \
        tracker_event =                                                        \
          new std::vector<std::pair<std::string,double> >[mimir_world_size];   \
    }else{                                                                     \
        tracker_event=new std::vector<std::pair<std::string,double> >[1];      \
    }                                                                          \
    tracker_info.prev_wtime=MR_GET_WTIME();                                    \
}

#define TRACKER_END                                                            \
{                                                                              \
    delete [] tracker_event;                                                   \
}

#define TRACKER_RECORD_EVENT(event_type)                                       \
{                                                                              \
    double t_start = MR_GET_WTIME();                                           \
    double t_prev = tracker_info.prev_wtime;                                   \
    tracker_event[0].push_back(std::make_pair(event_type, t_start-t_prev));    \
    double t_end = MR_GET_WTIME();                                             \
    tracker_info.prev_wtime = t_end;                                           \
}

#define TRACKER_PRINT(filename)                                                \
{                                                                              \
    int total_bytes=0, max_bytes=0;                                            \
    std::vector<std::pair<std::string,double> >::iterator iter;                \
    for(iter=tracker_event[0].begin(); iter!=tracker_event[0].end(); iter++){  \
        max_bytes+=(int)strlen(iter->first.c_str())+1;                         \
        max_bytes+=(int)sizeof(iter->second);                                  \
    }                                                                          \
    MPI_Reduce(&max_bytes, &total_bytes, 1, MPI_INT,                           \
               MPI_MAX, 0, mimir_world_comm);                                  \
    if (max_bytes>total_bytes) total_bytes=max_bytes;                          \
    char *tmp=(char*)mem_aligned_malloc(MEMPAGE_SIZE, total_bytes);            \
    if (mimir_world_rank==0){                                                  \
        MPI_Status st;                                                         \
        for(int i=0; i<mimir_world_size-1; i++){                               \
            MPI_Recv(tmp, total_bytes, MPI_BYTE,                               \
                     MPI_ANY_SOURCE, STAT_EVENT_TAG, mimir_world_comm, &st);   \
            int recv_rank=st.MPI_SOURCE;                                       \
            int recv_count=0;                                                  \
            MPI_Get_count(&st, MPI_BYTE, &recv_count);                         \
            int off=0;                                                         \
            while (off<recv_count){                                            \
                char *type=tmp+off;                                            \
                off+=(int)strlen(type)+1;                                      \
                double value=*(double*)(tmp+off);                              \
                tracker_event[recv_rank].push_back(std::make_pair(type, value));\
                off+=(int)sizeof(double);                                      \
            }                                                                  \
        }                                                                      \
    }else{                                                                     \
        int off=0;                                                             \
        std::vector<std::pair<std::string,double> >::iterator iter;            \
        for(iter=tracker_event[0].begin(); iter!=tracker_event[0].end(); iter++){\
            memcpy(tmp+off, iter->first.c_str(), strlen(iter->first.c_str())+1);\
            off+=(int)strlen(iter->first.c_str())+1;                           \
            memcpy(tmp+off, &(iter->second), sizeof(double));                  \
            off+=(int)sizeof(iter->second);                                    \
        }                                                                      \
        MPI_Send(tmp, off, MPI_BYTE, 0, STAT_EVENT_TAG, mimir_world_comm);     \
    }                                                                          \
    mem_aligned_free(tmp);                                                     \
    char fullname[1024];                                                       \
    FILE *fp=NULL;                                                             \
    if (mimir_world_rank==0){                                                  \
        sprintf(fullname, "%s_%s_trace.txt", filename, timestr);               \
        printf("filename=%s\n", fullname);                                     \
        fp = fopen(fullname, "w+");                                            \
        if (!fp) LOG_ERROR("Create file %s error!\n", fullname);               \
        for(int i=0; i<mimir_world_size; i++){                                 \
            fprintf(fp, "rank:%d,size:%d",i,mimir_world_size);                 \
            std::vector<std::pair<std::string,double> >::iterator iter;        \
            for(iter=tracker_event[i].begin();                                 \
                iter!=tracker_event[i].end(); iter++){                         \
                fprintf(fp, ",%s:%g", iter->first.c_str(), iter->second);      \
            }                                                                  \
            fprintf(fp, "\n");                                                 \
        }                                                                      \
        fclose(fp);                                                            \
    }                                                                          \
    MPI_Barrier(mimir_world_comm);                                             \
}

#endif

#endif
