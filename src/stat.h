/**
 * @file   mapreduce.h
 * @Author Tao Gao (taogao@udel.edu)
 * @date   September 1st, 2016
 * @brief  This file provides interfaces to application programs.
 *
 * This file includes two classes: MapReduce and MultiValueIter.
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

extern MPI_Comm stat_comm;
extern int stat_ref, stat_rank, stat_size;
extern double init_wtime;

extern Profiler_info profiler_info;
extern double *profiler_timer;
extern uint64_t *profiler_counter;

extern Tracker_info tracker_info;
extern std::vector<std::pair<std::string, double>> *tracker_event;

extern const char *timer_str[];
extern const char *counter_str[];

extern char timestr[];

#define MR_GET_WTIME() MPI_Wtime()

// Timers
#define TIMER_TOTAL                0    // Total time
#define TIMER_PFS_IO               1    // PFS time
#define TIMER_COMM_A2A             2    // MPI_Alltoall
#define TIMER_COMM_A2AV            3    // MPI_Alltoallv
#define TIMER_COMM_RDC             4    // MPI_Allreduce
#define TIMER_NUM                  5


// Counters
#define COUNTER_BUCKET_SIZE         0   // bucket size
#define COUNTER_INBUF_SIZE          1   // inbuf size
#define COUNTER_PAGE_SIZE           2   // page size
#define COUNTER_COMM_SIZE           3   // comm_size
#define COUNTER_SEND_BYTES          4   // send bytes
#define COUNTER_RECV_BYTES          5   // recv bytes
#define COUNTER_FILE_COUNT          6   // file count
#define COUNTER_FILE_SIZE           7   // file size
#define COUNTER_MAX_FILE            8   // max size
#define COUNTER_MAX_PAGES           9   // max pages
#define COUNTER_REDUCE_BUCKET      10   // max reduce bucket
#define COUNTER_COMBINE_BUCKET     11   // max combine bucket
#define COUNTER_PEAKMEM_USE        12   // peak memory usage
#define COUNTER_UNIQUE_KEY         13   // unique words
#define COUNTER_NUM                14

/// Events
//  Computation
#define EVENT_COMPUTE_MAP          "event_compute_map"  // map computation
#define EVENT_COMPUTE_RDC          "event_compute_reduce"       // reduce computation
#define EVENT_COMPUTE_SCAN         "event_compute_scan" // scan computation
#define EVENT_COMPUTE_OTHER        "event_compute_other"        // other computation

// Initialize
#define EVENT_INIT_GETFILES        "event_init_getfiles"        // get input filelist

// Communication
#define EVENT_COMM_ALLTOALL        "event_comm_alltoall"        // MPI_Alltoall
#define EVENT_COMM_ALLTOALLV       "event_comm_alltoallv"       // MPI_Alltoallv
#define EVENT_COMM_ALLREDUCE       "event_comm_allreduce"       // MPI_Allreduce

// Disk IO
#define EVENT_PFS_OPEN             "event_pfs_open"     // Open file
#define EVENT_PFS_SEEK             "event_pfs_seek"     // Seek file
#define EVENT_PFS_READ             "event_pfs_read"     // Read file
#define EVENT_PFS_CLOSE            "event_pfs_close"    // Close file


// Define initialize and uninitialize
#define INIT_STAT(rank, size, comm) \
{\
    if (stat_ref == 0){\
        stat_rank=rank; \
        stat_size=size; \
        stat_comm=comm;\
        PROFILER_START; \
        TRACKER_START; \
        init_wtime=MR_GET_WTIME();\
    }\
    stat_ref+=1;\
}

#define UNINIT_STAT \
{\
    stat_ref-=1;\
    if (stat_ref == 0){\
        PROFILER_END; \
        TRACKER_END; \
    }\
}

#define GET_CUR_TIME \
{\
    if (stat_rank==0){\
        time_t t = time(NULL);\
        struct tm tm = *localtime(&t);\
        sprintf(timestr, "%d-%d-%d-%d:%d:%d", \
                tm.tm_year + 1900,\
                tm.tm_mon + 1,\
                tm.tm_mday,\
                tm.tm_hour,\
                tm.tm_min,\
                tm.tm_sec);\
    }\
}

// Define profiler
#ifndef ENABLE_PROFILER

#define PROFILER_START
#define PROFILER_END
#define PROFILER_RECORD_TIME_START
#define PROFILER_RECORD_TIME_END(timer_type)
#define PROFILER_RECORD_COUNT(counter_type, count, op)
#define PROFILER_PRINT(filename)

#else

#define PROFILER_START \
{\
    profiler_timer=new double[TIMER_NUM];\
    for(int i=0; i < TIMER_NUM; i++) profiler_timer[i]=0.0;\
    profiler_counter=new uint64_t[COUNTER_NUM];\
    for(int i=0; i < COUNTER_NUM; i++) profiler_counter[i]=0.0;\
}

#define PROFILER_END \
{\
    delete [] profiler_timer;\
    delete [] profiler_counter;\
}

#define PROFILER_RECORD_TIME_START \
    profiler_info.prev_wtime=MR_GET_WTIME();

#define PROFILER_RECORD_TIME_END(timer_type) \
    profiler_timer[timer_type]+=\
(MR_GET_WTIME()-profiler_info.prev_wtime);\

#define PROFILER_RECORD_COUNT(counter_type, count, op) \
{\
    if (op==OPSUM){\
        profiler_counter[counter_type]+=count;\
    }else if (op==OPMAX){\
        if (profiler_counter[counter_type] < count){\
            profiler_counter[counter_type]=count;\
        }\
    }\
}

#define PROFILER_PRINT(filename) \
{\
    profiler_timer[TIMER_TOTAL]=MR_GET_WTIME()-init_wtime;\
    profiler_counter[COUNTER_PEAKMEM_USE]=peakmem;\
    char fullname[1024];\
    FILE *fp=NULL;\
    if (stat_rank==0){\
        sprintf(fullname, "%s_%s_profile.txt", filename, timestr);\
        printf("filename=%s\n", fullname);\
        fp = fopen(fullname, "w+");\
        fprintf(fp, "testtime,rank,size");\
        for(int i=0; i<TIMER_NUM; i++) fprintf(fp, ",%s", timer_str[i]);\
        for(int i=0; i<COUNTER_NUM; i++) fprintf(fp, ",%s", counter_str[i]);\
        fprintf(fp, "\n%s,0,%d", timestr, stat_size);\
        for(int i=0; i<TIMER_NUM; i++) fprintf(fp, ",%g", profiler_timer[i]);\
        for(int i=0; i<COUNTER_NUM; i++) fprintf(fp, ",%ld", profiler_counter[i]);\
    }\
    if (stat_rank==0){\
        MPI_Status st;\
        for(int i=1; i<stat_size; i++){\
            fprintf(fp, "\n%s,%d,%d", timestr, i, stat_size);\
            MPI_Recv(profiler_timer, TIMER_NUM, MPI_DOUBLE, i, 0x11, stat_comm, &st);\
            for(int i=0; i<TIMER_NUM; i++) fprintf(fp, ",%g", profiler_timer[i]);\
            MPI_Recv(profiler_counter, COUNTER_NUM, MPI_UINT64_T, i, 0x22, stat_comm, &st);\
            for(int i=0; i<COUNTER_NUM; i++) fprintf(fp, ",%ld", profiler_counter[i]);\
        }\
    }else{\
        MPI_Send(profiler_timer, TIMER_NUM, MPI_DOUBLE, 0, 0x11, stat_comm);\
        MPI_Send(profiler_counter, COUNTER_NUM, MPI_UINT64_T, 0, 0x22, stat_comm);\
    }\
    if (stat_rank==0) fclose(fp);\
    MPI_Barrier(stat_comm);\
}

#endif

// Tracker
#ifndef ENABLE_TRACKER

#define TRACKER_START
#define TRACKER_END
#define TRACKER_RECORD_EVENT(event_type)
//#define TRACKER_GATHER
#define TRACKER_PRINT(filename)

#else

#define TRACKER_START  \
{\
    if (stat_rank==0){\
        tracker_event=new std::vector<std::pair<std::string,double> >[stat_size];\
    }else{\
        tracker_event=new std::vector<std::pair<std::string,double> >[1];\
    }\
    tracker_info.prev_wtime=MR_GET_WTIME();\
}

#define TRACKER_END \
{\
    delete [] tracker_event;\
}

#define TRACKER_RECORD_EVENT(event_type) \
{\
    double t_start=MR_GET_WTIME();\
    double t_prev=tracker_info.prev_wtime;\
    tracker_event[0].push_back(std::make_pair(event_type, t_start-t_prev));\
    double t_end=MR_GET_WTIME();\
    tracker_info.prev_wtime=t_end;\
}

#define TRACKER_PRINT(filename) \
{\
    int total_bytes=0, max_bytes=0;\
    std::vector<std::pair<std::string,double> >::iterator iter;\
    for(iter=tracker_event[0].begin(); iter!=tracker_event[0].end(); iter++){\
        max_bytes+=(int)strlen(iter->first.c_str())+1;\
        max_bytes+=(int)sizeof(iter->second);\
    }\
    MPI_Reduce(&max_bytes, &total_bytes, 1, MPI_INT, MPI_MAX, 0, stat_comm);\
    if (max_bytes>total_bytes) total_bytes=max_bytes;\
    char *tmp=(char*)mem_aligned_malloc(MEMPAGE_SIZE, total_bytes);\
    if (stat_rank==0){\
        MPI_Status st;\
        for(int i=0; i<stat_size-1; i++){\
            MPI_Recv(tmp, total_bytes, MPI_BYTE, MPI_ANY_SOURCE, 0x33, stat_comm, &st);\
            int recv_rank=st.MPI_SOURCE;\
            int recv_count=0;\
            MPI_Get_count(&st, MPI_BYTE, &recv_count);\
            int off=0;\
            while (off<recv_count){\
                char *type=tmp+off;\
                off+=(int)strlen(type)+1;\
                double value=*(double*)(tmp+off);\
                tracker_event[recv_rank].push_back(std::make_pair(type, value));\
                off+=(int)sizeof(double);\
            }\
        }\
    }else{\
        int off=0;\
        std::vector<std::pair<std::string,double> >::iterator iter;\
        for(iter=tracker_event[0].begin(); iter!=tracker_event[0].end(); iter++){\
            memcpy(tmp+off, iter->first.c_str(), strlen(iter->first.c_str())+1);\
            off+=(int)strlen(iter->first.c_str())+1;\
            memcpy(tmp+off, &(iter->second), sizeof(double));\
            off+=(int)sizeof(iter->second);\
        }\
        MPI_Send(tmp, off, MPI_BYTE, 0, 0x33, stat_comm);\
    }\
    mem_aligned_free(tmp);\
    char fullname[1024];\
    FILE *fp=NULL;\
    if (stat_rank==0){\
        sprintf(fullname, "%s_%s_trace.txt", filename, timestr);\
        printf("filename=%s\n", fullname);\
        fp = fopen(fullname, "w+");\
        for(int i=0; i<stat_size; i++){\
            fprintf(fp, "rank:%d,size:%d",i,stat_size);\
            std::vector<std::pair<std::string,double> >::iterator iter;\
            for(iter=tracker_event[i].begin(); \
                iter!=tracker_event[i].end(); iter++){\
                fprintf(fp, ",%s:%g", iter->first.c_str(), iter->second);\
            }\
            fprintf(fp, "\n");\
        }\
        fclose(fp);\
    }\
    MPI_Barrier(stat_comm);\
}

#if 0
#define TRACKER_PRINT(filename) \
{\
    char tmp[1024];\
    if (stat_rank==0){\
        sprintf(tmp, "%s_trace.txt", filename);\
        FILE *fp = fopen(tmp, "w+");\
        for(int i=0; i<stat_size; i++){\
            fprintf(fp, "rank:%d,size:%d",i,stat_size);\
            std::vector<std::pair<std::string,double> >::iterator iter;\
            for(iter=tracker_event[i].begin(); \
                iter!=tracker_event[i].end(); iter++){\
                fprintf(fp, ",%s:%g", iter->first.c_str(), iter->second);\
            }\
            fprintf(fp, "\n");\
        }\
        fclose(fp);\
    }\
}
#endif

#endif

#endif
