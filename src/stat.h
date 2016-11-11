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
#include <omp.h>

#include <map>
#include <vector>
#include <string>

#include "memory.h"

typedef struct _profiler_info{
  double prev_wtime;
}Profiler_info;

typedef struct _tracker_info{
  double prev_wtime;
}Tracker_info;

/// Events
//  Computation
#define EVENT_COMPUTE_MAP          "event_compute_map"
#define EVENT_COMPUTE_RDC          "event_compute_reduce"
#define EVENT_COMPUTE_SCAN         "event_compute_scan"
#define EVENT_COMPUTE_OTHER        "event_compute_other"

// Initialize
#define EVENT_INIT_GETFILES        "event_init_getfiles"

// Communication
#define EVENT_COMM_ALLTOALL        "event_comm_alltoall"
#define EVENT_COMM_ALLTOALLV       "event_comm_alltoallv"
#define EVENT_COMM_ALLREDUCE       "event_comm_allreduce"

// Disk IO
#define EVENT_DISK_OPEN            "event_disk_fopen"
#define EVENT_DISK_SEEK            "event_disk_fseek"
#define EVENT_DISK_READ            "event_disk_fread"
#define EVENT_DISK_CLOSE           "event_disk_fclose"

// Timers
#define TIMER_DISK_IO              "timer_disk_io"
#define TIMER_COMM_A2A             "timer_comm_a2a"
#define TIMER_COMM_A2AV            "timer_comm_a2av"
#define TIMER_COMM_RDC             "timer_comm_rdc"

// Counters
#define COUNTER_SEND_BYTES         "counter_send_bytes"
#define COUNTER_RECV_BYTES         "counter_recv_bytes"
#define COUNTER_FILE_COUNT         "counter_file_count"
#define COUNTER_FILE_SIZE          "counter_file_size"
#define COUNTER_INBUF_SIZE         "counter_inbuf_size"

enum OpType{OPSUM, OPMAX};

// Interfaces 
#define INIT_STAT(rank, size)  INIT_STAT_THR(rank, size, 1)
#define UNINT_STAT   UNINIT_STAT_THR
#define PROFILER_RECORD_TIME_START \
    PROFILER_RECORD_TIME_START_THR(0)
#define PROFILER_RECORD_TIME_END(timer_type) \
    PROFILER_RECORD_TIME_END_THR(0, timer_type) 
#define PROFILER_RECORD_COUNT(counter_type, count, op) \
    PROFILER_RECORD_COUNT_THR(0, counter_type, count, op)
#define TRACKER_RECORD_EVENT(event_type) \
    TRACKER_RECORD_EVENT_THR(0, event_type)

// Ensure varibales accessibale outside
extern int stat_ref, stat_rank, stat_size;
extern int thread_count; 
extern double init_wtime;
extern std::map<std::string,double> *profiler_timer;
extern std::map<std::string,uint64_t> *profiler_counter;
extern std::vector<std::pair<std::string,double> > *tracker_event;
extern Tracker_info *tracker_info;
extern Profiler_info *profiler_info;

//#ifdef MTMR_MULTITHREAD
//#define MR_GET_WTIME() omp_get_wtime()
//#else
#define MR_GET_WTIME() MPI_Wtime()
//#endif

// Define initialize and uninitialize
#define INIT_STAT_THR(rank, size, tnum) \
{\
    if(stat_ref == 0){\
        PROFILER_START_THR(tnum); \
        TRACKER_START_THR(tnum); \
        stat_rank=rank; \
        stat_size=size; \
        thread_count=tnum; \
        init_wtime=MR_GET_WTIME();\
    }\
    stat_ref+=1;\
}

#define UNINIT_STAT_THR \
{\
    stat_ref-=1;\
    if(stat_ref == 0){\
        PROFILER_END_THR; \
        TRACKER_END_THR; \
    }\
}

// Define profiler
#ifndef ENABLE_PROFILER

#define PROFILER_START_THR(thread_count)
#define PROFILER_END_THR
#define PROFILER_RECORD_TIME_START_THR(thread_id)
#define PROFILER_RECORD_TIME_END_THR(thread_id, timer_type)
#define PROFILER_RECORD_COUNT_THR(thread_id, counter_type, count, op)
#define PROFILER_PRINT(out)

#else

#define PROFILER_START_THR(thread_count) \
    profiler_timer=\
        new std::map<std::string,double>[thread_count];\
    profiler_counter=\
        new std::map<std::string,uint64_t>[thread_count];\
    profiler_info=new Profiler_info[thread_count];\

#define PROFILER_END_THR \
    delete [] profiler_timer;\
    delete [] profiler_counter;\
    delete [] profiler_info;

#define PROFILER_RECORD_TIME_START_THR(thread_id) \
    profiler_info[thread_id].prev_wtime=MR_GET_WTIME();

#define PROFILER_RECORD_TIME_END_THR(thread_id, timer_type) \
    (profiler_timer[thread_id])[timer_type]+=\
        (MR_GET_WTIME()-profiler_info[thread_id].prev_wtime);\

#define PROFILER_RECORD_COUNT_THR(thread_id, counter_type, count, op) \
{\
    if(op==OPSUM){\
        (profiler_counter[thread_id])[counter_type]+=count;\
    }else if(op==OPMAX){\
        if((profiler_counter[thread_id])[counter_type] < count){\
            (profiler_counter[thread_id])[counter_type]=count;\
        }\
    }\
}

#define PROFILER_PRINT(out) \
    for(int i=0; i<thread_count; i++){\
        double total_wtime=MR_GET_WTIME()-init_wtime;\
        fprintf(out, "rank:%d,size:%d,tid:%d,tnum:%d",stat_rank,stat_size,i,thread_count);\
        fprintf(out, ",timer_total:%g", total_wtime);\
        std::map<std::string,double>::iterator iter;\
        for(iter=profiler_timer[i].begin(); \
            iter!=profiler_timer[i].end(); iter++){\
            fprintf(out, ",%s:%g", iter->first.c_str(), iter->second);\
        }\
        fprintf(out, ",peakmem:%ld", peakmem);\
        std::map<std::string,uint64_t>::iterator iter1;\
        for(iter1=profiler_counter[i].begin(); \
            iter1!=profiler_counter[i].end(); iter1++){\
            fprintf(out, ",%s:%ld", iter1->first.c_str(), iter1->second);\
        }\
    }

#endif

// Tracker
#ifndef ENABLE_TRACKER
#define TRACKER_START_THR(thread_count)
#define TRACKER_END_THR
#define TRACKER_RECORD_EVENT_THR(thread_id, event_type)
#define TRACKER_PRINT(out)
#else

#define TRACKER_START_THR(thread_count)  \
    tracker_info=new Tracker_info[thread_count];\
    tracker_event=\
        new std::vector<std::pair<std::string,double> >[thread_count];\
    for(int i=0; i<thread_count; i++){\
        tracker_info[i].prev_wtime=MR_GET_WTIME();\
    }

#define TRACKER_END_THR \
    delete [] tracker_info;\
    delete [] tracker_event;

#define TRACKER_RECORD_EVENT_THR(thread_id, event_type) \
{\
    double t_start=MR_GET_WTIME();\
    double t_prev=tracker_info[thread_id].prev_wtime;\
    tracker_event[thread_id].push_back(\
        std::make_pair<std::string,double>(event_type, t_start-t_prev));\
    double t_end=MR_GET_WTIME();\
    tracker_info[thread_id].prev_wtime=t_end;\
}

#define TRACKER_PRINT(out) \
    for(int i=0; i<thread_count; i++){\
        fprintf(out, "rank:%d,size:%d,tid:%d,tnum:%d",stat_rank,stat_size,i,thread_count);\
        std::vector<std::pair<std::string,double> >::iterator iter;\
        for(iter=tracker_event[i].begin(); \
            iter!=tracker_event[i].end(); iter++){\
            fprintf(out, ",%s:%g", iter->first.c_str(), iter->second);\
        }\
    }

#endif

#endif
