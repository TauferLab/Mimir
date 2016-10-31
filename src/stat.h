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

typedef struct _profiler_info{
  double prev_wtime;
}Profiler_info;

typedef struct _tracker_info{
  double prev_wtime;
}Tracker_info;

/// Events
//  Computation
#define EVENT_COMPUTE_MAP          "compute_map"
#define EVENT_COMPUTE_RDC          "compute_reduce"
#define EVENT_COMPUTE_SCAN         "compute_scan"
#define EVENT_COMPUTE_OTHER        "compute_other"

// Initialize
#define EVENT_INIT_GETFILES        "init_getfiles"

// Communication
#define EVENT_COMM_ALLTOALL        "comm_alltoall"
#define EVENT_COMM_ALLTOALLV       "comm_alltoallv"
#define EVENT_COMM_ALLREDUCE       "comm_allreduce"

// Disk IO
#define EVENT_DISK_OPEN            "disk_fopen"
#define EVENT_DISK_SEEK            "disk_fseek"
#define EVENT_DISK_READ            "disk_fread"
#define EVENT_DISK_CLOSE           "disk_fclose"


// Timers
//#define TIMER_MAP_FOP            "timer_map_fop"
//#define TIMER_MAP_ATOMIC         "timer_map_atomic"

// Counters
//#define COUNTER_COMM_THREAD_BUF  "counter_comm_thread_buf"
//#define COUNTER_COMM_SEND_BUF    "counter_comm_send_buf"
//#define COUNTER_COMM_RECV_BUF    "counter_comm_recv_buf"
//#define COUNTER_COMM_SEND_SIZE   "counter_comm_send_size"
//#define COUNTER_COMM_RECV_SIZE   "counter_comm_recv_size"
//#define COUNTER_COMM_SEND_PAD    "counter_comm_send_padding"
//#define COUNTER_COMM_RECV_PAD    "counter_comm_recv_padding"
//#define COUNTER_MAP_FILE_COUNT   "counter_map_file_count"
//#define COUNTER_MAP_FILE_SIZE    "counter_map_file_size"
//#define COUNTER_MAP_INPUT_SIZE   "counter_map_input_size"
//#define COUNTER_MAP_INPUT_BUF    "counter_map_input_buf"
//#define COUNTER_MAP_FILE_KV      "counter_map_file_kv"
//#define COUNTER_MAP_FILE_PAGE    "counter_map_file_page"
//#define COUNTER_MAP_OUTPUT_KV    "counter_map_output_kv"
//#define COUNTER_MAP_OUTPUT_PAGE  "counter_map_output_page"
//#define COUNTER_MAP_KV_COUNT     "counter_map_kv_count"
//#define COUNTER_CPS_INPUT_KV     "counter_cps_input_kv"
//#define COUNTER_CPS_PR_OUTKV     "counter_cps_pr_output_kv"
//#define COUNTER_CPS_OUTPUT_KV    "counter_cps_output_kv"
//#define COUNTER_PR_BUCKET_SIZE   "counter_pr_bucket_size"
//#define COUNTER_PR_UNIQUE_SIZE   "counter_pr_unique_size"
//#define COUNTER_PR_KMV_SIZE      "counter_pr_kmv_size"
//#define COUNTER_PR_OUTPUT_KV     "counter_pr_output_kv"
//#define COUNTER_CVT_BUCKET_SIZE  "counter_cvt_bucket_size"
//#define COUNTER_CVT_UNIQUE_SIZE  "counter_cvt_unique_size"
//#define COUNTER_CVT_SET_SIZE     "counter_cvt_set_size"
//#define COUNTER_CVT_NUNIQUE      "counter_cvt_nunique"
//#define COUNTER_CVT_KMV_SIZE     "counter_cvt_kmv_size"
//#define COUNTER_RDC_INPUT_KV     "counter_rdc_input_kv"
//#define COUNTER_RDC_OUTPUT_KV    "counter_rdc_output_kv"

// Interfaces 
#define INIT_STAT(rank, size)  INIT_STAT_THR(rank, size, 1)
#define UNINT_STAT   UNINIT_STAT_THR
#define PROFILER_RECORD_TIME_START \
    PROFILER_RECORD_TIME_START_THR(0)
#define PROFILER_RECORD_TIME_END(timer_type)\
    PROFILER_RECORD_TIME_END_THR(0) 
#define PROFILER_RECORD_COUNT(counter_type, count) \
    PROFILER_RECORD_COUNT_THR(0, counter_type, count)
#define TRACKER_RECORD_EVENT(event_type) \
    TRACKER_RECORD_EVENT_THR(0, event_type)

// Ensure varibales accessibale outside
extern int stat_ref, stat_rank, stat_size;
extern int thread_count; 
extern std::map<std::string,double> *profiler_event_timer;
extern std::map<std::string,uint64_t> *profiler_event_counter;
extern std::vector<std::pair<std::string,double> > *tracker_event_timer;
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
        PROFILER_START_THR(tnum) \
        TRACKER_START_THR(tnum) \
        stat_rank=rank;\
        stat_size=size;\
        thread_count=tnum;\
    }\
    stat_ref+=1;\
}

#define UNINIT_STAT_THR \
{\
    stat_ref-=1;\
    if(stat_ref == 0){\
        PROFILER_END_THR \
        TRACKER_END_THR \
    }\
}

// Define profiler
#ifndef ENABLE_PROFILER

#define PROFILER_START_THR(thread_count)
#define PROFILER_END_THR
#define PROFILER_RECORD_TIME_START_THR(thread_id)
#define PROFILER_RECORD_TIME_END_THR(thread_id, timer_type)
#define PROFILER_RECORD_COUNT_THR(thread_id, counter_type, count)
#define PROFILER_PRINT(out)

#else

#define PROFILER_START_THR(thread_count) \
    profiler_event_timer=\
        new std::map<std::string,double>[thread_count];\
    profiler_event_counter=\
        new std::map<std::string,uint64_t>[thread_count];\
    profiler_info=new Profiler_info[thread_count];\

#define PROFILER_END_THR \
    delete [] profiler_event_timer;\
    delete [] profiler_event_counter;\
    delete [] profiler_info;

#define PROFILER_RECORD_TIME_START_THR(thread_id) \
    profiler_start[thread_id].pre_wtime=MR_GET_WTIME();

#define PROFILER_RECORD_TIME_END_THR(thread_id, timer_type) \
    (profiler_event_timer[thread_id])[timer_type]+=\
        (MR_GET_WTIME()-profiler_start[thread_id].pre_wtime);\

#define PROFILER_RECORD_COUNT_THR(thread_id, counter_type, count) \
    (profiler_event_counter[thread_id])[counter_type]+=count;

#define PROFILER_PRINT(out) \
    for(int i=0; i<thread_count; i++){\
        fprintf(out, "rank:%d,size:%d,thread:%d",stat_rank,stat_size,i);\
        std::map<std::string,double>::iterator iter;\
        for(iter=profiler_event_timer[i].begin(); \
            iter!=profiler_event_timer[i].end(); iter++){\
            fprintf(out, ",%s:%g", iter->first.c_str(), iter->second);\
        }\
        std::map<std::string,uint64_t>::iterator iter1;\
        for(iter1=profiler_event_counter[i].begin(); \
            iter1!=profiler_event_counter[i].end(); iter1++){\
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
    tracker_event_timer=\
        new std::vector<std::pair<std::string,double> >[thread_count];\
    for(int i=0; i<thread_count; i++){\
        tracker_info[i].prev_wtime=MR_GET_WTIME();\
    }

#define TRACKER_END_THR \
    delete [] tracker_info;\
    delete [] tracker_event_timer;

#define TRACKER_RECORD_EVENT_THR(thread_id, event_type) \
{\
    double t_start=MR_GET_WTIME();\
    double t_prev=tracker_info[thread_id].prev_wtime;\
    tracker_event_timer[thread_id].push_back(\
        std::make_pair<std::string,double>(event_type, t_start-t_prev));\
    double t_end=MR_GET_WTIME();\
    tracker_info[thread_id].prev_wtime=t_end;\
}

#define TRACKER_PRINT(out) \
    for(int i=0; i<thread_count; i++){\
        fprintf(out, "rank:%d,size:%d,thread:%d",stat_rank,stat_size,i);\
        std::vector<std::pair<std::string,double> >::iterator iter;\
        for(iter=tracker_event_timer[i].begin(); \
            iter!=tracker_event_timer[i].end(); iter++){\
            fprintf(out, ",%s:%g", iter->first.c_str(), iter->second);\
        }\
    }

#endif

#endif
