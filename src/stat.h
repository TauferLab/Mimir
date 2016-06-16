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

// Events
#define EVENT_MR_GENERAL         "event_mr_general"
#define EVENT_MAP_COMPUTING      "event_map"
#define EVENT_MAP_DIS_FILES      "event_distribute_files"
#define EVENT_CPS_COMPUTING      "event_compress"
#define EVENT_RDC_COMPUTING      "event_reduce"
#define EVENT_OMP_IDLE           "event_omp_idle"
#define EVENT_OMP_BARRIER        "event_omp_barrier"
#define EVENT_COMM_ALLTOALL      "event_comm_alltoall"
#define EVENT_COMM_WAIT          "event_comm_wait"
#define EVENT_COMM_IALLTOALLV    "event_comm_ialltoallv"
#define EVENT_COMM_ALLTOALLV     "event_comm_alltoallv"
#define EVENT_COMM_ALLREDUCE     "event_comm_allreduce"
#define EVENT_PFS_OPEN           "event_pfs_open"
#define EVENT_PFS_SEEK           "event_pfs_seek"
#define EVENT_PFS_READ           "event_pfs_read"
#define EVENT_PFS_CLOSE          "event_pfs_close"
// Timers
#define TIMER_MAP_FOP            "timer_map_fop"
#define TIMER_MAP_ATOMIC         "timer_map_atomic"
// Counters
#define COUNTER_COMM_THREAD_BUF  "counter_comm_thread_buf"
#define COUNTER_COMM_SEND_BUF    "counter_comm_send_buf"
#define COUNTER_COMM_RECV_BUF    "counter_comm_recv_buf"
#define COUNTER_COMM_SEND_SIZE   "counter_comm_send_size"
#define COUNTER_COMM_RECV_SIZE   "counter_comm_recv_size"
#define COUNTER_COMM_SEND_PAD    "counter_comm_send_padding"
#define COUNTER_COMM_RECV_PAD    "counter_comm_recv_padding"
#define COUNTER_MAP_FILE_COUNT   "counter_map_file_count"
#define COUNTER_MAP_FILE_SIZE    "counter_map_file_size"
#define COUNTER_MAP_INPUT_SIZE   "counter_map_input_size"
#define COUNTER_MAP_OUTPUT_KV    "counter_map_output_kv"
#define COUNTER_MAP_KV_COUNT     "counter_map_kv_count"
#define COUNTER_CPS_BUCKET_SIZE  "counter_cps_bucket_size"
#define COUNTER_CPS_UNIQUE_SIZE  "counter_cps_unique_size"
#define COUNTER_CPS_KMV_SIZE     "counter_cps_kmv_size"
#define COUNTER_CPS_OUTPUT_KV    "counter_cps_output_kv"
#define COUNTER_CVT_BUCKET_SIZE  "counter_cvt_bucket_size"
#define COUNTER_CVT_UNIQUE_SIZE  "counter_cvt_unique_size"
#define COUNTER_CVT_SET_SIZE     "counter_cvt_set_size"
#define COUNTER_CVT_NUNIQUE      "counter_cvt_nunique"
#define COUNTER_CVT_KMV_SIZE     "counter_cvt_kmv_size"
#define COUNTER_RDC_INPUT_KV     "counter_rdc_input_kv"
#define COUNTER_RDC_OUTPUT_KV    "counter_rdc_output_kv"

#ifdef MTMR_MULTITHREAD
#define MR_GET_WTIME() omp_get_wtime()
#else
#define MR_GET_WTIME() MPI_Wtime()
#endif

// Profiler
#ifndef ENABLE_PROFILER
#define PROFILER_START(thread_count)
#define PROFILER_RECORD_TIME_START
#define PROFILER_RECORD_TIME_END(thread_id, timer_type)
#define PROFILER_RECORD_COUNT(thread_id, counter_type, count) 
#define PROFILER_END
#define PROFILER_PRINT(out, thread_count)
#else
extern bool enable_profiler;
extern std::map<std::string,double> *profiler_event_timer;
extern std::map<std::string,uint64_t> *profiler_event_counter;
#define PROFILER_START(thread_count) \
  if(!enable_profiler){\
    profiler_event_timer=new std::map<std::string,double>[thread_count];\
    profiler_event_counter=new std::map<std::string,uint64_t>[thread_count];\
    enable_profiler=true;\
  }

#define PROFILER_RECORD_TIME_START \
  double profiler_t_start=MR_GET_WTIME();

#define PROFILER_RECORD_TIME_END(thread_id, timer_type) \
  double profiler_t_stop=MR_GET_WTIME();\
  (profiler_event_timer[thread_id])[timer_type]+=(profiler_t_stop-profiler_t_start);

#define PROFILER_RECORD_COUNT(thread_id, counter_type, count) \
  (profiler_event_counter[thread_id])[counter_type]+=count;

#define PROFILER_END \
  delete [] profiler_event_timer;\
  delete [] profiler_event_counter;\
  enable_profiler=false;

#define PROFILER_PRINT(out, thread_count) \
  for(int i=0; i<thread_count; i++){\
    fprintf(out, "action:profiler_start");\
    std::map<std::string,double>::iterator iter;\
    for(iter=profiler_event_timer[i].begin(); iter!=profiler_event_timer[i].end(); iter++){\
      fprintf(out, ",%s:%g", iter->first.c_str(), iter->second);\
    }\
    std::map<std::string,uint64_t>::iterator iter1;\
    for(iter1=profiler_event_counter[i].begin(); iter1!=profiler_event_counter[i].end(); iter1++){\
      fprintf(out, ",%s:%ld", iter1->first.c_str(), iter1->second);\
    }\
    fprintf(out, ",action:profiler_end\n");\
  }
#endif

// Tracker
#ifndef ENABLE_TRACKER
#define TRACKER_START(thread_count)
#define TRACKER_TIMER_INIT(thread_id)
#define TRACKER_RECORD_EVENT(thread_id, event_type)
#define TRACKER_END
#define TRACKER_PRINT(out, thread_count)
#else
typedef struct _tracker_thread_info{
  double prev_wtime;
  double overhead;
}tracker_thread_info;

extern bool enable_tracker;

extern std::vector<std::pair<std::string,double> > *tracker_event_timer;
extern tracker_thread_info *tracker_info;

#define TRACKER_START(thread_count)  \
  if(!enable_tracker){\
    tracker_info=new tracker_thread_info[thread_count];\
    tracker_event_timer=new std::vector<std::pair<std::string,double> >[thread_count];\
    enable_tracker=true;\
  }

#define TRACKER_TIMER_INIT(thread_id) \
  tracker_info[thread_id].prev_wtime=MR_GET_WTIME();\
  tracker_info[thread_id].overhead=0.0;

#define TRACKER_RECORD_EVENT(thread_id, event_type) {\
  double t_start=MR_GET_WTIME();\
  double t_prev=tracker_info[thread_id].prev_wtime;\
  tracker_event_timer[thread_id].push_back(\
   std::make_pair<std::string,double>(event_type, t_start-t_prev));\
  double t_end=MR_GET_WTIME();\
  tracker_info[thread_id].prev_wtime=t_end;\
  tracker_info[thread_id].overhead+=t_end-t_start;}

#define TRACKER_END \
  delete [] tracker_info;\
  delete [] tracker_event_timer;\
  enable_tracker=false;

#endif

#endif
