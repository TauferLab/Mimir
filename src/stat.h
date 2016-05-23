#ifndef STAT_H
#define STAT_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <omp.h>

#include <map>
#include <vector>
#include <string>

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
  tracker_info[thread_id].prev_wtime=omp_get_wtime();\
  tracker_info[thread_id].overhead=0.0;

#define TRACKER_RECORD_EVENT(thread_id, event_type) {\
  double t_start=omp_get_wtime();\
  double t_prev=tracker_info[thread_id].prev_wtime;\
  printf("event_type=%s, thread_id=%d, %g\n", event_type, thread_id, t_start-t_prev);fflush(stdout);\
  tracker_event_timer[thread_id].push_back(\
   std::make_pair<std::string,double>(event_type, t_start-t_prev));\
  double t_end=omp_get_wtime();\
  tracker_info[thread_id].prev_wtime=t_end;\
  tracker_info[thread_id].overhead+=t_end-t_start;}

#define TRACKER_END \
  delete [] tracker_info;\
  delete [] tracker_event_timer;\
  enable_tracker=false;

#define TRACKER_PRINT(out, thread_count) \
  fprintf(out, "action:tracker_start");\
  for(int i=0;i<thread_count; i++){\
    std::vector<std::pair<std::string,double> >::iterator iter;\
    for(iter=tracker_event_timer[i].begin(); iter!=tracker_event_timer[i].end(); iter++){\
      fprintf(out, ",%s:%g", iter->first.c_str(), iter->second);\
    }\
  }\
  fprintf(out, ",action:tracker_stop");

#endif

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
  double profiler_t_start=omp_get_wtime();

#define PROFILER_RECORD_TIME_END(thread_id, timer_type) \
  double profiler_t_stop=omp_get_wtime();\
  (profiler_event_timer[thread_id])[timer_type]+=(profiler_t_stop-profiler_t_start);

#define PROFILER_RECORD_COUNT(thread_id, counter_type, count) \
  (profiler_event_counter[thread_id])[counter_type]+=count;

#define PROFILER_END \
  delete [] profiler_event_timer;\
  delete [] profiler_event_counter;\
  enable_profiler=false;

//#define PROFILER_PRINT(out, thread_count) 

//#if 0
#define PROFILER_PRINT(out, thread_count) \
  fprintf(out, "action:profiler_start");\
  for(int i=0; i<thread_count; i++){\
    std::map<std::string,double>::iterator iter;\
    for(iter=profiler_event_timer[i].begin(); iter!=profiler_event_timer[i].end(); iter++){\
      fprintf(out, ",%s:%g", iter->first.c_str(), iter->second);\
    }\
    std::map<std::string,uint64_t>::iterator iter1;\
    for(iter1=profiler_event_counter[i].begin(); iter1!=profiler_event_counter[i].end(); iter1++){\
      fprintf(out, ",%s:%ld", iter1->first.c_str(), iter1->second);\
    }\
  }\
  fprintf(out, ",action:profiler_stop");
#endif

//#endif

#endif
