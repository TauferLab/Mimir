#include "stat.h"

#ifdef ENABLE_TRACKER
//bool enable_tracker=false;
int tracker_ref=0;
std::vector<std::pair<std::string,double> > *tracker_event_timer=NULL;
tracker_thread_info *tracker_info;
#endif

#ifdef ENABLE_PROFILER
//bool enable_profiler=false;
int profiler_ref=0;
std::map<std::string,double> *profiler_event_timer=NULL;
std::map<std::string,uint64_t> *profiler_event_counter=NULL;
#endif
