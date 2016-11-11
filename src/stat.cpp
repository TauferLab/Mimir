#include "stat.h"

int stat_ref=0;
int stat_rank=0, stat_size=0;
int thread_count=0;

double init_wtime=0;

Profiler_info *profiler_info=NULL;
Tracker_info *tracker_info=NULL;

std::map<std::string,double> *profiler_timer=NULL;
std::map<std::string,uint64_t> *profiler_counter=NULL;

std::vector<std::pair<std::string,double> > *tracker_event=NULL;

