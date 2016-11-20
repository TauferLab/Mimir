#include "stat.h"

MPI_Comm stat_comm;

// ref data
int stat_ref=0, stat_rank=0, stat_size=0;

// time to init the application
double init_wtime=0;

// structure for profiler and tracker
Profiler_info profiler_info;
Tracker_info tracker_info;

// profile time and count
std::map<std::string,double> *profiler_timer=NULL;
std::map<std::string,uint64_t> *profiler_counter=NULL;


// trace data
std::vector<std::pair<std::string,double> > *tracker_event=NULL;


