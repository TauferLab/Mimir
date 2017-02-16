/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#include "stat.h"


// Communication information
MPI_Comm stat_comm;
int stat_ref = 0, stat_rank = 0, stat_size = 0;

// Time to init the application
double init_wtime = 0;

// structure for profiler and tracker
Profiler_info profiler_info;
double *profiler_timer = NULL;
uint64_t *profiler_counter = NULL;

char timestr[1024];

const char *timer_str[TIMER_NUM] = {
    "total_time",
    "pfs_io_time",
    "mpi_a2a_time",
    "mpi_a2av_time",
    "mpi_rdc_time",
};

const char *counter_str[COUNTER_NUM] = {
    "bucket_size",
    "inbuf_size",
    "page_size",
    "comm_size",
    "send_bytes",
    "recv_bytes",
    "file_count",
    "file_size",
    "max_file",
    "send_tail",
    "recv_tail",
    "max_pages",
    "reduce_bucket",
    "combine_bucket",
    "peakmem_use",
    "max_unique_count",
};

// trace data
Tracker_info tracker_info;
std::vector <std::pair<std::string, double>> *tracker_event = NULL;
