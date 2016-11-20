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

typedef struct _profiler_info{
    double   prev_wtime;
}Profiler_info;

typedef struct _tracker_info{
    double   prev_wtime;
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
#define COUNTER_PAGE_SIZE          "counter_page_size"
#define COUNTER_COMM_SIZE          "counter_comm_size"
#define COUNTER_MEM_PAGES          "counter_mem_pages"
#define COUNTER_MEM_BUCKET         "counter_mem_bucket"

enum OpType{OPSUM, OPMAX};

extern MPI_Comm stat_comm;

// Ensure varibales accessibale outside
extern int stat_ref, stat_rank, stat_size;
//extern int thread_count; 
extern double init_wtime;

extern Tracker_info tracker_info;
extern Profiler_info profiler_info;

extern std::map<std::string,double> *profiler_timer;
extern std::map<std::string,uint64_t> *profiler_counter;
extern std::vector<std::pair<std::string,double> > *tracker_event;

#define MR_GET_WTIME() MPI_Wtime()

// Define initialize and uninitialize
#define INIT_STAT(rank, size, comm) \
{\
    if(stat_ref == 0){\
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
    if(stat_ref == 0){\
        PROFILER_END; \
        TRACKER_END; \
    }\
}

// Define profiler
#ifndef ENABLE_PROFILER

#define PROFILER_START
#define PROFILER_END
#define PROFILER_RECORD_TIME_START
#define PROFILER_RECORD_TIME_END(timer_type)
#define PROFILER_RECORD_COUNT(counter_type, count, op)
#define PROFILER_GATHER
#define PROFILER_PRINT(filename, out)

#else

#define PROFILER_START \
{\
    if(stat_rank==0){\
        profiler_timer=new std::map<std::string,double>[stat_size];\
        profiler_counter=new std::map<std::string,uint64_t>[stat_size];\
    }else{\
        profiler_timer=new std::map<std::string,double>[1];\
        profiler_counter=new std::map<std::string,uint64_t>[1];\
    }\
}

#define PROFILER_END \
{\
    delete [] profiler_timer;\
    delete [] profiler_counter;\
}

#define PROFILER_RECORD_TIME_START \
    profiler_info.prev_wtime=MR_GET_WTIME();

#define PROFILER_RECORD_TIME_END(timer_type) \
    (profiler_timer[0])[timer_type]+=\
        (MR_GET_WTIME()-profiler_info.prev_wtime);\

#define PROFILER_RECORD_COUNT(counter_type, count, op) \
{\
    if(op==OPSUM){\
        (profiler_counter[0])[counter_type]+=count;\
    }else if(op==OPMAX){\
        if((profiler_counter[0])[counter_type] < count){\
            (profiler_counter[0])[counter_type]=count;\
        }\
    }\
}

#define PROFILER_GATHER \
{\
    int total_bytes=0;\
    double total_wtime=MR_GET_WTIME()-init_wtime;\
    (profiler_timer[0])["timer_total"]=total_wtime;\
    (profiler_counter[0])["peakmem"]=peakmem;\
    std::map<std::string,double>::iterator iter;\
    for(iter=profiler_timer[0].begin(); iter!=profiler_timer[0].end(); iter++){\
        total_bytes+=strlen(iter->first.c_str())+1;\
        total_bytes+=sizeof(iter->second);\
    }\
    std::map<std::string,uint64_t>::iterator iter1;\
    for(iter1=profiler_counter[0].begin(); iter1!=profiler_counter[0].end(); iter1++){\
        total_bytes+=strlen(iter1->first.c_str())+1;\
        total_bytes+=sizeof(iter1->second);\
    }\
    MPI_Allreduce(&total_bytes, &total_bytes, 1, MPI_INT, MPI_MAX, stat_comm);\
    char *tmp=(char*)mem_aligned_malloc(MEMPAGE_SIZE, total_bytes);\
    if(stat_rank==0){\
        MPI_Status st;\
        for(int i=0; i<stat_size-1; i++){\
            printf("recv1\n");\
            MPI_Recv(tmp, total_bytes, MPI_BYTE, MPI_ANY_SOURCE, 0x11, stat_comm, &st);\
            int recv_rank=st.MPI_SOURCE;\
            int recv_count=0;\
            MPI_Get_count(&st, MPI_BYTE, &recv_count);\
            int off=0;\
            while(off<recv_count){\
                char *type=tmp+off;\
                off+=strlen(type)+1;\
                double value=*(double*)(tmp+off);\
                (profiler_timer[recv_rank])[type]=value;\
                off+=sizeof(double);\
            }\
        }\
        for(int i=0; i<stat_size-1; i++){\
            printf("recv2\n");\
            MPI_Recv(tmp, total_bytes, MPI_BYTE, MPI_ANY_SOURCE, 0x22, stat_comm, &st);\
            int recv_rank=st.MPI_SOURCE;\
            int recv_count=0;\
            MPI_Get_count(&st, MPI_BYTE, &recv_count);\
            int off=0;\
            while(off<recv_count){\
                char *type=tmp+off;\
                off+=strlen(type)+1;\
                uint64_t value=*(uint64_t*)(tmp+off);\
                (profiler_counter[recv_rank])[type]=value;\
                off+=sizeof(uint64_t);\
            }\
        }\
    }else{\
        int off=0;\
        std::map<std::string,double>::iterator iter;\
        for(iter=profiler_timer[0].begin(); iter!=profiler_timer[0].end(); iter++){\
            memcpy(tmp+off, iter->first.c_str(), strlen(iter->first.c_str())+1);\
            off+=strlen(iter->first.c_str())+1;\
            memcpy(tmp+off, &(iter->second), sizeof(iter->second));\
            off+=sizeof(iter->second);\
        }\
        printf("send1: off=%d\n", off);\
        MPI_Send(tmp, off, MPI_BYTE, 0, 0x11, stat_comm);\
        off=0;\
        std::map<std::string,uint64_t>::iterator iter1;\
        for(iter1=profiler_counter[0].begin(); iter1!=profiler_counter[0].end(); iter1++){\
            memcpy(tmp+off, iter1->first.c_str(), strlen(iter1->first.c_str())+1);\
            off+=strlen(iter1->first.c_str())+1;\
            memcpy(tmp+off, &(iter1->second), sizeof(iter1->second));\
            off+=sizeof(iter1->second);\
        }\
        printf("send2: off=%d\n", off);\
        MPI_Send(tmp, off, MPI_BYTE, 0, 0x22, stat_comm);\
    }\
    mem_aligned_free(tmp);\
    MPI_Barrier(stat_comm);\
}

#define PROFILER_PRINT(filename, out) \
{\
    char tmp[1024];\
    if(stat_rank==0){\
        sprintf(tmp, "%s_profiler.txt", filename);\
        FILE *fp = fopen(tmp, "w+");\
        for(int i=0; i<stat_size; i++){\
            fprintf(out, "rank:%d,size:%d",i,stat_size);\
            std::map<std::string,double>::iterator iter;\
            for(iter=profiler_timer[i].begin(); \
                iter!=profiler_timer[i].end(); iter++){\
                fprintf(out, ",%s:%g", iter->first.c_str(), iter->second);\
            }\
            std::map<std::string,uint64_t>::iterator iter1;\
            for(iter1=profiler_counter[i].begin(); \
                iter1!=profiler_counter[i].end(); iter1++){\
                fprintf(out, ",%s:%ld", iter1->first.c_str(), iter1->second);\
            }\
            fprintf(fp, "\n");\
        }\
        fclose(fp);\
    }\
}

#endif

// Tracker
#ifndef ENABLE_TRACKER

#define TRACKER_START
#define TRACKER_END
#define TRACKER_RECORD_EVENT(event_type)
#define TRACKER_GATHER
#define TRACKER_PRINT(filename, out)

#else

#define TRACKER_START  \
{\
    if(stat_rank==0){\
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

#define TRACKER_GATHER \
{\
    int total_bytes=0;\
    std::vector<std::pair<std::string,double> >::iterator iter;\
    for(iter=tracker_event[0].begin(); iter!=tracker_event[0].end(); iter++){\
        total_bytes+=strlen(iter->first.c_str())+1;\
        total_bytes+=sizeof(iter->second);\
    }\
    MPI_Allreduce(&total_bytes, &total_bytes, 1, MPI_INT, MPI_MAX, stat_comm);\
    char *tmp=(char*)mem_aligned_malloc(MEMPAGE_SIZE, total_bytes);\
    if(stat_rank==0){\
        MPI_Status st;\
        for(int i=0; i<stat_size-1; i++){\
            MPI_Recv(tmp, total_bytes, MPI_BYTE, MPI_ANY_SOURCE, 0x33, stat_comm, &st);\
            int recv_rank=st.MPI_SOURCE;\
            int recv_count=0;\
            MPI_Get_count(&st, MPI_BYTE, &recv_count);\
            int off=0;\
            while(off<recv_count){\
                char *type=tmp+off;\
                off+=strlen(type)+1;\
                double value=*(double*)(tmp+off);\
                tracker_event[recv_rank].push_back(std::make_pair(type, value));\
                off+=sizeof(double);\
            }\
        }\
    }else{\
        int off=0;\
        std::vector<std::pair<std::string,double> >::iterator iter;\
        for(iter=tracker_event[0].begin(); iter!=tracker_event[0].end(); iter++){\
            memcpy(tmp+off, iter->first.c_str(), strlen(iter->first.c_str())+1);\
            off+=strlen(iter->first.c_str())+1;\
            memcpy(tmp+off, &(iter->second), sizeof(double));\
            off+=sizeof(iter->second);\
        }\
        MPI_Send(tmp, off, MPI_BYTE, 0, 0x33, stat_comm);\
    }\
    mem_aligned_free(tmp);\
    MPI_Barrier(stat_comm);\
}

#define TRACKER_PRINT(filename, out) \
{\
    char tmp[1024];\
    if(stat_rank==0){\
        sprintf(tmp, "%s_trace.txt", filename);\
        FILE *fp = fopen(tmp, "w+");\
        for(int i=0; i<stat_size; i++){\
            fprintf(out, "rank:%d,size:%d",i,stat_size);\
            std::vector<std::pair<std::string,double> >::iterator iter;\
            for(iter=tracker_event[i].begin(); \
                iter!=tracker_event[i].end(); iter++){\
                fprintf(out, ",%s:%g", iter->first.c_str(), iter->second);\
            }\
            fprintf(fp, "\n");\
        }\
        fclose(fp);\
    }\
}

#endif

#endif
