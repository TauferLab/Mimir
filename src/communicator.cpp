#include <stdio.h>
#include <stdlib.h>
#include "log.h"
#include "config.h"
#include "const.h"
#include "memory.h"
#include "communicator.h"
#include "alltoall.h"
#include "stat.h"

using namespace MAPREDUCE_NS;

Communicator* Communicator::Create(MPI_Comm _comm, int _commmode){
    Communicator *c=NULL;
    if(_commmode==0)
      c = new Alltoall(_comm);
    else
      LOG_ERROR("Create communicator error mode=%d!\n", _commmode);
    return c;
}

Communicator::Communicator(MPI_Comm _comm, int _commtype){
    comm = _comm;
    commtype = _commtype;

    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &size);

    medone=pdone=0;

    data=NULL;
    blockid=-1;

    send_buf_size = 0;
    nbuf = 0;
    send_buffers = NULL;
    send_offsets = NULL;
}

Communicator::~Communicator(){

    if(data != NULL){
      if(blockid!=-1)
        data->release_block(blockid);
    }

    for(int i = 0; i < nbuf; i++){
      if(send_buffers != NULL && send_buffers[i]) mem_aligned_free(send_buffers[i]);
      if(send_offsets !=NULL && send_offsets[i]) mem_aligned_free(send_offsets[i]);
    }

    if(send_buffers != NULL) delete [] send_buffers;
    if(send_offsets != NULL) delete [] send_offsets;
}

int Communicator::setup(int64_t _sbufsize, DataObject *_data){
    if(_sbufsize < (int64_t)COMM_UNIT_SIZE*(int64_t)size){
      LOG_ERROR("Error: send buffer(%ld) should be larger than COMM_UNIT_SIZE(%d)*size(%d).\n", \
        _sbufsize, COMM_UNIT_SIZE, size);
    }

    // Calculate the send buffer to each process
    send_buf_size = (_sbufsize/COMM_UNIT_SIZE/size)*COMM_UNIT_SIZE;

    if(send_buf_size < 0){
      LOG_ERROR("Error: send buffer size (%ld) should be > 0\n", send_buf_size);
    }

    medone=pdone=0;
    data=_data;
    blockid=-1;

#ifndef MIMIR_COMM_NONBLOCKING
    nbuf = 1;
#else
    nbuf = 2;
#endif

    send_buffers = new char*[nbuf];
    send_offsets = new int*[nbuf];

    int64_t total_send_buf_size=send_buf_size*(int64_t)size;
    for(int i = 0; i < nbuf; i++){
      send_buffers[i] = (char*)mem_aligned_malloc(MEMPAGE_SIZE, total_send_buf_size);
      send_offsets[i] = (int*)mem_aligned_malloc(MEMPAGE_SIZE, size*sizeof(int));
      for(int j = 0; j < size; j++) send_offsets[i][j] = 0;
    }

    PROFILER_RECORD_COUNT(0, COUNTER_COMM_SEND_BUF, total_send_buf_size*nbuf);

    return 0;
}
