#include <stdio.h>
#include <stdlib.h>
#include <omp.h>

#include "log.h"
#include "config.h"

#include "const.h"

#include "memory.h"

#include "communicator.h"
#include "alltoall.h"
#include "ptop.h"

using namespace MAPREDUCE_NS;

#if GATHER_STAT
#include "stat.h"
#endif

Communicator* Communicator::Create(MPI_Comm _comm, int _tnum, int _commmode){
  Communicator *c=NULL;
  if(_commmode==0)
    c = new Alltoall(_comm, _tnum);
  else if(_commmode==1)
    c = new Ptop(_comm, _tnum);
  else
    LOG_ERROR("Create communicator error mode=%d!\n", _commmode);
  return c;
}


Communicator::Communicator(MPI_Comm _comm, int _commtype, int _tnum){

  comm = _comm;
  commtype = _commtype;
  tnum = _tnum;
  
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &size);

  kvtype = ksize = vsize = 0;

  thread_buf_size = send_buf_size = nbuf = 0;

  thread_buffers = NULL;
  thread_offsets = NULL;
  send_buffers = NULL;
  send_offsets = NULL;

  blocks = new int[tnum];

  init();
}

Communicator::~Communicator(){

  if(!data){
    for(int i=0; i<tnum; i++)
      if(blocks[i] !=- 1) data->releaseblock(blocks[i]);
  }
  delete [] blocks;

  for(int i = 0; i < tnum; i++){
    //printf("free: buffers[%d]=%p\n", i, local_buffers[i]);
    if(thread_buffers && thread_buffers[i]) mem_aligned_free(thread_buffers[i]);
    if(thread_offsets && thread_offsets[i]) mem_aligned_free(thread_offsets[i]);
  }

  for(int i = 0; i < nbuf; i++){
    if(send_buffers && send_buffers[i]) mem_aligned_free(send_buffers[i]);
    if(send_offsets && send_offsets[i]) mem_aligned_free(send_offsets[i]);
  }

  if(thread_buffers) delete [] thread_buffers;
  if(thread_offsets) delete [] thread_offsets;

  if(send_buffers) delete [] send_buffers;
  if(send_offsets) delete [] send_offsets;
}

int Communicator::setup(int _tbufsize, int _sbufsize, int _kvtype, int _ksize, int _vsize, int _nbuf){
  thread_buf_size = _tbufsize*UNIT_1K_SIZE;
  send_buf_size = _sbufsize*UNIT_1M_SIZE;
  kvtype = _kvtype;
  ksize = _ksize;
  vsize = _vsize;
  nbuf = _nbuf;

  thread_buffers = new char*[tnum];
  thread_offsets = new int*[tnum];

#pragma omp parallel
  {
    int tid = omp_get_thread_num();
    thread_buffers[tid] = (char*)mem_aligned_malloc(MEMPAGE_SIZE, size*thread_buf_size);
    thread_offsets[tid]   = (int*)mem_aligned_malloc(MEMPAGE_SIZE, size*sizeof(int));
    for(int i = 0; i < size; i++) thread_offsets[tid][i] = 0;
  }
 
  send_buffers = new char*[nbuf];
  send_offsets = new int*[nbuf];

  for(int i = 0; i < nbuf; i++){
    send_buffers[i] = (char*)mem_aligned_malloc(MEMPAGE_SIZE, size*send_buf_size);
    send_offsets[i] = (int*)mem_aligned_malloc(MEMPAGE_SIZE, size*sizeof(int));
    for(int j = 0; j < size; j++) send_offsets[i][j] = 0;
  }

  for(int i = 0; i < tnum; i++){
    if(!thread_buffers[i]){
      LOG_ERROR("%s", "Error: communication buffer is overflow!\n");
    }
  }

  for(int i = 0; i < nbuf; i++){
    if(!send_buffers[i]){
      LOG_ERROR("%s", "Error: communication buffer is overflow!\n");
    }
  }

  return 0;
}

void Communicator::init(DataObject *_data){
  medone = tdone = pdone = 0;
  data = _data; 

  for(int i = 0; i < tnum; i++) blocks[i] = -1;
}


