#include <stdio.h>
#include <stdlib.h>
#include <omp.h>
#include "communicator.h"
#include "log.h"
#include "config.h"

using namespace MAPREDUCE_NS;

Communicator::Communicator(MPI_Comm _comm, int _commtype, int _tnum){

  comm = _comm;
  commtype = _commtype;
  tnum = _tnum;
  
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &size);

  kvtype = ksize = vsize = 0;

  lbufsize = gbufsize = nbuf = 0;

  local_buffers = NULL;
  local_offsets = NULL;
  global_buffers = NULL;
  global_offsets = NULL;

  blocks = new int[tnum];

  init();
}

Communicator::~Communicator(){

  delete [] blocks;

  for(int i = 0; i < tnum; i++){
    //printf("free: buffers[%d]=%p\n", i, local_buffers[i]);
    if(local_buffers && local_buffers[i]) free(local_buffers[i]);
    if(local_offsets && local_offsets[i]) free(local_offsets[i]);
  }

  for(int i = 0; i < nbuf; i++){
    if(global_buffers && global_buffers[i]) free(global_buffers[i]);
    if(global_offsets && global_offsets[i]) free(global_offsets[i]);
  }

  if(local_buffers) delete [] local_buffers;
  if(local_offsets) delete [] local_offsets;

  if(global_buffers) delete [] global_buffers;
  if(global_offsets) delete [] global_offsets;

}

int Communicator::setup(int _lbufsize, int _gbufsize, int _kvtype, int _ksize, int _vsize, int _nbuf){
  lbufsize = _lbufsize*UNIT_SIZE;
  gbufsize = _gbufsize*UNIT_SIZE;
  kvtype = _kvtype;
  ksize = _ksize;
  vsize = _vsize;
  nbuf = _nbuf;

  local_buffers = new char*[tnum];
  local_offsets = new int*[tnum];

#pragma omp parallel
  {
    int tid = omp_get_thread_num();
    local_buffers[tid] = (char*)malloc(size*lbufsize);
    local_offsets[tid]   = (int*)malloc(size*sizeof(int));
    for(int i = 0; i < size; i++) local_offsets[tid][i] = 0;
  }
 
  global_buffers = new char*[nbuf];
  global_offsets = new int*[nbuf];

  for(int i = 0; i < nbuf; i++){
    global_buffers[i] = (char*)malloc(size*gbufsize);
    global_offsets[i] = (int*)malloc(size*sizeof(int));
    for(int j = 0; j < size; j++) global_offsets[i][j] = 0;
  }

  for(int i = 0; i < tnum; i++){
    if(!local_buffers[i]){
      LOG_ERROR("%s", "Error: communication buffer is overflow!\n");
    }
  }

  for(int i = 0; i < nbuf; i++){
    if(!global_buffers[i]){
      LOG_ERROR("%s", "Error: communication buffer is overflow!\n");
    }
  }

  mem_bytes += tnum*size*lbufsize;
  mem_bytes += nbuf*size*gbufsize;
  return 0;
}

void Communicator::init(DataObject *_data){
  medone = tdone = pdone = 0;
  data = _data; 

  for(int i = 0; i < tnum; i++) blocks[i] = -1;

  send_bytes = recv_bytes = 0;
  mem_bytes = 0;
}
