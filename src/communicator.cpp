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

  lbufsize = gbufsize = nbuf = 0;

  local_buffers = NULL;
  local_offsets = NULL;
  global_buffers = NULL;
  global_offsets = NULL;

  blocks = new int[tnum];

  init();

#if GATHER_STAT
  //tcomm = st.init_timer("exchange kv");
  //tsyn  = st.init_timer("syn time");
  //tput  = st.init_timer("put kvs");
#endif
}

Communicator::~Communicator(){

  if(!data){
    for(int i=0; i<tnum; i++)
      if(blocks[i] !=- 1) data->releaseblock(blocks[i]);
  }
  delete [] blocks;

  for(int i = 0; i < tnum; i++){
    //printf("free: buffers[%d]=%p\n", i, local_buffers[i]);
    if(local_buffers && local_buffers[i]) mem_aligned_free(local_buffers[i]);
    if(local_offsets && local_offsets[i]) mem_aligned_free(local_offsets[i]);
  }

  for(int i = 0; i < nbuf; i++){
    if(global_buffers && global_buffers[i]) mem_aligned_free(global_buffers[i]);
    if(global_offsets && global_offsets[i]) mem_aligned_free(global_offsets[i]);
  }

  if(local_buffers) delete [] local_buffers;
  if(local_offsets) delete [] local_offsets;

  if(global_buffers) delete [] global_buffers;
  if(global_offsets) delete [] global_offsets;

  //if(send_kv_counts) delete [] send_kv_counts;
  //send_kv_counts = new uint64_t[size];
}

int Communicator::setup(int _lbufsize, int _gbufsize, int _kvtype, int _ksize, int _vsize, int _nbuf){
  lbufsize = _lbufsize*UNIT_1K_SIZE;
  gbufsize = _gbufsize*UNIT_1M_SIZE;
  kvtype = _kvtype;
  ksize = _ksize;
  vsize = _vsize;
  nbuf = _nbuf;

  local_buffers = new char*[tnum];
  local_offsets = new int*[tnum];

#pragma omp parallel
  {
    int tid = omp_get_thread_num();
    local_buffers[tid] = (char*)mem_aligned_malloc(MEMPAGE_SIZE, size*lbufsize);
    local_offsets[tid]   = (int*)mem_aligned_malloc(MEMPAGE_SIZE, size*sizeof(int));
    for(int i = 0; i < size; i++) local_offsets[tid][i] = 0;
  }
 
  global_buffers = new char*[nbuf];
  global_offsets = new int*[nbuf];

  for(int i = 0; i < nbuf; i++){
    global_buffers[i] = (char*)mem_aligned_malloc(MEMPAGE_SIZE, size*gbufsize);
    global_offsets[i] = (int*)mem_aligned_malloc(MEMPAGE_SIZE, size*sizeof(int));
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

  return 0;
}

void Communicator::init(DataObject *_data){
  medone = tdone = pdone = 0;
  data = _data; 

  for(int i = 0; i < tnum; i++) blocks[i] = -1;

  //send_bytes = recv_bytes = 0;
  //mem_bytes = 0;
}

//uint64_t Communicator::get_recv_KVs(){

//  int *recv_counts=new int[size];
//  for(int i=0; i<size; i++) recv_counts[i]=1;
//  MPI_Reduce_scatter(send_kv_counts, &recv_kv_counts, recv_counts, MPI_UINT64_T, MPI_SUM, comm);
//  delete [] recv_counts;

//  return recv_kv_counts;
//}


