#include <stdio.h>
#include <stdlib.h>
#include <omp.h>

#include "log.h"
#include "config.h"
#include "ptop.h"

#include "const.h"

using namespace MAPREDUCE_NS;

#define CHECK_MPI_REQS \
{\
  int flag;\
  MPI_Status st;\
  MPI_Test(&recv_req, &flag, &st);\
  if(flag){\
    int count;\
    MPI_Get_count(&st, MPI_BYTE, &count);\
    if(count>0) save_data(count);\
    else pdone++;\
    MPI_Irecv(recv_buf, gbufsize, MPI_BYTE, MPI_ANY_SOURCE, 0, comm, &recv_req);\
  }\
  for(int i=0;i<nbuf;i++){\
    for(int j=0;j<size;j++){\
      MPI_Test(&reqs[i][j], &flag, MPI_STATUS_IGNORE);\
      if(flag) reqs[i][j] = MPI_REQUEST_NULL;\
    }\
  }\
}


Ptop::Ptop(MPI_Comm _comm, int _tnum) : Communicator(_comm, 1, _tnum){

  int provided;
  MPI_Query_thread(&provided);
  if(provided < MPI_THREAD_FUNNELED){
    LOG_ERROR("%s", "Error: MPI_THREAD_FUNNELED mode should be supported!\n");
  }

  flags = NULL;
  ibuf = NULL;

  buf = NULL;
  off = NULL;

  lock = NULL;

  reqs = NULL;

  recv_buf = NULL;
  recv_req = MPI_REQUEST_NULL; 
}

Ptop::~Ptop(){
  delete [] flags;
  delete [] ibuf;

  delete [] buf;
  delete [] off;

  for(int i = 0; i < size; i++)
    omp_destroy_lock(&lock[i]);
  delete [] lock;

  for(int i=0; i<nbuf; i++)
      delete [] reqs[i];
  delete [] reqs;

  free(recv_buf);
}

int Ptop::setup(int _lbufsize, int _gbufsize, int _kvtype, int _ksize, int _vsize, int _nbuf){
  Communicator::setup(_lbufsize, _gbufsize, _kvtype, _ksize, _vsize, _nbuf);

  flags = new int[size];
  ibuf  = new int[size];
  buf   = new char*[size];
  off   = new int[size];

  lock  = new omp_lock_t[size];

  reqs = new MPI_Request*[nbuf];
  for(int i=0; i<_nbuf; i++)
    reqs[i] = new MPI_Request[size];

  recv_buf = (char*)malloc(gbufsize);
}

void Ptop::init(DataObject *_data){
  Communicator::init(_data);

  for(int i=0; i < size; i++){

    flags[i] = 0;
    ibuf[i] = 0;

    buf[i] = global_buffers[0]+gbufsize*i;
    off[i] = 0;

    omp_init_lock(&lock[i]);

    for(int j=0; j<nbuf; j++)
      reqs[j][i] = MPI_REQUEST_NULL;
  }

  MPI_Irecv(recv_buf, gbufsize, MPI_BYTE, MPI_ANY_SOURCE, 0, comm, &recv_req);
}

int Ptop::sendKV(int tid, int target, char *key, int keysize, char *val, int valsize){
  //printf("send:\n"); fflush(stdout);

#if SAFE_CHECK
  if(target < 0 || target >= size){
    LOG_ERROR("Error: target process (%d) isn't correct!\n", target);
  }

  if(tid < 0 || tid >= tnum){
    LOG_ERROR("Error: thread num (%d) isn't correct!\n", tid);
  }
#endif

  int kvsize = 0;
  GET_KV_SIZE(kvtype, keysize, valsize, kvsize);

  //printf("sendKV: %s,%s,kvsize=%d\n", key, val, kvsize); fflush(stdout);

#if SAFE_CHECK
  if(kvsize > lbufsize){
    LOG_ERROR("Error: send KV size is larger than local buffer size. (KV size=%d, local buffer size=%d)\n", kvsize, lbufsize);
  }
#endif 

  /* copy kv into local buffer */
  while(1){

    int   loff=local_offsets[tid][target];
    char *lbuf=local_buffers[tid]+target*lbufsize+loff;
    
    // local buffer has space
    if(loff + kvsize <= lbufsize){
     PUT_KV_VARS(kvtype,lbuf,key,keysize,val,valsize,kvsize);
     local_offsets[tid][target]+=kvsize;
     break;
    // local buffer is full
    }else{
      // make sure global buffer is ready
      while(flags[target] != 0){
        int flag;
        MPI_Is_thread_main(&flag);
        if(flag){
          exchange_kv();
        }
      }

      omp_set_lock(&lock[target]);
      // try to add the offset
      if(loff + off[target] <= gbufsize){
        int goff=off[target];
        memcpy(buf[target]+goff, local_buffers[tid]+target*lbufsize, loff);
        off[target]+=loff;
        local_offsets[tid][target] = 0;
      }else{
       flags[target]=1;
      }
      omp_unset_lock(&lock[target]);
    }
  }

  // do communication
  int flag;
  MPI_Is_thread_main(&flag);
  if(flag){
    exchange_kv();
  }

  return 0;
}

void Ptop::twait(int tid){
  LOG_PRINT(DBG_COMM, "%d[%d] thread %d start wait\n", rank, size, tid);

  int i=0;
  while(i<size){

    int loff = local_offsets[tid][i];
    if(loff == 0){
      i++;
      continue;
    }

    //printf("thread %d i=%d begin\n", tid, i); fflush(stdout);

    while(flags[i] != 0){
      int flag;
      MPI_Is_thread_main(&flag);
      if(flag){
        exchange_kv();  
      }
    }

    //printf("thread %d i=%d end\n", tid, i); fflush(stdout);

    int k=i;
    omp_set_lock(&lock[k]);
    int goff=off[i];
    if(goff+loff<=gbufsize){
      char *lbuf = local_buffers[tid]+i*lbufsize;
      memcpy(buf[i]+goff, lbuf, loff);
      local_offsets[tid][i] = 0;
      off[i]+=loff;
      i++;
    }else{
      flags[i]=1;
    }
    omp_unset_lock(&lock[k]);

    //printf("thread %d i=%d copy end\n", tid, i); fflush(stdout);
  }

  //printf("thread %d flush local buffer end\n", tid); fflush(stdout);

#pragma omp atomic
  tdone++;

  do{
    int flag;
    MPI_Is_thread_main(&flag);
    if(flag){
      exchange_kv();  
    }
  }while(tdone < tnum);

}

void Ptop::wait(){
  LOG_PRINT(DBG_COMM, "%d[%d] wait start\n", rank, size);

  for(int i = 0; i < size; i++){
    if(off[i] != 0){
      int ib=ibuf[i];
      //printf("%d send <%d,%d> last count=%d\n", rank, ib, i, off[i]); fflush(stdout);
      MPI_Isend(buf[i], off[i], MPI_BYTE, i, 0, comm, &reqs[ib][i]);
    }
  }

  //printf("%d begin wait data end\n", rank); fflush(stdout);

  for(int i = 0; i < size; i++){
    int ib=ibuf[i];
    while(reqs[ib][i] != MPI_REQUEST_NULL){
      CHECK_MPI_REQS;
    }
    MPI_Isend(NULL, 0, MPI_BYTE, i, 0, comm, &reqs[ib][i]);
  }
 
  do{
    CHECK_MPI_REQS;
  }while(pdone<size);

  for(int i=0; i<nbuf; i++){
    for(int j=0; j<size; j++){
      while(reqs[i][j] != MPI_REQUEST_NULL){
        CHECK_MPI_REQS;
      }
    }
  }

  if(recv_req != MPI_REQUEST_NULL)
    MPI_Cancel(&recv_req);

  LOG_PRINT(DBG_COMM, "%d[%d] wait end\n", rank, size);
}

void Ptop::exchange_kv(){
  int i;
  for(i = 0; i  < size; i++){
    if(flags[i] != 0){
      int ib = ibuf[i];
      //printf("%d send <%d,%d> count=%d\n", rank, ib, i, off[i]); fflush(stdout);
      MPI_Isend(buf[i], off[i], MPI_BYTE, i, 0, comm, &reqs[ib][i]);

      ib=(ib+1)%nbuf;

      while(reqs[ib][i] != MPI_REQUEST_NULL){
        CHECK_MPI_REQS;
      }

      reqs[ib][i] = MPI_REQUEST_NULL;

      omp_set_lock(&lock[i]);
      buf[i]   = global_buffers[ib]+i*gbufsize;
      off[i]   = 0;
      ibuf[i]  = ib;
      flags[i] = 0;
      omp_unset_lock(&lock[i]);
    }
  }
}

void Ptop::save_data(int recv_count){
  if(blocks[0]==-1){
    blocks[0] = data->addblock();
  }

  data->acquireblock(blocks[0]);

  while(data->adddata(blocks[0], recv_buf, recv_count)==-1){
    data->releaseblock(blocks[0]);
    blocks[0] = data->addblock();
    data->acquireblock(blocks[0]);
  }

  data->releaseblock(blocks[0]);
}
