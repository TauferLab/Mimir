#include <stdio.h>
#include <stdlib.h>
#include <omp.h>

#include "log.h"
#include "config.h"
#include "alltoall.h"

#include "const.h"

#include "memory.h"

using namespace MAPREDUCE_NS;

#include "stat.h"

#if 1
#define SAVE_ALL_DATA(ii) \
{\
  int offset=0;\
  for(int k=0;k<size;k++){\
    SAVE_DATA(recv_buf[ii]+offset, recv_count[ii][k])\
    offset+=(recv_count[ii][k]+one_type_bytes-1)/one_type_bytes*one_type_bytes;\
  }\
}
#endif

#if 0
#define SAVE_ALL_DATA(ii)\
{\
  for(int k=0;k<size;k++){\
    int recvcount = recv_count[ii][k];\
    if(blocks[0]==-1){\
      blocks[0] = data->add_block();\
      data->acquire_block(blocks[0]);\
    }\
    int datasize=data->getdatasize(blocks[0]);\
    if(datasize+recvcount>data->blocksize){\
      data->release_block(blocks[0]);\
      blocks[0] = data->add_block();\
      data->acquire_block(blocks[0]);\
      datasize=0;\
    }\
    char *databuf = data->getblockbuffer(blocks[0]);\
    int dataoff=0;\
    for(int i=0; i<comm_div_count; i++){\
        memcpy(databuf+datasize+dataoff, comm_recv_buf[i]+comm_recv_displs[i][k], comm_recv_count[i][k]);\
      dataoff+=comm_recv_count[i][k];\
    }\
    data->setblockdatasize(blocks[0], datasize+recvcount);\
  }\
}
#endif

Alltoall::Alltoall(MPI_Comm _comm, int _tnum):Communicator(_comm, 0, _tnum){
  int provided;
  MPI_Query_thread(&provided);
  if(provided < MPI_THREAD_FUNNELED){
    LOG_ERROR("%s", "Error: MPI_THREAD_FUNNELED mode should be supported!\n");
  }

  switchflag = 0;

  ibuf = 0;
  buf = NULL;
  off = NULL;

  recv_count = NULL;
  //send_displs = recv_displs = NULL;

  recv_buf = NULL;
  recvcounts = NULL;
  
  reqs = NULL;

  LOG_PRINT(DBG_COMM, "%d[%d] Comm: alltoall create.\n", rank, size);
}

Alltoall::~Alltoall(){
  for(int i = 0; i < nbuf; i++){
    if(recv_buf && recv_buf[i]) free(recv_buf[i]);
    if(recv_count && recv_count[i]) free(recv_count[i]);
  }

  //for(int i = 0; i< comm_div_count; i++){
  //  if(comm_recv_count && comm_recv_count[i]) free(comm_recv_count[i]);
  //  if(comm_recv_displs && comm_recv_displs[i]) free(comm_recv_displs[i]);
  //}
  //delete [] comm_recv_count;
  //delete [] comm_recv_displs;
  //delete [] comm_recv_buf;

  if(recv_count) delete [] recv_count;
  if(recv_buf) delete [] recv_buf;

  //if(send_displs) delete [] send_displs;
  //if(recv_displs) delete [] recv_displs;
  
  if(recvcounts) delete [] recvcounts;

  if(reqs) {
    //for(int i=0; i<nbuf; i++) delete [] reqs[i];
    delete [] reqs;
  }

  LOG_PRINT(DBG_COMM, "%d[%d] Comm: alltoall destroy.\n", rank, size);
}

/* setup communicator 
 *   lbufsize: local buffer size
 *   send_buf_size: global buffer size
 *   nbuf: pipeline buffer count
 */
int Alltoall::setup(int64_t _tbufsize, int64_t _sbufsize, int _kvtype, int _ksize, int _vsize, int _nbuf){

  Communicator::setup(_tbufsize, _sbufsize, _kvtype, _ksize, _vsize, _nbuf);

  one_type_bytes=1;
  size_t total_send_buf_size=(size_t)send_buf_size*size;

  while((int64_t)one_type_bytes*MAX_COMM_SIZE<total_send_buf_size)
    one_type_bytes<<=1;

  //printf("one_type_bytes=%d\n", one_type_bytes);

  //one_type_bytes=8;
  //comm_max_size=MAX_COMM_SIZE;
  //comm_unit_size=comm_max_size/size;
  //comm_div_count=send_buf_size/comm_unit_size;  
  //if(comm_div_count<=0) comm_div_count=1;

  recv_buf = new char*[nbuf];
  recv_count  = new int*[nbuf];

  for(int i = 0; i < nbuf; i++){
    recv_buf[i] = (char*)mem_aligned_malloc(MEMPAGE_SIZE, total_send_buf_size);
    recv_count[i] = (int*)mem_aligned_malloc(MEMPAGE_SIZE, size*sizeof(int));
  }

  PROFILER_RECORD_COUNT(0, COUNTER_COMM_RECV_BUF, total_send_buf_size*nbuf);

  //comm_recv_buf = new char*[comm_div_count];
  //comm_recv_count = new int*[comm_div_count];
  //comm_recv_displs = new int*[comm_div_count];
  //for(int i=0; i<comm_div_count; i++){
  //  comm_recv_count[i] = (int*)mem_aligned_malloc(MEMPAGE_SIZE, size*sizeof(int));
  //  comm_recv_displs[i] = (int*)mem_aligned_malloc(MEMPAGE_SIZE, size*sizeof(int));
  //}

  //send_displs = new uint64_t[size];
  //recv_displs = new uint64_t[size];
 
  reqs = new MPI_Request[nbuf];
  for(int i=0; i<nbuf; i++)
    reqs[i]=MPI_REQUEST_NULL;
  //for(int i=0; i<nbuf; i++){
  //  reqs[i]=new MPI_Request[comm_div_count];
  //}
  //for(int i = 0; i < nbuf; i++)
  //  for(int j = 0; j < comm_div_count; j++)
  //    reqs[i][j] = MPI_REQUEST_NULL;

  recvcounts = new uint64_t[nbuf];
  for(int i = 0; i < nbuf; i++){
    recvcounts[i] = 0;
  }

  //init(NULL);

  LOG_PRINT(DBG_COMM, "%d[%d] Comm: alltoall setup. (local bufffer size=%ld, global buffer size=%ld)\n", rank, size, thread_buf_size, send_buf_size);

  return 0;
}

void Alltoall::init(DataObject *_data){
  Communicator::init(_data);

  switchflag=0;
  ibuf = 0;
  buf = send_buffers[0];
  off = send_offsets[0];

  for(int i=0; i<size; i++) off[i] = 0;
}

/* send KV
 *   tid:     thread id
 *   target:  target process id
 *   key:     key buffer
 *   keysize: key size
 *   val:     value buffer
 *   valsize: value size
 */
int Alltoall::sendKV(int tid, int target, char *key, int keysize, char *val, int valsize){
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

#if SAFE_CHECK
  if(kvsize > thread_buf_size){
    LOG_ERROR("Error: send KV size is larger than local buffer size. (KV size=%d, local buffer size=%ld)\n", kvsize, thread_buf_size);
  }
#endif

  /* copy kv into local buffer */
  while(1){
    // need communication
    if(switchflag != 0){
#ifdef MTMR_MULTITHREAD 
      TRACKER_RECORD_EVENT(tid, EVENT_MAP_COMPUTING);
#pragma omp barrier  
      TRACKER_RECORD_EVENT(tid, EVENT_OMP_BARRIER);
      int flag;
      MPI_Is_thread_main(&flag);
      //printf("rank=%d, tid=%d, sendkv\n", rank, tid); fflush(stdout);
      if(flag){
#endif
        exchange_kv();
        switchflag = 0;
#ifdef MTMR_MULTITHREAD 
      }
#pragma omp barrier
      TRACKER_RECORD_EVENT(tid, EVENT_OMP_BARRIER);
#endif
    }

#ifdef MTMR_MULTITHREAD 
    int loff = thread_offsets[tid][target];
    char *lbuf = thread_buffers[tid]+target*thread_buf_size+loff;

    // local buffer has space
    if(loff + kvsize <= thread_buf_size){
      PUT_KV_VARS(kvtype,lbuf,key,keysize,val,valsize,kvsize);
      thread_offsets[tid][target]+=kvsize;
      break;
    // local buffer is full
    }else{
       // try to add the offset
      if(loff + off[target] <= send_buf_size){
         PROFILER_RECORD_TIME_START;
        int goff=fetch_and_add_with_max(&off[target], loff, send_buf_size);
         PROFILER_RECORD_TIME_END(tid, TIMER_MAP_FOP);

        if(goff + loff <= send_buf_size){
          size_t global_buf_off=target*(size_t)send_buf_size+goff;
          memcpy(buf+global_buf_off, thread_buffers[tid]+target*thread_buf_size, loff);
          thread_offsets[tid][target] = 0;
        // global buffer is full, add back the offset
        }else{
          /* need wait flush */
          PROFILER_RECORD_TIME_START;
#pragma omp atomic
          switchflag++;
          PROFILER_RECORD_TIME_END(tid, TIMER_MAP_ATOMIC);
        }
      /* need wait flush */
      }else{
        PROFILER_RECORD_TIME_START;
#pragma omp atomic
        switchflag++;
        PROFILER_RECORD_TIME_END(tid, TIMER_MAP_ATOMIC);
      }
    }
#else
    //printf("sendKV to %d\n", target); fflush(stdout);
    int goff=off[target];
    if(goff+kvsize<=send_buf_size){
      size_t global_buf_off=target*(size_t)send_buf_size+goff; 
      char *gbuf=buf+global_buf_off;
      PUT_KV_VARS(kvtype,gbuf,key,keysize,val,valsize,kvsize);
      off[target]+=kvsize;
      break;
    }else{
      switchflag=1;
    }
#endif
  }

  return 0;
}

void Alltoall::tpoll(int tid){
#ifdef MTMR_MULTITHREAD 
  PROFILER_RECORD_TIME_START;
#pragma omp atomic
  tdone++;
  PROFILER_RECORD_TIME_END(tid, TIMER_MAP_ATOMIC);

  // wait other threads
  do{
    if(switchflag != 0){
      TRACKER_RECORD_EVENT(tid, EVENT_MAP_COMPUTING);
#pragma omp barrier
      TRACKER_RECORD_EVENT(tid, EVENT_OMP_BARRIER);
      int flag;
      MPI_Is_thread_main(&flag);
      if(flag){
        exchange_kv();
        switchflag=0;
      }
#pragma omp barrier
      TRACKER_RECORD_EVENT(tid, EVENT_OMP_BARRIER);
    }
  }while(tdone < tnum);

  TRACKER_RECORD_EVENT(tid, EVENT_MAP_COMPUTING);

#pragma omp barrier
  if(tid==0){
    tdone=0;
  }
#pragma omp barrier

  TRACKER_RECORD_EVENT(tid, EVENT_OMP_BARRIER);
#endif
}

/* send KV
 *   tid:     thread id
 *   target:  target process id
 *   key:     key buffer
 *   keysize: key size
 *   val:     value buffer
 *   valsize: value size
 */
void Alltoall::twait(int tid){
#ifdef MTMR_MULTITHREAD 
  LOG_PRINT(DBG_COMM, "%d[%d] Comm: thread %d begin wait.\n", rank, size, tid);

  // flush local buffer
  int i =0;

  // flush all buffers
  while(i<size){
    
    // check communication
    if(switchflag != 0){
      TRACKER_RECORD_EVENT(tid, EVENT_MAP_COMPUTING);
#pragma omp barrier
      TRACKER_RECORD_EVENT(tid, EVENT_OMP_BARRIER);
      int flag;
      MPI_Is_thread_main(&flag);
      if(flag){
        exchange_kv();
        switchflag=0;
      }
#pragma omp barrier
      TRACKER_RECORD_EVENT(tid, EVENT_OMP_BARRIER);
    }
    
    int   loff = thread_offsets[tid][i];
    // skip empty buffer
    if(loff == 0){
      i++;
      continue;
    }

    // try to flush local buffer into global bufer
    char *lbuf = thread_buffers[tid]+i*thread_buf_size;
    PROFILER_RECORD_TIME_START;
    int goff=fetch_and_add_with_max(&off[i], loff, send_buf_size);
    PROFILER_RECORD_TIME_END(tid, TIMER_MAP_FOP);

     // copy data to global buffer
     if(goff+loff<=send_buf_size){
       size_t global_buf_off=i*(size_t)send_buf_size+goff;
       memcpy(buf+global_buf_off, lbuf, loff);
       thread_offsets[tid][i] = 0;
       i++;
       continue;
      // need flush global buffer firstly
     }else{
       PROFILER_RECORD_TIME_START;
#pragma omp atomic
       switchflag++;
       PROFILER_RECORD_TIME_END(tid, TIMER_MAP_ATOMIC);
     }
  } // end i <size

  PROFILER_RECORD_TIME_START;
  // add tdone counter
#pragma omp atomic
  tdone++;
  PROFILER_RECORD_TIME_END(tid, TIMER_MAP_ATOMIC);
  // wait other threads
  do{
    if(switchflag != 0){
      TRACKER_RECORD_EVENT(tid, EVENT_MAP_COMPUTING);
#pragma omp barrier
      TRACKER_RECORD_EVENT(tid, EVENT_OMP_BARRIER);
      int flag;
      MPI_Is_thread_main(&flag);
      if(flag){
        exchange_kv();
        switchflag=0;
      }
#pragma omp barrier
      TRACKER_RECORD_EVENT(tid, EVENT_OMP_BARRIER);
    }
  }while(tdone < tnum);


  LOG_PRINT(DBG_COMM, "%d[%d] Comm: thread %d finish wait.\n", rank, size, tid);
#endif
}

// wait all procsses done
void Alltoall::wait(){
   LOG_PRINT(DBG_COMM, "%d[%d] Comm: start wait.\n", rank, size);

   medone = 1;

   // do exchange kv until all processes done
   do{
     exchange_kv();
   }while(pdone < size);

#ifndef MTMR_COMM_BLOCKING
   // wait all pending communication
   for(int i = 0; i < nbuf; i++){
     if(reqs[i] != MPI_REQUEST_NULL){
       
       TRACKER_RECORD_EVENT(0, EVENT_MAP_COMPUTING);

       MPI_Status mpi_st;
       MPI_Wait(&reqs[i], &mpi_st);
       reqs[i] = MPI_REQUEST_NULL;

       TRACKER_RECORD_EVENT(0, EVENT_COMM_WAIT);

       uint64_t recvcount = recvcounts[i];

       PROFILER_RECORD_COUNT(0, COUNTER_COMM_RECV_SIZE, recvcount);

       LOG_PRINT(DBG_COMM, "%d[%d] Comm: receive data. (count=%ld)\n", rank, size, recvcount);      
       if(recvcount > 0) {
         SAVE_ALL_DATA(i);
       }
     }
   }
#endif

   LOG_PRINT(DBG_COMM, "%d[%d] Comm: finish wait.\n", rank, size);
}

void Alltoall::exchange_kv(){
  int i;  
  uint64_t sendcount=0;
  for(i=0; i<size; i++) sendcount += (uint64_t)off[i];

  // exchange send count
  TRACKER_RECORD_EVENT(0, EVENT_MAP_COMPUTING);
  PROFILER_RECORD_COUNT(0, COUNTER_COMM_SEND_SIZE, sendcount);

  // exchange send and recv counts
  MPI_Alltoall(off, 1, MPI_INT, recv_count[ibuf], 1, MPI_INT, comm);

  for(i=0; i<size; i++){
    printf("%d send count=%d, recv count=%d\n", i, off[i], recv_count[ibuf][i]);
  }

  TRACKER_RECORD_EVENT(0, EVENT_COMM_ALLTOALL);

  recvcounts[ibuf] = (uint64_t)recv_count[ibuf][0];
  for(i = 1; i < size; i++){
    recvcounts[ibuf] += (uint64_t)recv_count[ibuf][i];
  }

  int *a2a_s_count=new int[size];
  int *a2a_s_displs=new int[size];
  int *a2a_r_count=new int[size];
  int *a2a_r_displs= new int[size];

  for(i=0; i<size; i++){
    a2a_s_count[i]=(off[i]+one_type_bytes-1)/one_type_bytes;
    a2a_r_count[i]=(recv_count[ibuf][i]+one_type_bytes-1)/one_type_bytes;
    a2a_s_displs[i] = (i*send_buf_size)/one_type_bytes;
  }
  a2a_r_displs[0] = 0;
  for(i=1; i<size; i++)
    a2a_r_displs[i]=a2a_r_displs[i-1]+a2a_r_count[i-1];

  uint64_t send_padding_bytes=a2a_s_count[0];
  uint64_t recv_padding_bytes=a2a_r_count[0]; 
  for(i=1;i<size;i++){
    send_padding_bytes+=a2a_s_count[i];
    recv_padding_bytes+=a2a_r_count[i];
  }
  send_padding_bytes*=one_type_bytes;
  recv_padding_bytes*=one_type_bytes;
  send_padding_bytes-=sendcount;
  recv_padding_bytes-=recvcounts[ibuf];

  PROFILER_RECORD_COUNT(0, COUNTER_COMM_SEND_PAD, send_padding_bytes);
  PROFILER_RECORD_COUNT(0, COUNTER_COMM_RECV_PAD, recv_padding_bytes);
 
  MPI_Datatype comm_type;
  MPI_Type_contiguous(one_type_bytes, MPI_BYTE, &comm_type);
  MPI_Type_commit(&comm_type);

#ifdef MTMR_COMM_BLOCKING
  MPI_Alltoallv(send_buffers[ibuf], a2a_s_count, a2a_s_displs, comm_type, \
    recv_buf[ibuf], a2a_r_count, a2a_r_displs, comm_type, comm);
  uint64_t recvcount = recvcounts[ibuf];
  if(recvcount > 0) { 
    SAVE_ALL_DATA(ibuf);
  }
#else 
  MPI_Ialltoallv(send_buffers[ibuf], a2a_s_count, a2a_s_displs, comm_type, \
    recv_buf[ibuf], a2a_r_count, a2a_r_displs, comm_type, comm,  &reqs[ibuf]);

  TRACKER_RECORD_EVENT(0, EVENT_COMM_IALLTOALLV);

  // wait data
  ibuf = (ibuf+1)%nbuf;
  if(reqs[ibuf] != MPI_REQUEST_NULL) {

    MPI_Status mpi_st;
    MPI_Wait(&reqs[ibuf], &mpi_st);
    reqs[ibuf] = MPI_REQUEST_NULL;

    uint64_t recvcount = recvcounts[ibuf];

    TRACKER_RECORD_EVENT(0, EVENT_COMM_WAIT);
    PROFILER_RECORD_COUNT(0, COUNTER_COMM_RECV_SIZE, recvcount);

    //printf("ibuf=%d, recvcount=%d\n", ibuf, recvcount); fflush(stdout);

    LOG_PRINT(DBG_COMM, "%d[%d] Comm: receive data. (count=%ld)\n", rank, size, recvcount);
    if(recvcount > 0) { 
      SAVE_ALL_DATA(ibuf);
    }

    TRACKER_RECORD_EVENT(0, EVENT_MAP_COMPUTING);
  }

  // switch buffer
  buf = send_buffers[ibuf];
  off = send_offsets[ibuf];

#endif  

  for(int i = 0; i < size; i++) off[i] = 0;

  MPI_Type_free(&comm_type);  

  delete [] a2a_s_count;
  delete [] a2a_s_displs;
  delete [] a2a_r_count;
  delete [] a2a_r_displs;


  MPI_Allreduce(&medone, &pdone, 1, MPI_INT, MPI_SUM, comm);

  TRACKER_RECORD_EVENT(0, EVENT_COMM_ALLREDUCE);

  LOG_PRINT(DBG_COMM, "%d[%d] Comm: exchange KV. (send count=%ld, done count=%d)\n", rank, size, sendcount, pdone);
}
