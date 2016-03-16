#include <stdio.h>
#include <stdlib.h>
#include <omp.h>

#include "log.h"
#include "config.h"
#include "alltoall.h"

#include "const.h"

using namespace MAPREDUCE_NS;

#if GATHER_STAT
#include "stat.h"
#endif

#if 0
#define SAVE_ALL_DATA(ii) \
{\
  int offset=0;\
  for(int k=0;k<size;k++){\
    SAVE_DATA(recv_buf[ii]+offset, recv_count[ii][k])\
    offset += recv_count[ii][k];\
  }\
}
#endif

#define SAVE_ALL_DATA(ii)\
{\
  for(int k=0;k<size;k++){\
    int recvcount = recv_count[ii][k];\
    printf("recvcount=%d\n", recvcount);\
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
      printf("i=%d, dataoff=%d, comm_recv_displs=%d, comm_recv_count=%d\n", i, dataoff, comm_recv_displs[i][k], comm_recv_count[i][k]);\
      memcpy(databuf+datasize+dataoff, comm_recv_buf[i]+comm_recv_displs[i][k], comm_recv_count[i][k]);\
      dataoff+=comm_recv_count[i][k];\
    }\
    data->setblockdatasize(blocks[0], datasize+recvcount);\
  }\
}

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
  send_displs = recv_displs = NULL;

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

  for(int i = 0; i< comm_div_count; i++){
    if(comm_recv_count && comm_recv_count[i]) free(comm_recv_count[i]);
    if(comm_recv_displs && comm_recv_displs[i]) free(comm_recv_displs[i]);
  }
  delete [] comm_recv_count;
  delete [] comm_recv_displs;
  delete [] comm_recv_buf;

  if(recv_count) delete [] recv_count;
  if(recv_buf) delete [] recv_buf;

  if(send_displs) delete [] send_displs;
  if(recv_displs) delete [] recv_displs;
  
  if(recvcounts) delete [] recvcounts;

  if(reqs) {
    for(int i=0; i<nbuf; i++) delete [] reqs[i];
    delete [] reqs;
  }

#if GATHER_STAT
  //delete [] tsendkv; 
  //delete [] thwait;
#endif

  LOG_PRINT(DBG_COMM, "%d[%d] Comm: alltoall destroy.\n", rank, size);
}

/* setup communicator 
 *   lbufsize: local buffer size
 *   send_buf_size: global buffer size
 *   nbuf: pipeline buffer count
 */
int Alltoall::setup(int _tbufsize, int _sbufsize, int _kvtype, int _ksize, int _vsize, int _nbuf){

  Communicator::setup(_tbufsize, _sbufsize, _kvtype, _ksize, _vsize, _nbuf);

  comm_max_size=MAX_COMM_SIZE*UNIT_1M_SIZE;
  comm_unit_size=comm_max_size/size;
  comm_div_count=send_buf_size/comm_unit_size;  
  if(comm_div_count<=0) comm_div_count=1;

  //printf("comm: max size=%ld, unit size=%d, div count=%d\n", comm_max_size, comm_unit_size, comm_div_count); fflush(stdout);

  recv_buf = new char*[nbuf];
  recv_count  = new int*[nbuf];

  for(int i = 0; i < nbuf; i++){
    recv_buf[i] = (char*)malloc(size*send_buf_size);
    recv_count[i] = (int*)malloc(size*sizeof(int));
  }

  comm_recv_buf = new char*[comm_div_count];
  comm_recv_count = new int*[comm_div_count];
  comm_recv_displs = new int*[comm_div_count];
  for(int i=0; i<comm_div_count; i++){
    comm_recv_count[i] = (int*)malloc(size*sizeof(int));
    comm_recv_displs[i] = (int*)malloc(size*sizeof(int));
  }

  send_displs = new int[size];
  recv_displs = new int[size];
 
  reqs = new MPI_Request*[nbuf];
  for(int i=0; i<nbuf; i++){
    reqs[i]=new MPI_Request[comm_div_count];
  }
  for(int i = 0; i < nbuf; i++)
    for(int j = 0; j < comm_div_count; j++)
      reqs[i][j] = MPI_REQUEST_NULL;

  recvcounts = new int[nbuf];
  for(int i = 0; i < nbuf; i++){
    recvcounts[i] = 0;
  }

  init(NULL);

  LOG_PRINT(DBG_COMM, "%d[%d] Comm: alltoall setup. (local bufffer size=%d, global buffer size=%d)\n", rank, size, thread_buf_size, send_buf_size);

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

  //printf("send KV: %s\n", key);

  int kvsize = 0;
  GET_KV_SIZE(kvtype, keysize, valsize, kvsize);

#if SAFE_CHECK
  if(kvsize > thread_buf_size){
    LOG_ERROR("Error: send KV size is larger than local buffer size. (KV size=%d, local buffer size=%d)\n", kvsize, thread_buf_size);
  }
#endif

#if GATHER_STAT
  //double t1 = omp_get_wtime();      
#endif

  //char *lbuf = local_buffers[tid]+target*lbufsize;

  /* copy kv into local buffer */
  while(1){
    // need communication
    if(switchflag != 0){
#if GATHER_STAT
      double t1 = omp_get_wtime();
#endif
#pragma omp barrier
#if GATHER_STAT
      double t2 = omp_get_wtime();
      st.inc_timer(tid, TIMER_MAP_COMMSYN, t2-t1);
#endif      
      int flag;
      MPI_Is_thread_main(&flag);
      //printf("rank=%d, tid=%d, sendkv\n", rank, tid); fflush(stdout);
      if(flag){
        exchange_kv();
        switchflag = 0;
      }
#pragma omp barrier
#if GATHER_STAT
      double t3 = omp_get_wtime();
      st.inc_timer(tid, TIMER_MAP_COMM, t3-t1);
#endif
    }

    int loff = thread_offsets[tid][target];
    char *lbuf = thread_buffers[tid]+target*thread_buf_size+loff;

    //printf("loff=%d, kvsize=%d, thread_buf_size=%d\n", loff, kvsize, thread_buf_size); fflush(stdout);

    // local buffer has space
    if(loff + kvsize <= thread_buf_size){
      PUT_KV_VARS(kvtype,lbuf,key,keysize,val,valsize,kvsize);
      thread_offsets[tid][target]+=kvsize;
      break;
    // local buffer is full
    }else{
       // try to add the offset
      if(loff + off[target] <= send_buf_size){

#if GATHER_STAT
       double tstart = omp_get_wtime();
#endif

        int goff=fetch_and_add_with_max(&off[target], loff, send_buf_size);

#if GATHER_STAT
       double tstop = omp_get_wtime();
       st.inc_timer(tid, TIMER_MAP_LOCK, tstop-tstart);
#endif

        if(goff + loff <= send_buf_size){
          memcpy(buf+target*send_buf_size+goff, thread_buffers[tid]+target*thread_buf_size, loff);
          thread_offsets[tid][target] = 0;
        // global buffer is full, add back the offset
        }else{
          /* need wait flush */
#pragma omp atomic
          switchflag++;
        }
      /* need wait flush */
      }else{
#pragma omp atomic
        switchflag++;
      }
    }
  }

  //inc_counter(target); 

  return 0;
}


void Alltoall::tpoll(int tid){
#pragma omp atomic
  tdone++;

  // wait other threads
  do{
    if(switchflag != 0){
#if GATHER_STAT
      double t1 = omp_get_wtime();
#endif
#pragma omp barrier
#if GATHER_STAT
      double t2 = omp_get_wtime();
      st.inc_timer(tid, TIMER_MAP_COMMSYN, t2-t1);
#endif
      int flag;
      MPI_Is_thread_main(&flag);
      if(flag){
        exchange_kv();
        switchflag=0;
      }
#pragma omp barrier
#if GATHER_STAT
      double t3 = omp_get_wtime();
      st.inc_timer(tid, TIMER_MAP_COMM, t3-t1);
#endif
    }
  }while(tdone < tnum);

#pragma omp barrier
  if(tid==0){
    tdone=0;
  }
#pragma omp barrier
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

  LOG_PRINT(DBG_COMM, "%d[%d] Comm: thread %d begin wait.\n", rank, size, tid);

#if GATHER_STAT
  //double t1 = MPI_Wtime();
#endif

  // flush local buffer
  int i =0;

  // flush all buffers
  while(i<size){
    
    // check communication
    if(switchflag != 0){
#if GATHER_STAT
      double t1 = omp_get_wtime();
#endif
#pragma omp barrier
#if GATHER_STAT
      double t2 = omp_get_wtime();
      st.inc_timer(tid, TIMER_MAP_COMMSYN, t2-t1);
#endif
      int flag;
      MPI_Is_thread_main(&flag);
      //printf("rank=%d, tid=%d, twait first\n", rank, tid); fflush(stdout);
      if(flag){
        exchange_kv();
        switchflag=0;
      }
#pragma omp barrier
#if GATHER_STAT
      double t3 = omp_get_wtime();
      st.inc_timer(tid, TIMER_MAP_COMM, t3-t1);
#endif
    }
    
    int   loff = thread_offsets[tid][i];
    // skip empty buffer
    if(loff == 0){
      i++;
      continue;
    }

#if GATHER_STAT
    double tstart = omp_get_wtime();
#endif

    // try to flush local buffer into global bufer
    char *lbuf = thread_buffers[tid]+i*thread_buf_size;
    int goff=fetch_and_add_with_max(&off[i], loff, send_buf_size);

#if GATHER_STAT
    double tstop = omp_get_wtime();
    st.inc_timer(tid, TIMER_MAP_LOCK, tstop-tstart);
#endif

     // copy data to global buffer
     if(goff+loff<=send_buf_size){
       memcpy(buf+i*send_buf_size+goff, lbuf, loff);
       thread_offsets[tid][i] = 0;
       i++;
       continue;
      // need flush global buffer firstly
     }else{
#pragma omp atomic
       switchflag++;
     }
  } // end i <size
 
  // add tdone counter
#pragma omp atomic
  tdone++;

  // wait other threads
  do{
    if(switchflag != 0){
#if GATHER_STAT
      double t1 = omp_get_wtime();
#endif
#pragma omp barrier
#if GATHER_STAT
      double t2 = omp_get_wtime();
      st.inc_timer(tid, TIMER_MAP_COMMSYN, t2-t1);
#endif
      int flag;
      MPI_Is_thread_main(&flag);
      if(flag){
        exchange_kv();
        switchflag=0;
      }
#pragma omp barrier
#if GATHER_STAT
      double t3 = omp_get_wtime();
      st.inc_timer(tid, TIMER_MAP_COMM, t3-t1);
#endif
    }
  }while(tdone < tnum);

#if GATHER_STAT
  //double t2 = omp_get_wtime();
  //st.inc_timer(thwait[tid], t2-t1);    
#endif

  LOG_PRINT(DBG_COMM, "%d[%d] Comm: thread %d finish wait.\n", rank, size, tid);
}

// wait all procsses done
void Alltoall::wait(){
   LOG_PRINT(DBG_COMM, "%d[%d] Comm: start wait.\n", rank, size);

#if GATHER_STAT
  //double tstart = MPI_Wtime();
#endif

   medone = 1;

   // do exchange kv until all processes done
   do{
#if GATHER_STAT
     double t1 = MPI_Wtime();
#endif

     //printf("rank=%d, wait\n", rank); fflush(stdout);

     exchange_kv();

#if GATHER_STAT
    double t2 = MPI_Wtime();
    st.inc_timer(0, TIMER_MAP_LASTEXCH, t2-t1);
#endif
   }while(pdone < size);

   // wait all pending communication
   for(int i = 0; i < nbuf; i++){
     if(reqs[i][0] != MPI_REQUEST_NULL){
       
       for(int j=0; j<comm_div_count; j++){
         MPI_Status mpi_st;
         MPI_Wait(&reqs[i][j], &mpi_st);
         reqs[i][j] = MPI_REQUEST_NULL;
       }

       int recvcount = recvcounts[i];

       LOG_PRINT(DBG_COMM, "%d[%d] Comm: receive data. (count=%d)\n", rank, size, recvcount);      
       if(recvcount > 0) {
#if GATHER_STAT
         st.inc_counter(0, COUNTER_RECV_BYTES, recvcount);
#endif    
         
         SAVE_ALL_DATA(i);
         //save_data(i);
       }
     }
   }

#if GATHER_STAT
  //double tstop = MPI_Wtime();
  //st.inc_timer(pwait, tstop-tstart);
#endif

   LOG_PRINT(DBG_COMM, "%d[%d] Comm: finish wait.\n", rank, size);
}

void Alltoall::exchange_kv(){

#if GATHER_STAT
  double t1 = omp_get_wtime();
#endif

  int i;
  int sendcount=0;
  for(i=0; i<size; i++) sendcount += off[i];

  // exchange send count
  MPI_Alltoall(off, 1, MPI_INT, recv_count[ibuf], 1, MPI_INT, comm);

  for(i = 0; i < size; i++) send_displs[i] = i*send_buf_size;
  recvcounts[ibuf] = recv_count[ibuf][0];
  recv_displs[0] = 0;
  for(i = 1; i < size; i++){
    recv_displs[i] = recv_count[ibuf][i-1] + recv_displs[i-1];
    recvcounts[ibuf] += recv_count[ibuf][i];
  }

#if GATHER_STAT
  double t2 = omp_get_wtime();
  st.inc_timer(0, TIMER_MAP_ALLTOALL, t2-t1);
  st.inc_counter(0, COUNTER_SEND_BYTES, sendcount);
#endif

  int origin_ibuf=ibuf;
  // wait data
  ibuf = (ibuf+1)%nbuf;
  if(reqs[ibuf][0] != MPI_REQUEST_NULL) {

#if GATHER_STAT
    double t1 = omp_get_wtime();
#endif
    
    for(i=0; i<comm_div_count; i++){
      MPI_Status mpi_st;
      MPI_Wait(&reqs[ibuf][i], &mpi_st);
      reqs[ibuf][i] = MPI_REQUEST_NULL;
    }

#if GATHER_STAT
    double t2 = omp_get_wtime();
    st.inc_timer(0, TIMER_MAP_WAITDATA, t2-t1);
#endif

    int recvcount = recvcounts[ibuf];
    LOG_PRINT(DBG_COMM, "%d[%d] Comm: receive data. (count=%d)\n", rank, size, recvcount);
    if(recvcount > 0) {
#if GATHER_STAT
      st.inc_counter(0, COUNTER_RECV_BYTES, recvcount);
#endif    
      SAVE_ALL_DATA(ibuf);
    }
#if GATHER_STAT
    double t3 = omp_get_wtime();
    st.inc_timer(0, TIMER_MAP_COPYDATA, t3-t2);
#endif
  }

#if GATHER_STAT
  double t3 = omp_get_wtime();
  st.inc_timer(0, TIMER_MAP_SAVEDATA, t3-t2);
#endif

  //printf("send count!\n", )
  char *a2a_s_buf;
  if(comm_div_count==1) a2a_s_buf=send_buffers[origin_ibuf];
  else a2a_s_buf = send_buffers[ibuf];
  char *a2a_r_buf = recv_buf[origin_ibuf];
  int *a2a_s_count = new int[size];
  int *a2a_s_remain = new int[size];
  int *a2a_s_displs = new int[size];
  int *a2a_r_count = new int[size];
  int *a2a_r_remain = new int[size];
  int *a2a_r_displs = new int[size];
  for(i=0; i<size; i++){
    a2a_s_count[i] = a2a_r_count[i] = 0;
    a2a_s_remain[i] = off[i];
    a2a_r_remain[i] = recv_count[origin_ibuf][i];
    //recv_count[origin_ibuf][i] = 0;
  }
  // start communication
  for(int k=0; k<comm_div_count; k++){
    int a2a_send_count=0, a2a_recv_count=0;    

    for(i=0; i<size; i++){
      if(a2a_s_remain[i] <= comm_unit_size){
        a2a_s_count[i] = a2a_s_remain[i];
        a2a_s_remain[i] = 0;
      }else{
        a2a_s_count[i] = comm_unit_size;
        a2a_s_remain[i] -= comm_unit_size;
      }
      if(a2a_r_remain[i] <= comm_unit_size){
        a2a_r_count[i] = a2a_r_remain[i];
        a2a_r_remain[i] = 0;
      }else{
        a2a_r_count[i] = comm_unit_size;
        a2a_r_remain[i] -= comm_unit_size;
      }
      a2a_send_count+=a2a_s_count[i];
      a2a_recv_count+=a2a_r_count[i];
    }

    a2a_s_displs[0]=0;
    a2a_r_displs[0]=0;
    for(i=0; i<size; i++){
      if(comm_div_count > 1) memcpy(a2a_s_buf+a2a_s_displs[i], buf+send_displs[i], a2a_s_count[i]);
      if(i<size-1){
        if(comm_div_count==1) a2a_s_displs[i+1]=send_displs[i+1];
        else a2a_s_displs[i+1]=a2a_s_displs[i]+a2a_s_count[i];
        a2a_r_displs[i+1]=a2a_r_displs[i]+a2a_r_count[i];
      }
    }
    // exchange kv data
    MPI_Ialltoallv(a2a_s_buf, a2a_s_count, a2a_s_displs, MPI_BYTE, a2a_r_buf, a2a_r_count, a2a_r_displs, MPI_BYTE, comm,  &reqs[origin_ibuf][k]);
    comm_recv_buf[k] = a2a_r_buf;
    comm_recv_displs[k][0] = 0;
    for(i=0; i<size; i++){
      send_displs[i] += a2a_s_count[i];
      comm_recv_count[k][i] = a2a_r_count[i];
      if(i<size-1) comm_recv_displs[k][i+1]=comm_recv_displs[k][i]+a2a_r_count[i];
      //recv_count[origin_ibuf][(k*size+i)%comm_div_count] += a2a_r_count[i];
      //recv_displs[i] += a2a_r_count[i];
    }
    //for(i=0; i<comm_div_count)

    a2a_s_buf+=a2a_send_count;
    a2a_r_buf+=a2a_recv_count;
  }

  delete [] a2a_s_displs; 
  delete [] a2a_s_remain;
  delete [] a2a_r_remain;
  delete [] a2a_s_count;
  delete [] a2a_r_count;
  delete [] a2a_r_displs;

  if(comm_div_count > 1){
    char *tmp = send_buffers[ibuf];
    send_buffers[ibuf]=send_buffers[origin_ibuf];
    send_buffers[origin_ibuf]=tmp;
  }

#if GATHER_STAT
  double t4 = omp_get_wtime();
  st.inc_timer(0, TIMER_MAP_IALLTOALL, t4-t3);
#endif

#if 0
  // wait data
  ibuf = (ibuf+1)%nbuf;
  if(reqs[ibuf][0] != MPI_REQUEST_NULL) {

#if GATHER_STAT
    double t1 = omp_get_wtime();
#endif
    
    for(i=0; i<comm_div_count; i++){
      MPI_Status mpi_st;
      MPI_Wait(&reqs[ibuf][i], &mpi_st);
      reqs[ibuf][i] = MPI_REQUEST_NULL;
    }

#if GATHER_STAT
    double t2 = omp_get_wtime();
    st.inc_timer(0, TIMER_MAP_WAITDATA, t2-t1);
#endif

    int recvcount = recvcounts[ibuf];

    LOG_PRINT(DBG_COMM, "%d[%d] Comm: receive data. (count=%d)\n", rank, size, recvcount);
    if(recvcount > 0) {
#if GATHER_STAT
      st.inc_counter(0, COUNTER_RECV_BYTES, recvcount);
#endif    
      SAVE_ALL_DATA(ibuf);
    }
#if GATHER_STAT
    double t3 = omp_get_wtime();
    st.inc_timer(0, TIMER_MAP_COPYDATA, t3-t2);
#endif
  }

#if GATHER_STAT
  double t4 = omp_get_wtime();
  st.inc_timer(0, TIMER_MAP_SAVEDATA, t4-t3);
#endif
#endif

  // switch buffer
  buf = send_buffers[ibuf];
  off = send_offsets[ibuf];
  for(int i = 0; i < size; i++) off[i] = 0;

  //printf("MPI_Allreduce begin\n"); fflush(stdout);

  //printf("me=%d, medone=%d, pdone=%d\n", rank, medone, pdone); fflush(stdout);

  MPI_Allreduce(&medone, &pdone, 1, MPI_INT, MPI_SUM, comm);

  //printf("Exchange KV end\n"); fflush(stdout);

#if GATHER_STAT
  double t5 = omp_get_wtime();
  st.inc_timer(0, TIMER_MAP_ALLREDUCE, t5-t4);
  st.inc_timer(0, TIMER_MAP_EXCHANGE, t5-t1);
#endif

  LOG_PRINT(DBG_COMM, "%d[%d] Comm: exchange KV. (send count=%d, done count=%d)\n", rank, size, sendcount, pdone);
}

#if 0
void Alltoall::save_data(int i){
  if(blocks[0] == -1){
    blocks[0] = data->add_block();
  }

  data->acquire_block(blocks[0]);

  int offset=0;
  for(int k = 0; k < size; k++){
     if(recv_count[i][k] == 0) continue;

     recv_bytes += recv_count[i][k];

     while(data->adddata(blocks[0], recv_buf[i]+offset, recv_count[i][k]) == -1){
       data->release_block(blocks[0]);
       blocks[0] = data->add_block();
       data->acquire_block(blocks[0]);
     }

     offset += recv_count[i][k];
  }

  data->release_block(blocks[0]);
}
#endif
