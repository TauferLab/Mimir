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

#if GATHER_STAT
  tcomm = st.init_timer("exchange kv");
  //pwait = st.init_timer("process wait");
  //tsendkv = new int[tnum];
  //thwait = new int[tnum];
  //for(int i=0; i<tnum; i++){
  //  tsendkv[i] = st.init_timer("send KV");
  //}
  //for(int i=0; i<tnum; i++){
  //  thwait[i] = st.init_timer("thread wait");
  //}
#endif

  LOG_PRINT(DBG_COMM, "%d[%d] Comm: alltoall create.\n", rank, size);
}

Alltoall::~Alltoall(){
  for(int i = 0; i < nbuf; i++){
    if(recv_buf && recv_buf[i]) free(recv_buf[i]);
    if(recv_count && recv_count[i]) free(recv_count[i]);
  }

  if(recv_count) delete [] recv_count;
  if(recv_buf) delete [] recv_buf;

  if(send_displs) delete [] send_displs;
  if(recv_displs) delete [] recv_displs;
  
  if(recvcounts) delete [] recvcounts;

  if(reqs) delete [] reqs;

#if GATHER_STAT
  //delete [] tsendkv; 
  //delete [] thwait;
#endif

  LOG_PRINT(DBG_COMM, "%d[%d] Comm: alltoall destroy.\n", rank, size);
}

/* setup communicator 
 *   lbufsize: local buffer size
 *   gbufsize: global buffer size
 *   nbuf: pipeline buffer count
 */
int Alltoall::setup(int _lbufsize, int _gbufsize, int _kvtype, int _ksize, int _vsize, int _nbuf){

  Communicator::setup(_lbufsize, _gbufsize, _kvtype, _ksize, _vsize, _nbuf);

  recv_buf = new char*[nbuf];
  recv_count  = new int*[nbuf];

  for(int i = 0; i < nbuf; i++){
    recv_buf[i] = (char*)malloc(size*gbufsize);
    recv_count[i] = (int*)malloc(size*sizeof(int));
  }

  send_displs = new int[size];
  recv_displs = new int[size];

  reqs = new MPI_Request[nbuf];

  for(int i = 0; i < nbuf; i++)
    reqs[i] = MPI_REQUEST_NULL;

  recvcounts = new int[nbuf];
  for(int i = 0; i < nbuf; i++){
    recvcounts[i] = 0;
  }

  init(NULL);

  LOG_PRINT(DBG_COMM, "%d[%d] Comm: alltoall setup. (local bufffer size=%d, global buffer size=%d)\n", rank, size, lbufsize, gbufsize);

  return 0;
}

void Alltoall::init(DataObject *_data){
  Communicator::init(_data);

  switchflag=0;
  ibuf = 0;
  buf = global_buffers[0];
  off = global_offsets[0];

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

  //printf("send KV: %s, %s\n", key, val);

  int kvsize = 0;
  GET_KV_SIZE(kvtype, keysize, valsize, kvsize);

#if SAFE_CHECK
  if(kvsize > lbufsize){
    LOG_ERROR("Error: send KV size is larger than local buffer size. (KV size=%d, local buffer size=%d)\n", kvsize, lbufsize);
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
#pragma omp barrier
      int flag;
      MPI_Is_thread_main(&flag);
      if(flag){
       exchange_kv();
       switchflag = 0;
      }
#pragma omp barrier
    }

    int loff = local_offsets[tid][target];
    char *lbuf = local_buffers[tid]+target*lbufsize+loff;

    // local buffer has space
    if(loff + kvsize <= lbufsize){
      PUT_KV_VARS(kvtype,lbuf,key,keysize,val,valsize,kvsize);
      local_offsets[tid][target]+=kvsize;
      break;
    // local buffer is full
    }else{
       // try to add the offset
      if(loff + off[target] <= gbufsize){

        int goff=fetch_and_add_with_max(&off[target], loff, gbufsize);
        if(goff + loff <= gbufsize){
          memcpy(buf+target*gbufsize+goff, local_buffers[tid]+target*lbufsize, loff);
          local_offsets[tid][target] = 0;
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

#if GATHER_STAT
  //double t2 = omp_get_wtime();
  //st.inc_timer(tsendkv[tid], t2-t1);    
#endif

  return 0;
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
#pragma omp barrier
      int flag;
      MPI_Is_thread_main(&flag);
      if(flag){
        exchange_kv();
        switchflag=0;
      }
#pragma omp barrier
    }
    
    int   loff = local_offsets[tid][i];
    // skip empty buffer
    if(loff == 0){
      i++;
      continue;
    }

    // try to flush local buffer into global bufer
    char *lbuf = local_buffers[tid]+i*lbufsize;
    int goff=fetch_and_add_with_max(&off[i], loff, gbufsize);

     // copy data to global buffer
     if(goff+loff<=gbufsize){
       memcpy(buf+i*gbufsize+goff, lbuf, loff);
       local_offsets[tid][i] = 0;
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
#pragma omp barrier
      int flag;
      MPI_Is_thread_main(&flag);
      if(flag){
        exchange_kv();
        switchflag=0;
      }
#pragma omp barrier
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
     exchange_kv();
   }while(pdone < size);

   // wait all pending communication
   for(int i = 0; i < nbuf; i++){
     if(reqs[i] != MPI_REQUEST_NULL){
       MPI_Status st;
       MPI_Wait(&reqs[i], &st);
       reqs[i] = MPI_REQUEST_NULL;
       int recvcount = recvcounts[i];

       LOG_PRINT(DBG_COMM, "%d[%d] Comm: receive data. (count=%d)\n", rank, size, recvcount);      
       if(recvcount > 0) save_data(i);
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
  double tstart = MPI_Wtime();
#endif

  int i;
  int sendcount=0;
  for(i=0; i<size; i++) sendcount += off[i];
  // exchange send count
  MPI_Alltoall(off, 1, MPI_INT, recv_count[ibuf], 1, MPI_INT, comm);

  for(i = 0; i < size; i++) send_displs[i] = i*gbufsize;

  recvcounts[ibuf] = recv_count[ibuf][0];
  recv_displs[0] = 0;
  for(i = 1; i < size; i++){
    recv_displs[i] = recv_count[ibuf][i-1] + recv_displs[i-1];
    recvcounts[ibuf] += recv_count[ibuf][i];
  }

  // exchange kv data
  MPI_Ialltoallv(buf, off, send_displs, MPI_BYTE, recv_buf[ibuf], recv_count[ibuf], recv_displs,MPI_BYTE, comm,  &reqs[ibuf]);

  // wait data
  ibuf = (ibuf+1)%nbuf;
  if(reqs[ibuf] != MPI_REQUEST_NULL) {
    MPI_Status st;
    MPI_Wait(&reqs[ibuf], &st);
    reqs[ibuf] = MPI_REQUEST_NULL;
    int recvcount = recvcounts[ibuf];

    LOG_PRINT(DBG_COMM, "%d[%d] Comm: receive data. (count=%d)\n", rank, size, recvcount);
    if(recvcount > 0) save_data(ibuf);
  }

  // switch buffer
  buf = global_buffers[ibuf];
  off = global_offsets[ibuf];
  for(int i = 0; i < size; i++) off[i] = 0;

  MPI_Allreduce(&medone, &pdone, 1, MPI_INT, MPI_SUM, comm);

#if GATHER_STAT
  double tstop = MPI_Wtime();
  st.inc_timer(tcomm, tstop-tstart);
#endif

  LOG_PRINT(DBG_COMM, "%d[%d] Comm: exchange KV. (send count=%d, done count=%d)\n", rank, size, sendcount, pdone);
}

void Alltoall::save_data(int i){
  if(blocks[0] == -1){
    blocks[0] = data->addblock();
  }

  data->acquireblock(blocks[0]);

  int offset=0;
  for(int k = 0; k < size; k++){
     if(recv_count[i][k] == 0) continue;
     while(data->adddata(blocks[0], recv_buf[i]+offset, recv_count[i][k]) == -1){
       data->releaseblock(blocks[0]);
       blocks[0] = data->addblock();
       data->acquireblock(blocks[0]);
     }

     offset += recv_count[i][k];
  }

  data->releaseblock(blocks[0]);
}

