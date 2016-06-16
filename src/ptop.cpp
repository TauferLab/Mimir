#include <stdio.h>
#include <stdlib.h>
#include <omp.h>

#include "log.h"
#include "config.h"
#include "ptop.h"

#include "const.h"

#include "memory.h"

using namespace MAPREDUCE_NS;

#if GATHER_STAT
#include "stat.h"
#endif

#define CHECK_MPI_REQS \
{\
  int flag;\
  MPI_Status st;\
  MPI_Test(&recv_req, &flag, &st);\
  if(flag){\
    int count;\
    MPI_Get_count(&st, MPI_BYTE, &count);\
    if(count>0) SAVE_DATA(recv_buf, count)\
    else pdone++;\
    MPI_Irecv(recv_buf, send_buf_size, MPI_BYTE, MPI_ANY_SOURCE, 0, comm, &recv_req);\
  }\
  for(int i=0;i<nbuf;i++){\
    for(int j=0;j<size;j++){\
      MPI_Test(&reqs[i][j], &flag, MPI_STATUS_IGNORE);\
      if(flag) reqs[i][j] = MPI_REQUEST_NULL;\
    }\
  }\
}

#define MAKE_PROGRESS \
{\
  int i;\
  for(i=0;i<size;i++){\
    if(*(int*)(GET_VAL(flags, i, oneintlen)) != 0){\
      int ib = *(int*)GET_VAL(ibuf,i,oneintlen);\
      send_bytes += *(int*)GET_VAL(off,i,oneintlen);\
      if(i!=rank)\
        MPI_Isend(*(char**)GET_VAL(buf,i,oneptrlen), *(int*)GET_VAL(off,i,oneintlen), MPI_BYTE, i, 0, comm, &reqs[ib][i]);\
      else\
        SAVE_DATA(*(char**)GET_VAL(buf,i,oneptrlen), *(int*)GET_VAL(off,i,oneintlen))\
      ib=(ib+1)%nbuf;\
      while(reqs[ib][i] != MPI_REQUEST_NULL){\
        CHECK_MPI_REQS;\
      }\
      omp_set_lock((omp_lock_t*)GET_VAL(lock,i,onelocklen));\
      *(char**)GET_VAL(buf,i,oneptrlen)   = send_buffers[ib]+i*send_buf_size;\
      *(int*)GET_VAL(off,i,oneintlen)   = 0;\
      *(int*)GET_VAL(ibuf,i,oneintlen)=ib;\
      *(int*)(GET_VAL(flags, i, oneintlen))=0;\
      omp_unset_lock((omp_lock_t*)GET_VAL(lock,i,onelocklen));\
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

  onelocklen=sizeof(omp_lock_t);

  lock = NULL;

  reqs = NULL;

  recv_buf = NULL;
  recv_req = MPI_REQUEST_NULL; 
}

Ptop::~Ptop(){
  mem_aligned_free(flags);
  mem_aligned_free(ibuf);
  //delete [] flags;
  //delete [] ibuf;

  mem_aligned_free(buf);
  mem_aligned_free(off);
  //delete [] buf;
  //delete [] off;

#ifdef MTMR_MULTITHREAD  
  for(int i = 0; i < size; i++)
    omp_destroy_lock((omp_lock_t*)GET_VAL(lock,i,onelocklen));
#endif

  //delete [] lock;
  mem_aligned_free(lock);

  for(int i=0; i<nbuf; i++)
      delete [] reqs[i];
  delete [] reqs;

  free(recv_buf);
}

int Ptop::setup(int _thread_buf_size, int _send_buf_size, int _kvtype, int _ksize, int _vsize, int _nbuf){
  Communicator::setup(_thread_buf_size, _send_buf_size, _kvtype, _ksize, _vsize, _nbuf);

  //flags = new int[size];
  flags = (int*)mem_aligned_malloc(CACHELINE_SIZE, size*ALIGNED_SIZE(oneintlen));
  ibuf  = (int*)mem_aligned_malloc(CACHELINE_SIZE, size*ALIGNED_SIZE(oneintlen));
  //ibuf  = new int[size];
 // buf   = new char*[size];
  buf = (char**)mem_aligned_malloc(CACHELINE_SIZE, size*ALIGNED_SIZE(oneptrlen));
  off = (int*)mem_aligned_malloc(CACHELINE_SIZE, size*ALIGNED_SIZE(oneintlen));
  //off   = new int[size];

  //lock  = new omp_lock_t[size];
  lock  = (omp_lock_t*)mem_aligned_malloc(CACHELINE_SIZE, size*ALIGNED_SIZE(onelocklen));

  reqs = new MPI_Request*[nbuf];
  for(int i=0; i<nbuf; i++)
    reqs[i] = new MPI_Request[size];

  recv_buf = (char*)mem_aligned_malloc(MEMPAGE_SIZE, send_buf_size);

  return 0;
}

void Ptop::init(DataObject *_data){
  Communicator::init(_data);

  for(int i=0; i < size; i++){

    *(int*)GET_VAL(flags,i,oneintlen)=0;
    *(int*)GET_VAL(ibuf,i,oneintlen)=0;
    //flags[i] = 0;
    //ibuf[i] = 0;

   // buf[i] = send_buffers[0]+send_buf_size*i;
   *(char**)GET_VAL(buf,i,oneptrlen)=send_buffers[0]+send_buf_size*i;
   *(int*)GET_VAL(off,i,oneintlen)=0;
   //off[i] = 0;

    //omp_init_lock(&lock[i]);
#ifdef MTMR_MULTITHREAD  
    omp_init_lock((omp_lock_t*)GET_VAL(lock, i, onelocklen));
#endif

    for(int j=0; j<nbuf; j++)
      reqs[j][i] = MPI_REQUEST_NULL;
  }

  MPI_Irecv(recv_buf, send_buf_size, MPI_BYTE, MPI_ANY_SOURCE, 0, comm, &recv_req);
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

#if SAFE_CHECK
  if(kvsize > thread_buf_size){
    LOG_ERROR("Error: send KV size is larger than local buffer size. (KV size=%d, local buffer size=%ld)\n", kvsize, thread_buf_size);
  }
#endif 

  /* copy kv into local buffer */
  while(1){
#ifdef MTMR_MULTITHREAD 
    int   loff=thread_offsets[tid][target];
    char *lbuf=thread_buffers[tid]+target*thread_buf_size+loff;
    
    // local buffer has space
    if(loff + kvsize <= thread_buf_size){
     PUT_KV_VARS(kvtype,lbuf,key,keysize,val,valsize,kvsize);
     thread_offsets[tid][target]+=kvsize;
     break;
    // local buffer is full
    }else{
      // make sure global buffer is ready
      while(*(int*)GET_VAL(flags,target,oneintlen) != 0){
        int flag;
        MPI_Is_thread_main(&flag);
        if(flag){
          MAKE_PROGRESS;
        }
      }

      omp_set_lock((omp_lock_t*)GET_VAL(lock, target, onelocklen));
     // try to add the offset
      if(loff + *(int*)GET_VAL(off,target,oneintlen) <= send_buf_size){
        int goff=*(int*)GET_VAL(off,target,oneintlen);
        memcpy(*(char**)GET_VAL(buf,target,oneptrlen)+goff, thread_buffers[tid]+target*thread_buf_size, loff);
        *(int*)GET_VAL(off,target,oneintlen)+=loff;
        thread_offsets[tid][target] = 0;
      }else{
       //flags[target]=1;
       *(int*)GET_VAL(flags, target, oneintlen)=1;
      }
      //omp_unset_lock(&lock[target]);
      omp_unset_lock((omp_lock_t*)GET_VAL(lock, target, onelocklen));
    }
#else
    int goff=*(int*)GET_VAL(off,target,oneintlen);
    if(kvsize+goff<=send_buf_size){
      char *gbuf=*(char**)GET_VAL(buf,target,oneptrlen)+goff; 
      PUT_KV_VARS(kvtype,gbuf,key,keysize,val,valsize,kvsize);
      *(int*)GET_VAL(off,target,oneintlen)+=kvsize;
    }
#endif
  }
  return 0;
}

void Ptop::tpoll(int tid){
#ifdef MTMR_MULTITHREAD  
#pragma omp atomic
  tdone++;

  do{
    int flag;
    MPI_Is_thread_main(&flag);
    if(flag){
      MAKE_PROGRESS;
    }
  }while(tdone < tnum);

#pragma omp barrier
  if(tid==0) tdone=0;
#pragma omp barrier
#endif
}

void Ptop::twait(int tid){
#ifdef MTMR_MULTITHREAD  
  LOG_PRINT(DBG_COMM, "%d[%d] thread %d start wait\n", rank, size, tid);

  int i=0;
  while(i<size){

#ifdef MTMR_MULTITHREAD 
    int loff = thread_offsets[tid][i];
    if(loff == 0){
      i++;
      continue;
    }
#endif

    //printf("thread %d i=%d begin\n", tid, i); fflush(stdout);

    while(*(int*)GET_VAL(flags,i,oneintlen) != 0){
      int flag;
      MPI_Is_thread_main(&flag);
      if(flag){
        //MAKE_PROGRESS; 
#if GATHER_STAT
          //double t1 = omp_get_wtime();
#endif
          MAKE_PROGRESS;
#if GATHER_STAT
          //double t2 = omp_get_wtime();
          //st.inc_timer(TIMER_MAP_SERIAL, t2-t1);
#endif
      }
    }

    //printf("thread %d i=%d end\n", tid, i); fflush(stdout);

    int k=i;
    //omp_set_lock(&lock[k]);
#ifdef MTMR_MULTITHREAD 
    omp_set_lock((omp_lock_t*)GET_VAL(lock, k, onelocklen));
    int goff=*(int*)GET_VAL(off,i,oneintlen);
    if(goff+loff<=send_buf_size){
      char *lbuf = thread_buffers[tid]+i*thread_buf_size;
      memcpy(*(char**)GET_VAL(buf,i,oneptrlen)+goff, lbuf, loff);
      thread_offsets[tid][i] = 0;
      *(int*)GET_VAL(off,i,oneintlen)+=loff;
      i++;
    }else{
      //flags[i]=1;
      *(int*)GET_VAL(flags, i, oneintlen)=1;
    }
    //omp_unset_lock(&lock[k]);
    omp_unset_lock((omp_lock_t*)GET_VAL(lock, k, onelocklen));
#endif
    //printf("thread %d i=%d copy end\n", tid, i); fflush(stdout);
  }

  //printf("thread %d flush local buffer end\n", tid); fflush(stdout);

#pragma omp atomic
  tdone++;

  do{
    int flag;
    MPI_Is_thread_main(&flag);
    if(flag){
      //exchange_kv();  
      //MAKE_PROGRESS;
#if GATHER_STAT
          //double t1 = omp_get_wtime();
#endif
          MAKE_PROGRESS;
#if GATHER_STAT
          //double t2 = omp_get_wtime();
          //st.inc_timer(TIMER_MAP_SERIAL, t2-t1);
#endif

    }
  }while(tdone < tnum);
#endif
}

void Ptop::wait(){
  LOG_PRINT(DBG_COMM, "%d[%d] wait start\n", rank, size);

  for(int i = 0; i < size; i++){
    if(*(int*)GET_VAL(off,i,oneintlen) != 0){
      int ib=*(int*)GET_VAL(ibuf, i, oneintlen);
      //printf("%d send <%d,%d> last count=%d\n", rank, ib, i, off[i]); fflush(stdout);
      send_bytes += *(int*)GET_VAL(off,i,oneintlen);
      if(i!=rank)
        MPI_Isend(*(char**)GET_VAL(buf,i,oneptrlen), *(int*)GET_VAL(off,i,oneintlen), MPI_BYTE, i, 0, comm, &reqs[ib][i]);
      else
        SAVE_DATA(*(char**)GET_VAL(buf,i,oneptrlen), (*(int*)GET_VAL(off,i,oneintlen)))
    }
  }

  //printf("%d begin wait data end\n", rank); fflush(stdout);

  for(int i = 0; i < size; i++){
    //int ib=ibuf[i];
    int ib=*(int*)GET_VAL(ibuf, i, oneintlen);
    while(reqs[ib][i] != MPI_REQUEST_NULL){
      CHECK_MPI_REQS;
    }
    if(i!=rank)
      MPI_Isend(NULL, 0, MPI_BYTE, i, 0, comm, &reqs[ib][i]);
  }
 
  do{
    CHECK_MPI_REQS;
  }while(pdone<size-1);

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


#if 0
void Ptop::exchange_kv(){
#if GATHER_STAT
  double tstart = omp_get_wtime();
#endif
#if GATHER_STAT
      double t1 = omp_get_wtime();
#endif

  int i;
  for(i = 0; i  < size; i++){
    if(flags[i] != 0){
      int ib = ibuf[i];
      //printf("%d send <%d,%d> count=%d\n", rank, ib, i, off[i]); fflush(stdout);

#if GATHER_STAT
      //double t1 = omp_get_wtime();
#endif
      send_bytes += off[i];
      MPI_Isend(buf[i], off[i], MPI_BYTE, i, 0, comm, &reqs[ib][i]);

#if GATHER_STAT
      //double t2 = omp_get_wtime();
      //st.inc_timer(TIMER_ISEND, t2-t1);;
#endif

      ib=(ib+1)%nbuf;

      while(reqs[ib][i] != MPI_REQUEST_NULL){
        CHECK_MPI_REQS;
      }

#if GATHER_STAT
      //double t3 = omp_get_wtime();
      //st.inc_timer(TIMER_CHECK, t3-t2);;
#endif


      reqs[ib][i] = MPI_REQUEST_NULL;

      omp_set_lock(&lock[i]);
      buf[i]   = send_buffers[ib]+i*send_buf_size;
      off[i]   = 0;
      ibuf[i]  = ib;
      flags[i] = 0;
      omp_unset_lock(&lock[i]);

#if GATHER_STAT
      //double t4 = omp_get_wtime();
      //st.inc_timer(TIMER_LOCK, t4-t3);;
#endif
    }
  }

#if GATHER_STAT
    double t2 = omp_get_wtime();
#endif

#if GATHER_STAT
  double tend = omp_get_wtime();
    st.inc_timer(TIMER_ISEND, t2-t1);;
  st.inc_timer(TIMER_COMM, tend-tstart);
#endif
}
#endif

#if 0
void Ptop::save_data(int recv_count){

  recv_bytes += recv_count;

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
#endif
