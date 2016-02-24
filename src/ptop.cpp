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
    MPI_Irecv(recv_buf, gbufsize, MPI_BYTE, MPI_ANY_SOURCE, 0, comm, &recv_req);\
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
      *(char**)GET_VAL(buf,i,oneptrlen)   = global_buffers[ib]+i*gbufsize;\
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

  for(int i = 0; i < size; i++)
    omp_destroy_lock((omp_lock_t*)GET_VAL(lock,i,onelocklen));
  //delete [] lock;
  mem_aligned_free(lock);

  for(int i=0; i<nbuf; i++)
      delete [] reqs[i];
  delete [] reqs;

  free(recv_buf);
}

int Ptop::setup(int _lbufsize, int _gbufsize, int _kvtype, int _ksize, int _vsize, int _nbuf){
  Communicator::setup(_lbufsize, _gbufsize, _kvtype, _ksize, _vsize, _nbuf);

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

  recv_buf = (char*)mem_aligned_malloc(MEMPAGE_SIZE, gbufsize);

  return 0;
}

void Ptop::init(DataObject *_data){
  Communicator::init(_data);

  for(int i=0; i < size; i++){

    *(int*)GET_VAL(flags,i,oneintlen)=0;
    *(int*)GET_VAL(ibuf,i,oneintlen)=0;
    //flags[i] = 0;
    //ibuf[i] = 0;

   // buf[i] = global_buffers[0]+gbufsize*i;
   *(char**)GET_VAL(buf,i,oneptrlen)=global_buffers[0]+gbufsize*i;
   *(int*)GET_VAL(off,i,oneintlen)=0;
   //off[i] = 0;

    //omp_init_lock(&lock[i]);
    omp_init_lock((omp_lock_t*)GET_VAL(lock, i, onelocklen));

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
      while(*(int*)GET_VAL(flags,target,oneintlen) != 0){
        int flag;
        MPI_Is_thread_main(&flag);
        if(flag){
#if GATHER_STAT
          double t1 = omp_get_wtime();
#endif
          MAKE_PROGRESS;
#if GATHER_STAT
          double t2 = omp_get_wtime();
          //st.inc_timer(TIMER_MAP_SERIAL, t2-t1);
#endif
        }
      }

#if GATHER_STAT
      //double t1 = omp_get_wtime();
#endif
      //omp_set_lock(&lock[target]);
      omp_set_lock((omp_lock_t*)GET_VAL(lock, target, onelocklen));
#if GATHER_STAT
      //double t2 = omp_get_wtime();
      //if(tid==0) st.inc_timer(TIMER_SYN, t2-t1);
#endif
      // try to add the offset
      if(loff + *(int*)GET_VAL(off,target,oneintlen) <= gbufsize){
        int goff=*(int*)GET_VAL(off,target,oneintlen);
        memcpy(*(char**)GET_VAL(buf,target,oneptrlen)+goff, local_buffers[tid]+target*lbufsize, loff);
        *(int*)GET_VAL(off,target,oneintlen)+=loff;
        local_offsets[tid][target] = 0;
      }else{
       //flags[target]=1;
       *(int*)GET_VAL(flags, target, oneintlen)=1;
      }
      //omp_unset_lock(&lock[target]);
      omp_unset_lock((omp_lock_t*)GET_VAL(lock, target, onelocklen));
    }
  }

  //inc_counter(target); 

  // do communication
  //int flag;
  //MPI_Is_thread_main(&flag);
  //if(flag){
  //  exchange_kv();
  //}

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

    while(*(int*)GET_VAL(flags,i,oneintlen) != 0){
      int flag;
      MPI_Is_thread_main(&flag);
      if(flag){
        //MAKE_PROGRESS; 
#if GATHER_STAT
          double t1 = omp_get_wtime();
#endif
          MAKE_PROGRESS;
#if GATHER_STAT
          double t2 = omp_get_wtime();
          //st.inc_timer(TIMER_MAP_SERIAL, t2-t1);
#endif
      }
    }

    //printf("thread %d i=%d end\n", tid, i); fflush(stdout);

    int k=i;
    //omp_set_lock(&lock[k]);
    omp_set_lock((omp_lock_t*)GET_VAL(lock, k, onelocklen));
    int goff=*(int*)GET_VAL(off,i,oneintlen);
    if(goff+loff<=gbufsize){
      char *lbuf = local_buffers[tid]+i*lbufsize;
      memcpy(*(char**)GET_VAL(buf,i,oneptrlen)+goff, lbuf, loff);
      local_offsets[tid][i] = 0;
      *(int*)GET_VAL(off,i,oneintlen)+=loff;
      i++;
    }else{
      //flags[i]=1;
      *(int*)GET_VAL(flags, i, oneintlen)=1;
    }
    //omp_unset_lock(&lock[k]);
    omp_unset_lock((omp_lock_t*)GET_VAL(lock, k, onelocklen));
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
          double t1 = omp_get_wtime();
#endif
          MAKE_PROGRESS;
#if GATHER_STAT
          double t2 = omp_get_wtime();
          //st.inc_timer(TIMER_MAP_SERIAL, t2-t1);
#endif

    }
  }while(tdone < tnum);

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
      buf[i]   = global_buffers[ib]+i*gbufsize;
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
