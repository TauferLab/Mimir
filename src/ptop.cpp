#include <stdio.h>
#include <stdlib.h>
#include <omp.h>

#include "log.h"
#include "config.h"
#include "ptop.h"

using namespace MAPREDUCE_NS;

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

  recv_buf = NULL;
  recv_req = MPI_REQUEST_NULL;
 
  lock = NULL;

  reqs = NULL;
}

Ptop::~Ptop(){
  if(flags)    delete [] flags;
  if(ibuf)     delete [] ibuf;
  if(buf)      delete [] buf;
  if(off)      delete [] off;
  if(recv_buf) delete [] recv_buf;

  for(int i=0; i<nbuf; i++){
    if(reqs[i] != NULL)
      delete reqs[i];
  }
  if(reqs)     delete [] reqs;

  for(int i = 0; i < size; i++){
    omp_destroy_lock(&lock[i]);
  }
  if(lock)     delete [] lock;
}

int Ptop::setup(int _lbufsize, int _gbufsize, int _kvtype, int _ksize, int _vsize, int _nbuf){
  Communicator::setup(_lbufsize, _gbufsize, _kvtype, _ksize, _vsize, _nbuf);

  flags = new int[size];
  ibuf = new int[size];
  buf = new char*[size];
  off = new int*[size];
  lock = new omp_lock_t[size];

  recv_buf = (char*)malloc(gbufsize);

  reqs = new MPI_Request*[nbuf];
  for(int i=0; i<_nbuf; i++){
    reqs[i] = new MPI_Request[size];
    for(int k=0; k<size; k++){
      *(reqs[i]+k) = MPI_REQUEST_NULL;
    }
  }

  for(int i=0; i<size; i++){
    flags[i] = 0;
    ibuf[i]  = 0;
    buf[i] = global_buffers[0]+gbufsize*i;
    off[i] = &global_offsets[0][i];
    *off[i] = 0;

    omp_init_lock(&lock[i]);
  }
}

void Ptop::init(DataObject *_data){
  Communicator::init(_data);

  for(int i=0; i < size; i++){
    flags[i] = 0;
    ibuf[i] = 0;
    buf[i] = global_buffers[0]+gbufsize*i;
    off[i] = &global_offsets[0][i];
    *off[i] = 0;

    omp_init_lock(&lock[i]);
  }

  recv_req = MPI_REQUEST_NULL;
}

int Ptop::sendKV(int tid, int target, char *key, int keysize, char *val, int valsize){
  //printf("T%d of P%d send KV(%s,%s) to %d\n", tid, rank, key, val, target);

  if(target < 0 || target >= size){
    LOG_ERROR("Error: target process (%d) isn't correct!\n", target);
  }

  if(tid < 0 || tid >= tnum){
    LOG_ERROR("Error: thread num (%d) isn't correct!\n", tid);
  }

  int kvsize = 0;
  if(kvtype == 0) kvsize = keysize+valsize+sizeof(int)*2;
  else if(kvtype == 1) kvsize = keysize+valsize;
  else if(kvtype == 2) kvsize = keysize+valsize;
  else LOG_ERROR("%s", "Error undefined kv type\n");

  if(kvsize > lbufsize){
    LOG_ERROR("Error: send KV size is larger than local buffer size. (KV size=%d, local buffer size=%d)\n", kvsize, lbufsize);
  }
 
  /* copy kv into local buffer */
  while(1){

    int loff = local_offsets[tid][target];
    
    // local buffer has space
    if(loff + kvsize <= lbufsize){
     if(kvtype == 0){
       memcpy(local_buffers[tid]+target*lbufsize+loff, (char*)&keysize, sizeof(int));       loff += sizeof(int);
       memcpy(local_buffers[tid]+target*lbufsize+loff, key, keysize);
       loff += keysize;
       memcpy(local_buffers[tid]+target*lbufsize+loff, (char*)&valsize, sizeof(int));
       loff += sizeof(int);
       memcpy(local_buffers[tid]+target*lbufsize+loff, val, valsize);
       loff += valsize;
     }else if(kvtype == 1){
       memcpy(local_buffers[tid]+target*lbufsize+loff, key, keysize);
       loff += keysize;
       memcpy(local_buffers[tid]+target*lbufsize+loff, val, valsize);
       loff += valsize;
     }else if(kvtype == 2){
       if(ksize != keysize || vsize != valsize){
         LOG_ERROR("Error: key (%d) or val (%d) size mismatch for KV type 2\n", keysize, valsize);
       }
       memcpy(local_buffers[tid]+target*lbufsize+loff, key, keysize);
       loff += keysize;
       memcpy(local_buffers[tid]+target*lbufsize+loff, val, valsize);
       loff += valsize;
     }else{
       LOG_ERROR("%s", "Error undefined kv type\n");
     }
     if(loff > lbufsize){
       LOG_ERROR("%s", "Error: local buffer offset is larger than local buffer size!\n");
     }
     local_offsets[tid][target] = loff;
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
      if(loff + *off[target] <= gbufsize){
        //int goff=fetch_and_add_with_max(off[target], loff, gbufsize);
        // get global buffer successfully
        int goff=*off[target];
        if(goff + loff <= gbufsize){
          memcpy(buf[target]+goff, local_buffers[tid]+target*lbufsize, loff);
          *off[target]+=loff;
          local_offsets[tid][target] = 0;
        }else{
         flags[target]=1;
         // __sync_bool_compare_and_swap(&flags[target], 0, 1);
        }
      // need wait flush
      }else{
        flags[target]=1;
        //__sync_bool_compare_and_swap(&flags[target], 0, 1);
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
  printf("P%d,t%d wait\n", rank, tid);

  int i=0;
  while(i<size){

    int loff = local_offsets[tid][i];
    if(loff == 0){
      i++;
      continue;
    }

    while(flags[i] != 0){
      int flag;
      MPI_Is_thread_main(&flag);
      if(flag){
        exchange_kv();  
      }
    }

    char *lbuf = local_buffers[tid]+i*lbufsize;

    int k=i;
    omp_set_lock(&lock[k]);
    int goff=*off[i];
    if(goff+loff<=gbufsize){
      memcpy(buf[i]+goff, lbuf, loff);
      local_offsets[tid][i] = 0;
      *off[i]+=loff;
      i++;
      //continue;
    }else{
      //__sync_bool_compare_and_swap(&flags[i], 0, 1);
      flags[i]=1;
    }
    omp_unset_lock(&lock[k]);
  }

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
  printf("P%d wait\n", rank);

  //printf("Begin to flush buffers:\n");
  for(int i = 0; i < size; i++){
    if(*off[i] != 0){
      //printf("%d off=%d\n", i, *off[i]);
      int ib=ibuf[i];
      //printf("ib=%d\n", ib);
      //printf("%d->%d: length=%d\n", rank, i, *off[i]);
      MPI_Isend(buf[i], *off[i], MPI_BYTE, i, 0, comm, reqs[ib]+i);
    }
  }

  //printf("Send zero messages:\n");
  for(int i = 0; i < size; i++){
    int ib=ibuf[i];
    //printf("%d->%d: length=0\n", rank, i);
    MPI_Isend(buf[i], 0, MPI_BYTE, i, 0, comm, reqs[ib]+i);
  }

  //printf("wait others!\n");
  do{
    exchange_kv();
    //int flag;
    //MPI_Is_thread_main(&flag);
    //if(flag){
    //  exchange_kv();  
    //}
  }while(pdone<size);

  //printf("cancel recveieve\n");
  if(recv_req != MPI_REQUEST_NULL)
    MPI_Cancel(&recv_req);
}

void Ptop::exchange_kv(){
  //printf("%d exchange KV\n", rank);

  int i;
  for(i = 0; i  < size; i++){
    if(flags[i] != 0){
      int ib = ibuf[i];
      //printf("%d send to %d (length=%d, ibuf=%d)\n", rank, i, *off[i], ib);
      MPI_Isend(buf[i], *off[i], MPI_BYTE, i, 0, comm, reqs[ib]+i);

      ib=(ib+1)%nbuf;
      if(*(reqs[ib]+i) != MPI_REQUEST_NULL){
        MPI_Status st;
        //printf("MPI_Wait: ibuf=%d,i=%d\n", ib, i);
        MPI_Wait(reqs[ib]+i, &st);
        *(reqs[ib]+i) = MPI_REQUEST_NULL;
      }

      omp_set_lock(&lock[i]);
      buf[i]                = global_buffers[ib]+i*gbufsize;
      global_offsets[ib][i] = 0;
      off[i]                = &global_offsets[ib][i];
      ibuf[i]               = ib;
      flags[i]              = 0;
      omp_unset_lock(&lock[i]);
    }
  }  

  if(recv_req == MPI_REQUEST_NULL){
    MPI_Irecv(recv_buf, gbufsize, MPI_BYTE, MPI_ANY_SOURCE, 0, comm, &recv_req);
  }else{
    int flag;
    MPI_Status st;
    MPI_Test(&recv_req, &flag, &st);
    if(flag){
      int count;
      MPI_Get_count(&st, MPI_BYTE, &count);
      if(count>0) save_data(count);
      else pdone++;
      MPI_Irecv(recv_buf, gbufsize, MPI_BYTE, MPI_ANY_SOURCE, 0, comm, &recv_req);
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
