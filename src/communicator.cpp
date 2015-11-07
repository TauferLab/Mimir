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

    //printf("malloc: buffers[%d]=%p\n", tid, local_buffers[tid]);

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
 
  return 0;
}


void Communicator::init(DataObject *_data){
  medone = tdone = pdone = 0;
  data = _data; 

  for(int i = 0; i < tnum; i++) blocks[i] = -1;
}


Alltoall::Alltoall(MPI_Comm _comm, int _tnum):Communicator(_comm, 0, _tnum){
  switchflag = 0;

  ibuf = 0;
  buf = NULL;
  off = NULL;

  send_displs = recv_count = recv_displs = NULL;

  recv_buf = NULL;
  recvcounts = NULL;
  
  reqs = NULL;

  LOG_PRINT(DBG_COMM, "%d[%d] Comm: alltoall create.\n", rank, size);
}


Alltoall::~Alltoall(){
  for(int i = 0; i < nbuf; i++){
    if(recv_buf && recv_buf[i]) free(recv_buf[i]);
  }

  if(recv_buf) delete [] recv_buf;

  if(send_displs) delete [] send_displs;
  if(recv_displs) delete [] recv_displs;
  if(recv_count) delete [] recv_count;

  if(recvcounts) delete [] recvcounts;

  if(reqs) delete [] reqs;

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

  for(int i = 0; i < nbuf; i++){
    recv_buf[i] = (char*)malloc(size*gbufsize);
  }

  send_displs = new int[size];
  recv_count  = new int[size];
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

  if(target < 0 || target >= size){
    LOG_ERROR("Error: target process (%d) isn't correct!\n", target);
  }

  if(tid < 0 || tid >= tnum){
    LOG_ERROR("Error: thread num (%d) isn't correct!\n", tid);
  }

  int kvsize = 0;
  if(kvtype == 0) kvsize = keysize+valsize;
  else if(kvtype == 1) kvsize = keysize+valsize+sizeof(int)*2;
  else if(kvtype == 2) kvsize = keysize+valsize;
  else LOG_ERROR("%s", "Error undefined kv type\n");

  if(kvsize > lbufsize){
    LOG_ERROR("Error: send KV size is larger than local buffer size. (KV size=%d, local buffer size=%d)\n", kvsize, lbufsize);
  }
 
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

    // local buffer has space
    if(loff + kvsize <= lbufsize){
      if(kvtype == 0){
        memcpy(local_buffers[tid]+target*lbufsize+loff, key, keysize);
        loff += keysize;
        memcpy(local_buffers[tid]+target*lbufsize+loff, val, valsize);
        loff += valsize;
     }else if(kvtype == 1){
        memcpy(local_buffers[tid]+target*lbufsize+loff, (char*)&keysize, sizeof(int)); 
        loff += sizeof(int);
        memcpy(local_buffers[tid]+target*lbufsize+loff, key, keysize);
        loff += keysize;
        memcpy(local_buffers[tid]+target*lbufsize+loff, (char*)&valsize, sizeof(int));
        loff += sizeof(int);
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
      }
      else{
        LOG_ERROR("%s", "Error undefined kv type\n");
      }
      local_offsets[tid][target] = loff;
      break;
    // local buffer is full
    }else{
       // try to add the offset
      if(loff + off[target] <= gbufsize){

//#pragma omp critical
//{
//        goff = off[target];
//        if(off[target] + loff <= gbufsize)
//          off[target] += loff;
//}
        //printf("local=%d, global=%d\n", loff, off[target]);
        int goff=0;
        do{
          goff = off[target];
          if(goff + loff > gbufsize) break;
          if(__sync_bool_compare_and_swap(&off[target], goff, goff+loff))
            break;
        }while(1);
       // printf("global=%d\n", off[target]);
        //int goff = __sync_fetch_and_add(&off[target], loff);
        // get global buffer successfully
        if(goff + loff <= gbufsize){
          memcpy(buf+target*gbufsize+goff, local_buffers[tid]+target*lbufsize, loff);
          local_offsets[tid][target] = 0;
        // global buffer is full, add back the offset
        }else{
          //int noff = 0-loff;
          //__sync_fetch_and_add(&off[target], noff);
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

  return 0;
}

void Alltoall::twait(int tid){

  LOG_PRINT(DBG_COMM, "%d[%d] Comm: thread %d begin wait.\n", rank, size, tid);

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
      //int goff = __sync_fetch_and_add(&off[i], loff);

//      int goff;
//#pragma omp critical
//{
//      goff = off[target];
//      if(off[target] + loff <= gbufsize)
//        off[target] += loff;
//}
    int goff=0;
    do{
      goff = off[i];
      if(goff + loff > gbufsize) break;
      if(__sync_bool_compare_and_swap(&off[i], goff, goff+loff))
         break;
     }while(1);

     // copy data to global buffer
     if(goff+loff<=gbufsize){
       memcpy(buf+i*gbufsize+goff, lbuf, loff);
       local_offsets[tid][i] = 0;
       i++;
       continue;
      // need flush global buffer firstly
     }else{
        //int noff = 0-loff;
        //__sync_fetch_and_add(&off[i], noff);
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

  LOG_PRINT(DBG_COMM, "%d[%d] Comm: thread %d finish wait.\n", rank, size, tid);
}

// wait all procsses done
void Alltoall::wait(){
   LOG_PRINT(DBG_COMM, "%d[%d] Comm: start wait.\n", rank, size);

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
       if(recvcount > 0){
         if(blocks[0] == -1){
           blocks[0] = data->addblock();
         }

         data->acquireblock(blocks[0]);

         while(data->adddata(blocks[0], recv_buf[i], recvcount) == -1){
           data->releaseblock(blocks[0]);
           blocks[0] = data->addblock();
           data->acquireblock(blocks[0]);
         }

         data->releaseblock(blocks[0]);
       }
     }
   }

   LOG_PRINT(DBG_COMM, "%d[%d] Comm: finish wait.\n", rank, size);
}

void Alltoall::exchange_kv(){
  int i;

  //printf("%d[%d] exchange kv\n", rank, size);

  int sendcount=0;
  for(i=0; i<size; i++) sendcount += off[i];
  // exchange send count
  MPI_Alltoall(off, 1, MPI_INT, recv_count, 1, MPI_INT, comm);

  for(i = 0; i < size; i++) send_displs[i] = i*gbufsize;

  recvcounts[ibuf] = recv_count[0];
  recv_displs[0] = 0;
  for(i = 1; i < size; i++){
    recv_displs[i] = recv_count[i-1] + recv_displs[i-1];
    recvcounts[ibuf] += recv_count[i];
  }

  // exchange kv data
  MPI_Ialltoallv(buf, off, send_displs, MPI_BYTE, recv_buf[ibuf], recv_count, recv_displs,MPI_BYTE, comm,  &reqs[ibuf]);

  //printf("send ibuf=%d, send count=%d\n", ibuf, sendcount);  

  // wait data
  ibuf = (ibuf+1)%nbuf;
  if(reqs[ibuf] != MPI_REQUEST_NULL) {
    MPI_Status st;
    MPI_Wait(&reqs[ibuf], &st);
    reqs[ibuf] = MPI_REQUEST_NULL;
    int recvcount = recvcounts[ibuf];

    //printf("recv ibuf=%d, recv count=%d\n", ibuf, recvcount); 

    LOG_PRINT(DBG_COMM, "%d[%d] Comm: receive data. (count=%d)\n", rank, size, recvcount);
    if(recvcount > 0){
      //data->addblock(recv_buf[ibuf], recvcount);
      if(blocks[0] == -1){
        blocks[0] = data->addblock();
      }

      data->acquireblock(blocks[0]);

      while(data->adddata(blocks[0], recv_buf[ibuf], recvcount) == -1){
        data->releaseblock(blocks[0]);
        blocks[0] = data->addblock();
        data->acquireblock(blocks[0]);
      }

      data->releaseblock(blocks[0]);

      ///data->print();

    }
  }

  // switch buffer
  buf = global_buffers[ibuf];
  off = global_offsets[ibuf];
  for(int i = 0; i < size; i++) off[i] = 0;

  MPI_Allreduce(&medone, &pdone, 1, MPI_INT, MPI_SUM, comm);

  LOG_PRINT(DBG_COMM, "%d[%d] Comm: exchange KV. (send count=%d, done count=%d)\n", rank, size, sendcount, pdone);

  //printf("exchange kv end\n");
}

