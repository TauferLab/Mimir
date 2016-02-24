#ifndef COMMUNICATOR_H
#define COMMUNICATOR_H

#include <mpi.h>
#include "dataobject.h"

#include "config.h"

#define SAVE_DATA(recvbuf, recvcount) \
{\
  if(blocks[0]==-1){\
    blocks[0] = data->add_block();\
    data->acquire_block(blocks[0]);\
  }\
  int datasize=data->getfreespace(blocks[0]);\
  if(datasize+recvcount>data->blocksize){\
    data->release_block(blocks[0]);\
    blocks[0] = data->add_block();\
    data->acquire_block(blocks[0]);\
    datasize=0;\
  }\
  char *databuf = data->getblockbuffer(blocks[0]);\
  memcpy(databuf+datasize, recvbuf, recvcount);\
  data->setblockdatasize(blocks[0], datasize+recvcount);\
}

namespace MAPREDUCE_NS {

class Communicator{
public:
  Communicator(MPI_Comm _comm, int _commtype, int _tnum);
  
  virtual ~Communicator();

  // main thread
  virtual int setup(int, int, int kvtype=0, int ksize=0, int vsize=0, int nbuf=2);

  // main thread
  virtual void init(DataObject *data = NULL);

  // multi-threads
  virtual int sendKV(int, int, char *, int, char *, int) = 0;

  // multi-threads
  virtual void twait(int tid) = 0;

  // main thread
  virtual void wait() = 0;

protected:
  int fetch_and_add_with_max(int *counter, int adder, int maxnum){
    int val=0;
    do{
      val = *counter;
      if(val+adder>maxnum) break;
      if(__sync_bool_compare_and_swap(counter, val, val+adder))
        break;
    }while(1);
    return val;
  }

  // communicator and thread information
  MPI_Comm comm;
  int rank, size, tnum;

  // 0 for Alltoall, 1 for Isend/Irecv
  int commtype;   

  // terminate flag
  int medone; // if me done
  int tdone;  // done count for thread
  int pdone;  // done count for process

  // received data added into this object
  int *blocks;
  DataObject *data;

  // kv type
  int kvtype, ksize, vsize;

  // buffer size information
  int thread_buf_size, send_buf_size, nbuf;

  char **thread_buffers;   // local buffers for threads
  int  **thread_offsets;   // local offsets for threads

  char **send_buffers;  // global buffers
  int  **send_offsets;  // global offsets

public:
  uint64_t send_bytes, recv_bytes;

public:
  static Communicator* Create(MPI_Comm, int, int);
};

}

#endif
