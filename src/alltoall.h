#ifndef ALLTOALL_H
#define ALLTOALL_H

#include <mpi.h>

#include "dataobject.h"
#include "communicator.h"

namespace MAPREDUCE_NS {

class Alltoall : public Communicator{
public:
  Alltoall(MPI_Comm, int);
  ~Alltoall();

  int setup(int, int, int kvtype=0, int ksize=0, int vsize=0, int nbuf=2);

  void init(DataObject *);
 
  int sendKV(int, int, char *, int, char *, int);
 
  // multi-threads
  void twait(int tid);

  // main thread
  void wait();

private:
#if GATHER_STAT
  int tcomm, tsyn;
  //int *tsendkv, *thwait;
#endif

   // exchange kv buffer
  void exchange_kv();

  void save_data(int);

   // data struct for type 0
  int switchflag;

  // global buffer informatoion
  int ibuf;
  char *buf;
  int  *off;

  // used for MPI_Ialltoall
  int *send_displs;   
  int *recv_displs;

  int **recv_count;
  char **recv_buf;     
  int  *recvcounts; 
  MPI_Request *reqs;
};

}
#endif
