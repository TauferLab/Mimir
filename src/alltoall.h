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

  int setup(int64_t, int64_t, int kvtype=0, int ksize=0, int vsize=0, int nbuf=2);

  void init(DataObject *);
 
  int sendKV(int, int, char *, int, char *, int);

  void tpoll(int tid);
 
  // multi-threads
  void twait(int tid);

  // main thread
  void wait();

private:

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
  //uint64_t *send_displs;   
  //uint64_t *recv_displs;

  int **recv_count;
  char **recv_buf;     
  uint64_t  *recvcounts;
  int one_type_bytes;

  //char **comm_recv_buf;
  //int  **comm_recv_count;
  //int  **comm_recv_displs;

  //uint64_t comm_max_size;    // communication max size
  //int comm_unit_size;   // communication unit size
  //int comm_div_count;   // communication divide count

  MPI_Request *reqs;
};

}
#endif
