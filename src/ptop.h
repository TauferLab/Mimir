#ifndef PTOP_H
#define PTOP_H

#include <mpi.h>
#include <omp.h>

#include "dataobject.h"
#include "communicator.h"

#define ALIGNED_SIZE(typesize) \
  ((typesize+CACHELINE_SIZE-1)/CACHELINE_SIZE*CACHELINE_SIZE)
#define GET_VAL(startptr, index, typesize) \
  (((char*)startptr)+(index*ALIGNED_SIZE(typesize)))

namespace MAPREDUCE_NS {

class Ptop : public Communicator{
public:
  Ptop(MPI_Comm, int);
  ~Ptop();

  // main thread
  int setup(int, int, int kvtype=0, int ksize=0, int vsize=0, int nbuf=2);

  // main thread
  void init(DataObject *);

  // multi-thread
  int sendKV(int, int, char *, int, char *, int);

  // thread wait
  void twait(int tid);

  // process wait
  void wait();

private:
  //void exchange_kv();
  //void recv_data();
  void save_data(int);

  int  *flags; 
  int  *ibuf;

  char* *buf;
  int   *off;
  omp_lock_t *lock;

  int onelocklen;

  MPI_Request **reqs;


  char *recv_buf;
  MPI_Request recv_req;
};

}

#endif
