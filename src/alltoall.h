/**
 * @file   alltoall.h
 * @Author Tao Gao (taogao.china@gmail.com)
 * @date   Oct. 17th, 2016
 * @brief  This file provides MPI_Alltoll communication.
 *
 */
#ifndef ALLTOALL_H
#define ALLTOALL_H

#include <mpi.h>

#include "dataobject.h"
#include "communicator.h"

namespace MIMIR_NS {

class Alltoall : public Communicator{
public:
    Alltoall(MPI_Comm);
    ~Alltoall();

    int setup(int64_t, KeyValue *, MapReduce *, UserCombiner, UserHash);
    int sendKV(const char *, int, const char *, int);
    void wait();

    void gc();

private:
    void exchange_kv();
    void save_data(int);

    int   switchflag;
    int   ibuf;
    char *buf;
    int  *off;

    int **recv_count;
    char **recv_buf;
    int64_t  *recvcounts;
    int type_log_bytes;

    MPI_Request *reqs;
};

}
#endif
