/**
 * @file   communicator.h
 * @Author Tao Gao (taogao.china@gmail.com)
 * @date   Oct. 17th, 2016
 * @brief  This file provides interfaces of communicator.
 *
 */
#ifndef COMMUNICATOR_H
#define COMMUNICATOR_H

#include <mpi.h>
#include "dataobject.h"
#include "mapreduce.h"
#include "config.h"

namespace MIMIR_NS {

class Communicator{
public:
    Communicator(MPI_Comm _comm, int _commtype);
    virtual ~Communicator();

    virtual int setup(int64_t, DataObject *data)=0;

    virtual int sendKV(int, char *, int, char *, int) = 0;

    virtual void wait() = 0;

protected:
    /// communicator information
    MPI_Comm comm;
    int rank, size;
    int commtype;

    ///  termination check
    int medone, pdone;

    /// data object
    DataObject *data;
    int blockid;

    /// buffer information
    int nbuf;
    int64_t send_buf_size;
    char **send_buffers;
    int  **send_offsets;

public:
    static Communicator* Create(MPI_Comm, int);
};

}

#endif
