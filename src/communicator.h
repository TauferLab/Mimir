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
#include <unordered_map>

#include "interface.h"
#include "container.h"
#include "mapreduce.h"
#include "hashbucket.h"
#include "config.h"

namespace MIMIR_NS {

template <class ElemType> class HashBucket;

class Communicator : public BaseOutput {
public:
    Communicator(MPI_Comm _comm, int _commtype);
    virtual ~Communicator();

    virtual int setup(int64_t, BaseOutput *kv, UserCombiner combiner, HashCallback myhash);

    virtual bool open() = 0;
    virtual void add(const char*, int, const char*, int) = 0;
    virtual void close() = 0;
    virtual int updateKV(const char*, int, const char*, int) = 0;
    virtual void wait() = 0;
    virtual void gc() = 0;

protected:
    /// communicator information
    MPI_Comm comm;
    int rank, size;
    int commtype;

    ///  termination check
    int medone, pdone;

    /// data object
    MapReduce *mr;
    UserCombiner mycombiner;
    HashCallback myhash;
    BaseOutput *kv;
    //int blockid;

    /// buffer information
    int nbuf;
    int64_t send_buf_size;
    char **send_buffers;
    int **send_offsets;

public:
    std::unordered_map<char*, int> slices;
    CombinerHashBucket *bucket;
    CombinerUnique *u = NULL;
    int target;
    char *ukey, *uvalue;
    int ukeysize, uvaluesize, ukvsize;

public:
    static Communicator* Create(MPI_Comm, int);
};

}
#endif
