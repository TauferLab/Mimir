/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_COMBINE_KV_CONTAINER_H
#define MIMIR_COMBINE_KV_CONTAINER_H

#include <stdio.h>
#include <stdlib.h>
#include <unordered_map>
#include "kvcontainer.h"
#include "hashbucket.h"

namespace MIMIR_NS {

class CombinerHashBucket;
struct CombinerUnique;

class CombineKVContainer : public KVContainer, public Combinable {
public:
    CombineKVContainer(CombineCallback user_combine,
                       void *user_ptr);
    ~CombineKVContainer();

    virtual bool open();
    virtual void close();
    virtual void write(BaseRecordFormat *record);
    virtual void update(BaseRecordFormat *record);

private:
    void garbage_collection();

    CombineCallback user_combine;
    void *user_ptr;
    std::unordered_map<char*, int> slices;
    CombinerHashBucket *bucket;
    CombinerUnique *u;
};

}

#endif
