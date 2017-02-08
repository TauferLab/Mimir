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
    virtual void write(KVRecord *record);
    virtual void update(KVRecord *record);

public:
    void garbage_collection();

    CombineCallback user_combine;
    void *user_ptr;
    std::unordered_map<char*, int> slices;
    CombinerHashBucket *bucket;
};

}

#endif
