#ifndef MIMIR_COMBINE_KV_CONTAINER_H
#define MIMIR_COMBINE_KV_CONTAINER_H

#include <stdio.h>
#include <stdlib.h>
#include <unordered_map>
#include "kvcontainer.h"
//#include "mapreduce.h"
#include "hashbucket.h"

namespace MIMIR_NS {

class CombinerHashBucket;
struct CombinerUnique;

class CombineKVContainer : public KVContainer {
public:
    CombineKVContainer();
    ~CombineKVContainer();

    void set_kv_size(int _ksize, int _vsize) {
        ksize = _ksize;
        vsize = _vsize;
    }

    int getNextKV(char**, int&, char**, int&);
    int addKV(const char*, int, const char*, int);
    int updateKV(const char*, int, const char*, int);

    void gc();

    //void set_combiner(MapReduce *_mr, UserCombiner _combiner);

    uint64_t get_kv_count(){
        return kvcount;
    }

    void print(FILE *fp, ElemType ktype, ElemType vtype);

public:
    Page *page;
    int64_t pageoff;

    int ksize, vsize;
    uint64_t kvcount;

    //MapReduce *mr;
    //UserCombiner mycombiner;

    std::unordered_map < char *, int >slices;
    CombinerHashBucket *bucket;

    CombinerUnique *u = NULL;
    char *ukey, *uvalue;
    int ukeybytes, uvaluebytes, ukvsize;
};

}

#endif
