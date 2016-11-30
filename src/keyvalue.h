/**
 * @file   keyvalue.h
 * @Author Tao Gao (taogao@udel.edu)
 * @date   September 1st, 2016
 * @brief  This file includes <Key,Value> object.
 */
#ifndef KEY_VALUE_H
#define KEY_VALUE_H

#include <stdio.h>
#include <stdlib.h>
#include <unordered_map>

#include "dataobject.h"
#include "mapreduce.h"
#include "hashbucket.h"

namespace MIMIR_NS {

//class CombinerUnique;
//template<class ElemType>
//class HashBucket;

class CombinerHashBucket;

class KeyValue : public DataObject{
public:
    KeyValue(int, int,
        int64_t pagesize=1,
        int maxpages=4);

    ~KeyValue();

    void set_kv_size(int _ksize, int _vsize){
        ksize = _ksize;
        vsize = _vsize;
    }

    // Scan KVs one by one
    int getNextKV(char **, int &, char **, int &);

    // Add KVs one by one
    int addKV(const char *, int, const char *, int);

    void gc();

    void set_combiner(MapReduce *_mr, UserCombiner _combiner);

    uint64_t get_local_count(){
        return local_kvs_count;
    }
    uint64_t get_global_count(){
        return global_kvs_count;
    }
    void set_global_count(uint64_t _count){
        global_kvs_count = _count;
    }

    /* used for debug */
    void print(FILE *fp, ElemType ktype, ElemType vtype);

public:
    //enum KVType kvtype;
    int    ksize, vsize;
    uint64_t local_kvs_count, global_kvs_count; 

    MapReduce *mr;
    UserCombiner mycombiner;

    std::unordered_map<char*, int> slices;
    CombinerHashBucket* bucket;
};

}

#endif
