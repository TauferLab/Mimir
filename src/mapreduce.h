/**
 * @file   mapreduce.h
 * @Author Tao Gao (taogao@udel.edu)
 * @date   September 1st, 2016
 * @brief  This file provides interfaces to application programs.
 *
 * This file includes two classes: MapReduce and MultiValueIter.
 */
#ifndef MAP_REDUCE_H
#define MAP_REDUCE_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h>
#include <string.h>
#include <mpi.h>
#include <string>
#include <vector>

#include "inputstream.h"

//#include "config.h"
//#include "const.h"

#include "callbacks.h"

/// KV Type
enum KVType {
    KVGeneral = -2,             // variable length bytes
    KVString,   // string
    KVFixed
};      // fixed-size KV

enum ElemType {
    StringType,
    Int32Type,
    Int64Type
};

namespace MIMIR_NS {

class MapReduce;
class Communicator;
class DataObject;
class KeyValue;
class Spool;
class MultiValueIterator;
class ReducerHashBucket;

class ReducerUnique;
class ReducerSet;

enum OpPhase { NonePhase, MapPhase, LocalMapPhase, ReducePhase, ScanPhase, CombinePhase };

/// map callback to map files
typedef void (*ProcessBinaryFile)(MapReduce*, IStream*, void*);

/// hash callback
typedef int (*UserHash) (const char*, int);

/// map callback to init KVs
typedef void (*UserInitKV) (MapReduce*, void*);

/// map callback to map files
typedef void (*UserMapFile) (MapReduce*, const char*, void*);

/// map callback to map KVs
typedef void (*UserMapKV) (MapReduce*, char*, int, char*, int, void*);

/// reduce callback
typedef void (*UserReduce) (MapReduce*, char*, int, void*);

/// combiner callback
typedef void (*UserCombiner) (MapReduce*,
                              const char*, int, const char*, int, const char*, int, void*);

/// User-defined scan function
typedef void (*UserScan) (char*, int, char*, int, void*);

/// MapReduce
class MapReduce {
public:
    /**
      Constructor function.

      @param[in]  comm MPI communicator
      @return MapReduce object pointer
      */
    MapReduce(MPI_Comm comm);

    /**
      Copy function.

      @param[in]  mr MapReduce object
      @return MapReduce object pointer
      */
    MapReduce(const MapReduce &mr);

    /**
      Destructor function.
      */
    ~MapReduce();

    /**
      init_key_value  load KVs from memory (e.g. in-situ workload)

      @param[in]  myinit user-defined init function
      @param[in]  ptr    user-defined pointer (default: NULL)
      @param[in]  comm   with communication or not (default: 1)
      @return output <key,value> count
      */
    uint64_t init_key_value(UserInitKV myinit, void *ptr=NULL, int repartition=1);

    /**
      Map function with text files.

      @param[in]  filename   input file name or directory
      @param[in]  shared     if the input files in shared file system
      @param[in]  recurse    if read subdirectory recursely
      @param[in]  seperator  seperator string, for exampel "\n"
      @param[in]  mymap      user-defined map function
      @param[in]  ptr        user-defined pointer (default: NULL)
      @param[in]  comm       with communication or not (default: 1)
      @return output <key,value> count
      */
    uint64_t map_text_file(const char *filename, int shared, int recurse,
                           const char *seperator, UserMapFile mymap, void *ptr =
                           NULL, int repartition = 1);
    uint64_t process_binary_file(const char *filepath, int shared, int recurse, 
                                 ProcessBinaryFile myfunc, UserSplit mysplit, 
                                 void* ptr=NULL, int repartition=1);

    /**
      Map function with MapReduce object as input.

      @param[in]  mr MapReduce Object pointer
      @param[in]  mymap user-defined map function
      @param[in]  ptr   user-defined pointer (default: NULL)
      @param[in]  comm  communication or not (default: 1)
      @return output <key,value> count
      */
    uint64_t map_key_value(MapReduce *mr, UserMapKV mymap, void *ptr = NULL, int repartition = 1);

    /**
      Reduce function.

      @param[in]  myreduce user-defined reduce function
      @param[in]  ptr user-defined pointer (default: NULL)
      @return output <key,value> count
      */
    uint64_t reduce(UserReduce _myreduce, void *ptr = NULL);

    /**
      Scan function.

      @param[in]  myscan user-defined scan function
      @param[in]  ptr user-defined pointer (default: NULL)
      */
    void scan(UserScan myscan, void *ptr = NULL);

    uint64_t bcast(int rank = 0);
    uint64_t collect(int rank = 0);

    /**
      Add <key,value>. This function only can be invoked in callbacks.

      @param[in]  key key pointer
      @param[in]  keybytes key size
      @param[in]  value value pointer
      @param[in]  valubytes value size
      @return nothing
      */
    void add_key_value(const char *key, int keybytes, const char *value, int valuebytes);

    void update_key_value(const char *key, int keybytes, const char *value, int valuebytes);

    void output(FILE *fp, ElemType key, ElemType val);

    void set_key_length(int);
    void set_value_length(int);

    void set_combiner(UserCombiner combiner) {
        mycombiner = combiner;
    } void set_hash(UserHash _myhash) {
        myhash = _myhash;
    } const void *get_first_value();
    const void *get_next_value();

    static void output_stat(const char *filename);

private:
    friend class MultiValueIterator;
    friend class Alltoall;

private:
    MPI_Comm comm;  ///< MPI communicator
    int me, nprocs;         ///< MPI communicator information

    OpPhase phase;          ///< operation mode
    KeyValue *kv;           ///< KV container
    Communicator *c;        ///< communicator
    std::vector < std::pair < std::string, int64_t > >ifiles;

    UserHash myhash;        ///< user-define hash function
    UserCombiner mycombiner;        ///< user-defined combiner function

    //enum KVType kvtype;              ///< KV types
    int ksize, vsize;

public:
    void *myptr;
    MultiValueIterator *iter;

private:

    // Get default values
    void _get_default_values();

    // Get number of KVs
    uint64_t _get_kv_count();

    // Convert string to int64_t
    int64_t _convert_to_int64(const char*);

    // Get input file list
    void _get_input_files(const char *, int, int);

    // distribute input file list
    void _dist_input_files(const char *, int, int);

    // reduce phase
    void _reduce(ReducerHashBucket * u, UserReduce _myreduce, void *ptr);

    // convert phase
    void _convert(KeyValue *inkv, DataObject *mv, ReducerHashBucket * u);

public:
    static int ref;

};  //class MapReduce

class MultiValueIterator {
public:

    MultiValueIterator(KeyValue *kv, ReducerUnique *ukey);

    void set_kv_type(enum KVType, int, int);

    void Begin();
    void Next();

    int Done() {
        return isdone;
    }
    const char *getValue() {
        return value;
    }
    int getSize() {
        return valuesize;
    }
    int64_t getCount() {
        return nvalue;
    }

private:

    int64_t nvalue;
    int *valuebytes;
    char *values;

    int64_t ivalue;
    int64_t value_start;
    int64_t value_end;

    int isdone;
    char *value;
    int valuesize;

    KeyValue *kv;
    ReducerUnique *ukey;
    ReducerSet *pset;
};

}       //namespace

#endif
