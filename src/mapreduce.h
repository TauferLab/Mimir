#ifndef MAP_REDUCE_H
#define MAP_REDUCE_H

#include <stdio.h>
#include <stdlib.h>

#include <string>
#include <vector>

#include <mpi.h>

#include "hash.h"
#include "dataobject.h"
#include "keyvalue.h"
#include "keymultivalue.h"
#include "communicator.h"

namespace MAPREDUCE_NS {

class MapReduce {
public:
    MapReduce(MPI_Comm);
    ~MapReduce();

    /* setup function */
    void sethash(int (*_myhash)(char *, int)){
      myhash = _myhash;
    }

    /* map functions */

    uint64_t map(char *, int, int, int, 
      void (*mymap) (MapReduce *, char *, void *), void *);

    uint64_t map_local(char *, int, int, int, 
      void (*mymap) (MapReduce *, char *, void *), void *);


    uint64_t map(char *, int, int, 
      void (*mymap) (MapReduce *, const char *, void *), void *);

    uint64_t map_local(char *, int, int, 
      void (*mymap) (MapReduce *, const char *, void *), void *);


    uint64_t map(MapReduce *, 
      void (*mymap) (MapReduce *, char *, int, char *, int, void *), void *);

    uint64_t map_local(MapReduce *,
      void (*mymap) (MapReduce *, char *, int, char *, int, void *), void *);

    /* reduce function */
    uint64_t reduce(void (myreduce)(MapReduce *, char *, int, int, char *, 
       int *, void*), void* );

    /* convert function */
    uint64_t convert();

    /* scan function */
    uint64_t scan(void (myscan)(char *, int, int, char *, int *,void *), void *);

    /* add function invoked in mymap or myreduce */
    int add(char *key, int keybytes, char *value, int valuebytes);

private:
    void disinputfiles(const char *, int, int);
    void getinputfiles(const char *, int, int);

    // MPI Commincator
    MPI_Comm comm;
    int me,nprocs; 

    DataObject *data;

    int *blocks;      // current block id for each threads, used in map and reduce
    int tnum;         // number of threads
    
    int addtype;      // -1 for nothing, 0 for map, 1 for map_local, 2 for reduce;
    Communicator *c;  // the communicator

    int (*myhash)(char *, int);

    std::vector<std::string> ifiles;
};//class MapReduce

}//namespace

#endif
