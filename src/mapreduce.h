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

    // set prarameters
    void setKVtype(int);
    void setBlocksize(int);
    void setMaxblocks(int);
    void setMaxmem(int);
    void setTmpfilePath(const char *);
    void setOutofcore(int);
    void setLocalbufsize(int);
    void setGlobalbufsize(int);
    void sethash(int (*_myhash)(char *, int));

    // map and reduce interfaces
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

    uint64_t reduce(void (myreduce)(MapReduce *, char *, int, int, char *, 
       int *, void*), void* );

    uint64_t convert();

    uint64_t scan(void (myscan)(char *, int, int, char *, int *,void *), void *);

    /* add function invoked in mymap or myreduce */
    int add(char *key, int keybytes, char *value, int valuebytes);

    // output data into file
    // type: 0 for string, 1 for int, 2 for int64_t
    void output(int type=0, FILE *fp=stdout, int format=0);

private:
    // configuable parameters
    int kvtype;
    int blocksize;
    int nmaxblock;
    int maxmemsize;
    int outofcore;
    
    int lbufsize;
    int gbufsize;

    std::string tmpfpath;
    int (*myhash)(char *, int);
 
    // MPI Commincator
    MPI_Comm comm;
    int me, nprocs, tnum; 

    // used to add 
    int addtype;      // -1 for nothing, 0 for map, 1 for map_local, 2 for reduce;
    int *blocks;      // current block id for each threads, used in map and reduce

    // data object
    DataObject *data;

    // the communicator for map
    Communicator *c;

    // input file list
    std::vector<std::string> ifiles;

    // counter for each thread
    uint64_t *nitems;
    //uint64_t sum;

    // private functions
    void init();
    void disinputfiles(const char *, int, int);
    void getinputfiles(const char *, int, int);

    uint64_t sumcount();
};//class MapReduce

}//namespace

#endif
