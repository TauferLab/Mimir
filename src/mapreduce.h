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

#include "spool.h"

namespace MAPREDUCE_NS {

enum OpMode{NoneMode, MapMode, MapLocalMode, ReduceMode};

class MapReduce {
public:
    MapReduce(MPI_Comm);
    ~MapReduce();

    // set prarameters
    void setKVtype(int, int ksize=0, int vsize=0);
    void setBlocksize(int);
    void setMaxblocks(int);
    void setMaxmem(int);
    void setTmpfilePath(const char *);
    void setOutofcore(int);
    void setLocalbufsize(int);
    void setGlobalbufsize(int);
    void setCommMode(int);
    void sethash(int (*_myhash)(char *, int));

    // map and reduce interfaces
    uint64_t map(void (*mymap)(MapReduce *, void *), void *);
    uint64_t map_local(void (*mymap)(MapReduce *, void *), void*);

    uint64_t map(char *, int, int, char *, 
      void (*mymap) (MapReduce *, char *, void *), void *);

    uint64_t map_local(char *, int, int, char *, 
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

    // interfaces in user-defined map and reduce functions
    void add(char *key, int keybytes, char *value, int valuebytes);

    void clear_stat();
    void print_stat(int verb=0, FILE *fp=stdout);

    // output data into file
    // type: 0 for string, 1 for int, 2 for int64_t
    void output(int type=0, FILE *fp=stdout, int format=0);

private:
    // configuable parameters
    int kvtype, ksize, vsize;
    int blocksize;
    int nmaxblock;
    int maxmemsize;
    int outofcore;

    int commmode;
 
    int lbufsize;
    int gbufsize;

    std::string tmpfpath;
    int (*myhash)(char *, int);
 
    // MPI Commincator
    MPI_Comm comm;
    int me, nprocs, tnum; 

    // Current operation
    OpMode mode;      // -1 for nothing, 0 for map, 1 for map_local, 2 for reduce;
    
    // thread private data
    int *blocks;      // current block id for each threads, used in map and reduce
    uint64_t *nitems;

    // data object
    DataObject *data;

    // the communicator for map
    Communicator *c;

    // input file list
    std::vector<std::string> ifiles;

    // private functions
    void init();
    void tinit(int); // thread initialize
    void disinputfiles(const char *, int, int);
    void getinputfiles(const char *, int, int);

    struct Block
    {
      uint64_t nvalue;
      uint64_t mvbytes;
      int      *soffset;
      int      s_off;
      char     *voffset;
      int      v_off;
      int      bid;
      Block    *next;
    };

    struct Unique
    {
      char *key;              // key pointer
      int  keybytes;          // key bytes
      uint64_t nvalue;        // value count
      uint64_t mvbytes;       // multi-value bytes
      int  *soffset;          // start offset
      char *voffset;          // value offset
      Block *blocks;          // block pointer
      Unique *next;           // next pointer
    };

    int nbucket;

    inline void checkbuffer(int, char **, int *, Spool*);
    
    int findukey(Unique **, int, char *, int, Unique **, Unique **pre=NULL);

    void unique2kmv(Unique **, KeyValue *, KeyMultiValue *);
    void unique2tmp(Unique **, KeyValue *, DataObject *, int);
    void mergeunique(Unique **, DataObject *, KeyMultiValue *);

    void unique2kmv_keyonly(Spool *, KeyMultiValue *);
};//class MapReduce

// check if we need add another block
void MapReduce::checkbuffer(int bytes, char **buf, int *off, Spool *pool){
  //if(bytes > pool->getblocksize())
  //  LOG_ERROR("Error: memory requirement %d is larger than block size %d!\n", bytes, pool->getblocksize()); 

  int blocksize = pool->getblocksize();
  if(*off+bytes > blocksize){
    //memset(*buf+*off, 0, blocksize-*off);
    *buf = pool->addblock();
    *off = 0;
  }
}

}//namespace

#endif
