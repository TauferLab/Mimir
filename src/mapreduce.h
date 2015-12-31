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

enum KVType{GeneralKV, StringKV, FixedKV, StringKeyOnly};

enum OpMode{NoneMode, MapMode, MapLocalMode, ReduceMode};

class MapReduce {
public:
    MapReduce(MPI_Comm);
    ~MapReduce();

    void setKVtype(enum KVType _kvtype, int _ksize=-1, int _vsize=-1){
      kvtype = _kvtype;
      ksize = _ksize;
      vsize = _vsize;
    }

    void setBlocksize(int _blocksize){
      blocksize = _blocksize;
    }

    void setMaxblocks(int _nmaxblock){
      nmaxblock = _nmaxblock;
    }

    void setMaxmem(int _maxmemsize){
      maxmemsize = _maxmemsize;
    }

    void setTmpfilePath(const char *_fpath){
      tmpfpath = std::string(_fpath);
    }

    void setOutofcore(int _flag){
      outofcore = _flag;
    }

    void setLocalbufsize(int _lbufsize){
      lbufsize = _lbufsize;
    }
 
    void setGlobalbufsize(int _gbufsize){
      gbufsize = _gbufsize;
    }

    void setCommMode(int _commmode){
      commmode = _commmode;
    }

    void sethash(int (*_myhash)(char *, int)){
      myhash = _myhash;
    }

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

    // convert KV object to KMV object
    //   input must be KV obejct
    //   output must be KMV object
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
      int       id;
      int       nvalue;
      int       mvbytes;
      int      *soffset;
      char     *voffset;
      int       s_off;
      int       v_off;
      int       blockid;
      Block    *next;
    };

    struct Unique
    {
      char    *key;
      int      keybytes;
      int      nvalue;
      int      mvbytes;
      int     *soffset;
      char    *voffset;
      Block   *blocks;
      Unique  *next;
    };

    struct KV_Block_info
    {
      Spool *hid_pool;
      Spool *pos_pool;
      Spool *len_pool;
      int   kv_num;
    };

    // check if a buffer is ok
    void checkbuffer(int, char **, int *, Spool*);
    int findukey(Unique **, int, char *, int, Unique **, Unique **pre=NULL);

    void preprocess(KeyValue *, KeyValue *tkv);
    void merge(DataObject *, KeyMultiValue *, Spool *);
};//class MapReduce


}//namespace

#endif
