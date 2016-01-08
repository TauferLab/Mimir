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

    //
    // Data Structure used in convert function
    // KV_Block_info: record information in KV block
    // UniqueList: whole unique list information
    // Unique: unique item
    // UKeyBlock: 

#if 0    
    struct KV_Block_info
    {
      int   *tkv_off;
      int   *tkv_size;
      Spool *hid_pool;
      Spool *off_pool;
      int   nkey;
    };
#endif

    struct Set
    {
      int       myid;
      int       nvalue;
      int       mvbytes;
      int      *soffset;
      char     *voffset;
      int       s_off;
      int       v_off;
      int       pid;        // Partition ID
      Set      *next;
    };

    struct Partition
    {
      int     last_block;
      int     last_offset;
      int     next_set;
      Partition *next;
    };

    struct Unique
    {
      char      *key;
      int        keybytes;
      int        nvalue;
      int        mvbytes;
      int       *soffset;
      char      *voffset;
      Set       *firstset;
      Set       *lastset;
      int        flag;
      Unique    *next;
    };

    struct UniqueInfo
    {
      int        nunique;
      int        nset;
      Unique   **ubucket;
      Spool     *unique_pool;
      Spool     *set_pool;
    };

    int thashmask, uhashmask;
    int kalign, valign, talign, ualign;
    int kalignm, valignm, talignm, ualignm;

    int nbucket;
    int ukeyoffset;

    MapReduce::Unique* findukey(Unique **, int, char *, int, Unique *&pre);

    void unique2set(UniqueInfo *);
    
    void  kv2unique(int, KeyValue *, UniqueInfo *, Partition *);
    void unique2mv(int, KeyValue *, UniqueInfo *, Partition *, DataObject *);
    void mv2kmv(DataObject *, UniqueInfo *, KeyMultiValue *);
    void unique2kmv(KeyValue *, UniqueInfo *, KeyMultiValue *);

    uint64_t convert_small(KeyValue *, KeyMultiValue *);
    uint64_t convert_median(KeyValue *, KeyMultiValue *);
    uint64_t convert_large(KeyValue *, KeyMultiValue *);

    // check if a buffer is ok
    //void checkbuffer(int, char **, int *, Spool*);

    //void  buildkvinfo(KeyValue *, KV_Block_info *);
    //void  kv2tkv(KeyValue *, KeyValue *, KV_Block_info *);

    //void  unique2tmp_first();
    //void  unique2tmp();

    //void  unique2kmv(UniqueList *, Partition *p, KeyMultiValue *);
    //void  merge(DataObject *, KeyMultiValue *, Spool *);

    //void preprocess(KeyValue *, KeyValue *tkv);
};//class MapReduce


}//namespace

#endif
