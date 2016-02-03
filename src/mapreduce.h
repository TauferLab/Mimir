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

#include "config.h"

namespace MAPREDUCE_NS {

enum KVType{GeneralKV, StringKV, FixedKV, StringKeyOnly};

enum OpMode{NoneMode, MapMode, MapLocalMode, ReduceMode};

class MultiValueIterator;

class MapReduce {
public:
    MapReduce(MPI_Comm);
    MapReduce(const MapReduce &mr);
    ~MapReduce();

    void set_KVtype(enum KVType _kvtype, int _ksize=-1, int _vsize=-1){
      kvtype = _kvtype;
      ksize = _ksize;
      vsize = _vsize;
    }

    void set_blocksize(int _blocksize){
      blocksize = _blocksize;
    }

    void set_maxblocks(int _nmaxblock){
      nmaxblock = _nmaxblock;
    }

    void set_maxmem(int _maxmemsize){
      maxmemsize = _maxmemsize;
    }

    void set_filepath(const char *_fpath){
      tmpfpath = std::string(_fpath);
    }

    void set_outofcore(int _flag){
      outofcore = _flag;
    }

    void set_localbufsize(int _lbufsize){
      lbufsize = _lbufsize;
    }
 
    void set_globalbufsize(int _gbufsize){
      gbufsize = _gbufsize;
    }

    void set_commmode(int _commmode){
      commmode = _commmode;
    }

    void set_hash(int (*_myhash)(char *, int)){
      myhash = _myhash;
    }

    // map and reduce interfaces
    uint64_t map(void (*mymap)(MapReduce *, void *), void *ptr=NULL);
    uint64_t map_local(void (*mymap)(MapReduce *, void *), void*ptr=NULL);

    uint64_t map(char *, int, int, char *, 
      void (*mymap) (MapReduce *, char *, void *), void *ptr=NULL);

    uint64_t map_local(char *, int, int, char *, 
      void (*mymap) (MapReduce *, char *, void *), void *ptr=NULL);

    uint64_t map(char *, int, int, 
      void (*mymap) (MapReduce *, const char *, void *), void *ptr=NULL);

    uint64_t map_local(char *, int, int, 
      void (*mymap) (MapReduce *, const char *, void *), void *ptr=NULL);


    uint64_t map(MapReduce *, 
      void (*mymap) (MapReduce *, char *, int, char *, int, void *), void *ptr=NULL);

    uint64_t map_local(MapReduce *,
      void (*mymap) (MapReduce *, char *, int, char *, int, void *), void * ptr=NULL);

    uint64_t reduce(void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*), int compress=0, void* ptr=NULL);

    // convert KV object to KMV object
    //   input must be KV obejct
    //   output must be KMV object
    //uint64_t convert();

    void scan(void (myscan)(char *, int, char *, int ,void *), void * ptr=NULL);

    // interfaces in user-defined map and reduce functions
    void add(char *key, int keybytes, char *value, int valuebytes);

    void init_stat();
    void show_stat(int verb=0, FILE *fp=stdout);

    // output data into file
    // type: 0 for string, 1 for int, 2 for int64_t
    void output(int type=0, FILE *fp=stdout, int format=0);

    double get_timer(int id);

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
      int     start_blockid;
      int     start_offset;
      int     end_blockid;
      int     end_offset;
      int     start_set;
      int     end_set;
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

    uint64_t get_global_KVs(){
      return global_kvs_count;
    }
    
    uint64_t get_local_KVs(){
      return local_kvs_count;
    }

private:
    uint64_t global_kvs_count, local_kvs_count;

    uint64_t send_bytes, recv_bytes;
    uint64_t max_mem_bytes;

private:

    // parameters for binding threads
    int bind_thread;
    int show_binding;
    int procs_per_node;
    int thrs_per_proc;

    // configuable parameters
    int kvtype, ksize, vsize;
    int nmaxblock;
    int maxmemsize;
    int outofcore;

    int commmode;  
 
    int lbufsize;  // KB
    int gbufsize;  // MB
    int blocksize; // MB

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
    //void init();

    void get_default_values();
    void bind_threads();

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


    int thashmask, uhashmask;
 
    int ualign, ualignm;
    int nbucket;
    int ukeyoffset;

    MapReduce::Unique* findukey(Unique **, int, char *, int, Unique *&pre);

    void unique2set(UniqueInfo *);
    
    int  kv2unique(int, KeyValue *, UniqueInfo *, DataObject *);
    void unique2mv(int, KeyValue *, Partition *, UniqueInfo *, DataObject *);
    void mv2kmv(DataObject *,UniqueInfo *,
      void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*), void *);
    void unique2kmv(int, KeyValue *, UniqueInfo *, DataObject *, 
      void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*), void*);

    uint64_t convert_small(KeyValue *, void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*), void*);
    uint64_t convert_median(KeyValue *, void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*));
    uint64_t convert_large(KeyValue *, void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*));
};//class MapReduce

class MultiValueIterator{
public:
  MultiValueIterator(int _nvalue, int *_valuebytes, char *_values, int _kvtype, int _vsize){
    nvalue=_nvalue;
    valuebytes=_valuebytes;
    values=_values;
    kvtype=_kvtype;
    vsize=_vsize;
    
    mode=0;

    Begin();
  }

  MultiValueIterator(MapReduce::Unique *_ukey, DataObject *_mv){
    ukey=_ukey;
    mv=_mv;
    pset=NULL;

    nvalue=ukey->nvalue;

    mode=1;

    Begin();
  }

  ~MultiValueIterator(){
  }

  int Done(){
    return isdone;
  }

  void Begin(){
    ivalue=0;
    value_start=0;
    isdone=0;
    if(ivalue >= nvalue) isdone=1;
    else{
      if(mode==1){
         pset=ukey->firstset;

         mv->acquireblock(pset->pid);
         char *tmpbuf = mv->getblockbuffer(pset->pid);
         pset->soffset = (int*)(tmpbuf + pset->s_off);
         pset->voffset = tmpbuf + pset->v_off;
         
         valuebytes=pset->soffset;
         values=pset->voffset;

         value_end=pset->nvalue;
      }
      value=values;
      if(kvtype==0) valuesize=valuebytes[ivalue-value_start];
      else if(kvtype==1) valuesize=strlen(value)+1;
      else if(kvtype==2) valuesize=vsize;
    }
  }

  void Next(){
    ivalue++;
    if(ivalue >= nvalue) {
      isdone=1;
      if(mode==1 && pset){
         mv->releaseblock(pset->pid);
      }
    }else{
      if(mode==1 && ivalue >=value_end){
        value_start += pset->nvalue;
        mv->releaseblock(pset->pid);
        pset=pset->next;

        mv->acquireblock(pset->pid);
        char *tmpbuf = mv->getblockbuffer(pset->pid);
        pset->soffset = (int*)(tmpbuf + pset->s_off);
        pset->voffset = tmpbuf + pset->v_off;
        valuebytes=pset->soffset;
        values=pset->voffset;
        value_end+=pset->nvalue;
      }
      value+=valuesize;
      if(kvtype==0) valuesize=valuebytes[ivalue-value_start];
      else if(kvtype==1) valuesize=strlen(value)+1;
      else if(kvtype==2) valuesize=vsize;
    }
  }

  const char *getValue(){
    return value;
  }

  int getSize(){
    return valuesize;
  }

  int getCount(){
    return nvalue;
  }

private:
  int mode,kvtype,vsize;

  int  nvalue;
  int *valuebytes;
  char *values;

  int ivalue;
  int value_start;
  int value_end;

  DataObject *mv;
  MapReduce::Unique *ukey;
  MapReduce::Set *pset;

  int isdone;
  char *value;
  int valuesize;
};

}//namespace

#endif
