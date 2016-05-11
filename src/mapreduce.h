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

class MapReduce;
class MultiValueIterator;

enum KVType{GeneralKV, StringKV, FixedKV, StringKeyOnly};

typedef void (*UserInitKV)(MapReduce *, void *);
typedef void (*UserMapFile) (MapReduce *, char *, void *);
typedef void (*UserMapKV) (MapReduce *, char *, int, char *, int, void *);
typedef void (*UserReduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*);
typedef void (*UserScan)(char *, int, char *, int ,void *);

class MapReduce {
public:
  MapReduce(MPI_Comm);
  MapReduce(const MapReduce &mr);
  ~MapReduce();

  uint64_t init_key_value(
    UserInitKV _myinit, void *ptr=NULL, int comm=1);
  uint64_t map_text_file(char *, int, int, char *, 
    UserMapFile _mymap, void *ptr=NULL, int comm=1);
  uint64_t map_key_value(MapReduce *, 
    UserMapKV _mymap, void *ptr=NULL, int comm=1);
  uint64_t reduce(UserReduce _myreduce, int compress=0, 
    void* ptr=NULL);
  void scan(UserScan _myscan, void * ptr=NULL);
  void add_key_value(char *key, int keybytes, 
    char *value, int valuebytes);
  void output(int type=0, FILE *fp=stdout, int format=0);
  void init_stat();
  void show_stat(int verb=0, FILE *fp=stdout);

  void set_KVtype(enum KVType _kvtype, int _ksize=-1, int _vsize=-1){
    kvtype = _kvtype;
    ksize = _ksize;
    vsize = _vsize;
  }
  void set_blocksize(int _blocksize){
    blocksize = _blocksize;
  }
  void set_inputsize(int _inputsize){
    inputsize = _inputsize;
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
  void set_threadbufsize(int _tbufsize){
    lbufsize = _tbufsize;
  }
  void set_sendbufsize(int _sbufsize){
    gbufsize = _sbufsize;
  }
  void set_commmode(int _commmode){
    commmode = _commmode;
  }
  void set_hash(int (*_myhash)(char *, int)){
    myhash = _myhash;
  }

  uint64_t get_global_KVs(){
    return global_kvs_count;
  }
    
  uint64_t get_local_KVs(){
    return local_kvs_count;
  }

private:
  friend class MultiValueIterator; 
  enum OpMode{NoneMode, MapMode, MapLocalMode, ReduceMode};

  /**** Structure used for converting ****/
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



private:
  // Environemnt Variables
  int bind_thread;
  int show_binding;
  int procs_per_node;
  int thrs_per_proc;

private:
  // Statatics Variables
  uint64_t global_kvs_count, local_kvs_count;

private:
  // Configuable parameters
  int kvtype, ksize, vsize;
  int nmaxblock;
  int maxmemsize;
  int outofcore;
  int commmode;  
  int lbufsize;  // KB
  int gbufsize;  // MB
  int blocksize; // MB
  int inputsize; // MB
  std::string tmpfpath;
  int (*myhash)(char *, int);

  int nbucket, nset;
private:
  // MPI Commincator
  MPI_Comm comm;
  int me, nprocs, tnum;
  OpMode mode;
  int ukeyoffset; 
  uint64_t *nitems;
  int *blocks;
  DataObject  *data;
  Communicator *c;
  std::vector<std::string> ifiles;

  // internal functions
  void _get_default_values();
  void _bind_threads();

  uint64_t _map_master_io(char *, int, int, char *, 
    void (*mymap) (MapReduce *, char *, void *), void *ptr=NULL, int comm=1);
  uint64_t _map_multithread_io(char *, int, int, char *, 
    void (*mymap) (MapReduce *, char *, void *), void *ptr=NULL, int comm=1);

  void _tinit(int); // thread initialize
  void _disinputfiles(const char *, int, int);
  void _getinputfiles(const char *, int, int);


  MapReduce::Unique* _findukey(Unique **, int, char *, int, Unique *&pre);
  void _unique2set(UniqueInfo *);
    
  int  _kv2unique(int, KeyValue *, UniqueInfo *, DataObject *, 
  void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*), void *,
    int shared=1);

    
  void _unique2mv(int, KeyValue *, Partition *, UniqueInfo *, DataObject *, int shared=1);
  void _unique2kmv(int, KeyValue *, UniqueInfo *, DataObject *, 
  void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*), void*, int shared=1);

  void _mv2kmv(DataObject *,UniqueInfo *,int,
    void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*), void *);

  uint64_t _convert_small(KeyValue *, void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*), void*);
  // FIXME: _convert_media and _convert_large hasn't been implemneted
  uint64_t _convert_media(KeyValue *, void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*), void*);
  uint64_t _convert_large(KeyValue *, void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*), void*);
  uint64_t _convert_compress(KeyValue *, void (*myreduce)(MapReduce *, char *, int, MultiValueIterator *iter, void*), void*);

  uint64_t _get_kv_count(){
    local_kvs_count=0;
    for(int i=0; i<tnum; i++) local_kvs_count+=nitems[i];
    MPI_Allreduce(&local_kvs_count, &global_kvs_count, 1, MPI_UINT64_T, MPI_SUM, comm);
    return  global_kvs_count;
  }
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

  MultiValueIterator(MapReduce::Unique *_ukey, DataObject *_mv, int _kvtype){
    mv=_mv;
    ukey=_ukey;

    nvalue=ukey->nvalue;
    kvtype=_kvtype;
    vsize=mv->vsize;
    pset=NULL;

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
         mv->acquire_block(pset->pid);
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
         mv->release_block(pset->pid);
      }
    }else{
      if(mode==1 && ivalue >=value_end){
        value_start += pset->nvalue;
        mv->release_block(pset->pid);
        pset=pset->next;
        mv->acquire_block(pset->pid);
        char *tmpbuf = mv->getblockbuffer(pset->pid);
        pset->soffset = (int*)(tmpbuf + pset->s_off);
        pset->voffset = tmpbuf + pset->v_off;

        valuebytes=pset->soffset;
        values=pset->voffset;

        value=values;
        value_end+=pset->nvalue;
      }else{
        value+=valuesize;
      }
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
