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
#include <math.h>
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

#include "stat.h"

namespace MAPREDUCE_NS {

class MapReduce;
class MultiValueIterator;

/// KVType represents KV Type
enum KVType{GeneralKV, StringKV, FixedKV, StringKFixedV, FixedKStringV};

/// User-defined map function to init KV
typedef void (*UserInitKV)(MapReduce *, void *);

/// User-defined map function to map files
typedef void (*UserMapFile) (MapReduce *, char *, void *);

/// User-defined map function to map KV object
typedef void (*UserMapKV) (MapReduce *, char *, int, char *, int, void *);

/// User-defined compress function 
typedef void (*UserCompress)(MapReduce *, char *, int,  MultiValueIterator *iter, int, void*);

/// User-defined reduce function
typedef void (*UserReduce)(MapReduce *, char *, int,  MultiValueIterator *iter, int, void*);

/// User-defined reduce function
typedef void (*UserBiReduce)(MapReduce *, char *, int,  char *, int, char *, int, void*);

/// User-defined scan function
typedef void (*UserScan)(char *, int, char *, int ,void *);

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
    Map function without input.
 
    @param[in]  myinit user-defined map function
    @param[in]  ptr    user-defined pointer (default: NULL)
    @param[in]  comm   with communication or not (default: 1)
    @return output <key,value> count
  */
  uint64_t init_key_value(
    UserInitKV myinit, void *ptr=NULL, int comm=1);
 
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
  uint64_t map_text_file(char *filename, int shared, int recurse, 
    char *seperator, UserMapFile mymap, UserCompress mycompress=NULL, 
    void *ptr=NULL, int compress=0, int comm=1);
  
  /**
    Map function with MapReduce object as input.
 
    @param[in]  mr MapReduce Object pointer
    @param[in]  mymap user-defined map function
    @param[in]  ptr   user-defined pointer (default: NULL)
    @param[in]  comm  communication or not (default: 1)  
    @return output <key,value> count
  */
  uint64_t map_key_value(MapReduce *mr, 
    UserMapKV mymap,  UserCompress mycompress=NULL, \
    void *ptr=NULL, int compress=0, int comm=1);

  /**
    Compress local <key,value>
 
    @param[in]  mycompress user-defined compress function
    @param[in]  ptr        user-defined pointer (default: NULL)
    @param[in]  comm       communication or not (default: 1)
    @return output <key,value> count
  */
  uint64_t compress(UserCompress mycompress, void *ptr=NULL, int comm=1);
 
  /**
    Reduce function.
 
    @param[in]  myreduce user-defined reduce function
    @param[in]  compress if apply compress (default: 0)
    @param[in]  ptr user-defined pointer (default: NULL)
    @return output <key,value> count
  */
  uint64_t reduce(UserReduce _myreduce, int compress=0, 
    void* ptr=NULL);

  /**
    Reduce function.
 
    @param[in]  myreduce user-defined reduce function
    @param[in]  ptr user-defined pointer (default: NULL)
    @return output <key,value> count
  */
  uint64_t reducebykey(UserBiReduce _myreduce, void* ptr=NULL);

  /**
    Scan function.
 
    @param[in]  myscan user-defined scan function
    @param[in]  ptr user-defined pointer (default: NULL)
  */ 
  void scan(UserScan myscan, void * ptr=NULL);

  /**
    Add <key,value>. This function only can be invoked in user-defined map or reduce functions.
 
    @param[in]  key key pointer
    @param[in]  keybytes key size
    @param[in]  value value pointer
    @param[in]  valubytes value size
    @return nothing
  */
  void add_key_value(char *key, int keybytes, 
    char *value, int valuebytes);
  
   /**
    Add <key,value>. This function only can be invoked in user-defined map or reduce functions.
 
    @param[in]  key key pointer
    @param[in]  keybytes key size
    @param[in]  value value pointer
    @param[in]  valubytes value size
    @return nothing
  */ 
  void output(int type=0, FILE *fp=stdout, int format=0);


  //void init_stat();
  static void print_stat(MapReduce *mr, FILE *fp=stdout);

  /**
    Set <key,value> type. The KV type can be GeneralKV, StringKV, FixedKV.
 
    @param[in]  _kvtype <key,value> type (GeneralKV, StringKV, FixedKV)
    @param[in]  _ksize  key size (only valid for FixedKV type)
    @param[in]  _vsize  value size (only valid for Fixed type)
  */
  void set_KVtype(enum KVType _kvtype, int _ksize=-1, int _vsize=-1){
    kvtype = _kvtype;
    ksize = _ksize;
    vsize = _vsize;
  }

  /**
    Set page size. (G,GB,M,MB,K,KB)
 
    @param[in]  _blocksize page size
  */
  void set_blocksize(const char *_blocksize){
    blocksize = _stringtoint(_blocksize);
  }

  /**
    Set hash bucket size.
 
    @param[in]  _estimate uses estimated bucket size
    @param[in] _nbucket  hash bucket size 2^nbucket
    @param[in] _factor   calculate the hash bucket size <key,value>/_factor (only valid if estimate is set)
  */
  void set_nbucket(int _estimate, int _nbucket, int _factor){
    estimate = _estimate;
    nbucket = pow(2,_nbucket);
    nset = nbucket;
    factor = _factor;
  }

  /**
    Set input buffer size. (G,GB,M,MB,K,KB)
 
    @param[in]  _inputsize input buffer size
  */
  void set_inputsize(const char *_inputsize){
    inputsize = _stringtoint(_inputsize);
  }

  /**
    Set thread local buffer size. (G,GB,M,MB,K,KB)
 
    @param[in]  _tbufsize thread buffer size
  */
  void set_threadbufsize(const char* _tbufsize){
    lbufsize = _stringtoint(_tbufsize);
  }

  /**
    Set send buffer size. (G,GB,M,MB,K,KB)
 
    @param[in]  _sbufsize send buffer size
  */
  void set_sendbufsize(const char* _sbufsize){
    gbufsize = _stringtoint(_sbufsize);
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
  void set_commmode(const char* _commmode){
    commmode = -1;
    if(strcmp(_commmode,"a2a")==0)
      commmode = 0;
    else if(strcmp(_commmode, "p2p")==0)
      commmode = 1;
  }
  /**
    Set hash function.
 
    @param[in]  _myhash user-defined hash function
  */
  void set_hash(int (*_myhash)(char *, int)){
    myhash = _myhash;
  }

  uint64_t get_global_KVs(){
    return global_kvs_count;
  }
    
  uint64_t get_local_KVs(){
    return local_kvs_count;
  }

  uint64_t get_unique_count(){
    return nunique;
  }

private:
  friend class MultiValueIterator;

  /** \enum OpMode
   * \brief Operation Mode, used in add_key_value function.
   *
   * NoneMode: add_key_value invoked outside any map/reduce functions. \n
   * MapMode: add_key_value invoked in map function with communication. \n
   * MapLocalMode: add_key_value invoked in map function without communication. \n
   * ReduceMode: add_key_value invoked in reduce function.
   */  
  enum OpMode{NoneMode, MapMode, MapLocalMode, ReduceMode, CompressMode};

  /// \brief The set is the partial KMV within one page
  /// The set is the partial KMV within one pages
  struct Set
  {
    int       myid;    ///< set id
    int       nvalue;  ///< number of values
    int       mvbytes; ///< bytes of multiple values
    int      *soffset; ///< start pointer of value size array
    char     *voffset; ///< start pointer of values
    int       s_off;   ///< offset in the page of value size array
    int       v_off;   ///< offset in the page pf values
    int       pid;     ///< partition id to generate this set
    Set      *next;    ///< Set pointer in the next page
  };

  /// \brief Partition is a range of a KV object 
  struct Partition
  {
    int     start_blockid; ///< start block
    int     start_offset;  ///< start offset
    int     end_blockid;   ///< end block
    int     end_offset;    ///< end offset
    int     start_set;     ///< start set 
    int     end_set;       ///< end set
  };

  /// \brief Unique is used to record unique key
  struct Unique
  {
    char      *key;        ///< key data
    int        keybytes;   ///< key bytes
    int        nvalue;     ///< number of value
    int        mvbytes;    ///< multiple value bytes
    int       *soffset;    ///< start pointer of value size array
    char      *voffset;    ///< start pointer of values
    Set       *firstset;   ///< first set 
    Set       *lastset;    ///< last set
    int        flag;       ///< flag
    Unique    *next;       ///< next key
  };

  /// \brief UniqueInfo is used to convert KVs to KMVs
  struct UniqueInfo
  {
    int        nunique;       ///< number of unique key
    int        nset;          ///< number of set
    Unique   **ubucket;       ///< hash bucket
    Spool     *unique_pool;   ///< memory pool of unique keys
    Spool     *set_pool;      ///< memory pool of sets
  };

private:
  // Environemnt Variables
  int bind_thread;
  int show_binding;
  int procs_per_node;
  int thrs_per_proc;

private:
  // Statatics Variables
  uint64_t global_kvs_count, local_kvs_count, nunique;
private:
  // Configuable parameters
  int kvtype, ksize, vsize;
  int nmaxblock;
  int maxmemsize;
  int outofcore;
  int commmode;  
  int64_t lbufsize;  // KB
  int64_t gbufsize;  // MB
  int64_t blocksize; // MB
  int64_t inputsize; // MB
  std::string tmpfpath;

  int (*myhash)(char *, int);

  int estimate, nbucket, factor, nset;

  int maxstringbytes,maxkeybytes,maxvaluebytes;
private:
  // MPI Commincator
  MPI_Comm comm;
  int me, nprocs, tnum;
  OpMode mode;
  int ukeyoffset; 

struct thread_private_info{
  uint64_t nitem;
  int      block;
};

  thread_private_info *thread_info;

  //uint64_t *nitems;
  //int *blocks;
  DataObject  *data/*,*tmpdata*/;
  Unique *cur_ukey;
  //UserCompress mycompress;
  void *ptr;
  Communicator *c;
  std::vector<std::string> ifiles;

  // internal functions
  void _get_default_values();
  void _bind_threads();

  int64_t _stringtoint(const char *);

  uint64_t _map_master_io(char *, int, int, char *, 
    UserMapFile, UserCompress mycompress, 
    void *ptr=NULL, int compress=0, int comm=1);
  uint64_t _map_multithread_io(char *, int, int, char *, 
    UserMapFile, UserCompress mycompress, 
    void *ptr=NULL, int compress=0, int comm=1);

  void _tinit(int); // thread initialize
  void _disinputfiles(const char *, int, int);
  void _getinputfiles(const char *, int, int);


  MapReduce::Unique* _findukey(Unique **, int, char *, int, Unique *&pre);
  void _unique2set(UniqueInfo *);
    
  int  _kv2unique(int, KeyValue *, UniqueInfo *, DataObject *, 
  void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, int, void*), void *,
    int shared=1);

    
  void _unique2mv(int, KeyValue *, Partition *, UniqueInfo *, DataObject *, int shared=1);
  void _unique2kmv(int, KeyValue *, UniqueInfo *, DataObject *, 
  void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, int, void*), void*, int shared=1);

  void _mv2kmv(DataObject *,UniqueInfo *,int,
    void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, int, void*), void *);

  uint64_t _reduce(KeyValue *, UserReduce, void*);
  uint64_t _reduce_compress(KeyValue *, UserReduce, void*);

  uint64_t _get_kv_count();
#if 0
  {
    local_kvs_count=0;
    for(int i=0; i<tnum; i++) local_kvs_count+=thread_info[i].nitem;
    MPI_Allreduce(&local_kvs_count, &global_kvs_count, 1, MPI_UINT64_T, MPI_SUM, comm);
    TRACKER_RECORD_EVENT(0, EVENT_COMM_ALLREDUCE);
    return  global_kvs_count;
  }
#endif
};//class MapReduce

#if 1
class MultiValueIterator{
public:

  MultiValueIterator(int _nvalue, int *_valuebytes, char *_values, int _kvtype, int _vsize);
  MultiValueIterator(MapReduce::Unique *_ukey, DataObject *_mv, int _kvtype);
  ~MultiValueIterator(){
  }

  int Done(){
    return isdone;
  }
  void Begin();
  void Next();
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
#endif

}//namespace

#if 0
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
#endif
#if 0
  {
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
#endif

#if 0
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
#endif


#endif
