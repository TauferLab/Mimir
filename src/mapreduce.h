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
#include <string.h>
#include <mpi.h>
#include <string>
#include <vector>

#include "config.h"
#include "const.h"

namespace MIMIR_NS {

class MapReduce;
class Communicator;
class DataObject;
class KeyValue;
class Spool;
class MultiValueIterator;

/// hash callback
typedef  uint32_t (*UserHash)(char *, int);

/// map callback to init KVs
typedef void (*UserInitKV)(MapReduce *, void *);

/// map callback to map files
typedef void (*UserMapFile) (MapReduce *, char *, void *);

/// map callback to map KVs
typedef void (*UserMapKV) (MapReduce *, char *, int, char *, int, void *);

/// reduce callback
typedef void (*UserReduce)(MapReduce *, char *, int,  void*);

/// combiner callback
typedef void (*UserCombiner)(MapReduce *, 
  char *, int, char *, int, char *, int, void*);

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
      init_key_value  load KVs from memory (e.g. in-situ workload)

      @param[in]  myinit user-defined init function
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
      char *seperator, UserMapFile mymap, void *ptr=NULL, int comm=1);

    /**
      Map function with MapReduce object as input.

      @param[in]  mr MapReduce Object pointer
      @param[in]  mymap user-defined map function
      @param[in]  ptr   user-defined pointer (default: NULL)
      @param[in]  comm  communication or not (default: 1)
      @return output <key,value> count
    */
    uint64_t map_key_value(MapReduce *mr,
      UserMapKV mymap,  void *ptr=NULL, int comm=1);

    /**
      Reduce function.

      @param[in]  myreduce user-defined reduce function
      @param[in]  ptr user-defined pointer (default: NULL)
      @return output <key,value> count
    */
    uint64_t reduce(UserReduce _myreduce, void* ptr=NULL);

    /**
      Scan function.

      @param[in]  myscan user-defined scan function
      @param[in]  ptr user-defined pointer (default: NULL)
    */
    void scan(UserScan myscan, void * ptr=NULL);

    /**
      Add <key,value>. This function only can be invoked in callbacks.

      @param[in]  key key pointer
      @param[in]  keybytes key size
      @param[in]  value value pointer
      @param[in]  valubytes value size
      @return nothing
    */
    void add_key_value(char *key, int keybytes,
        char *value, int valuebytes);

    void update_key_value(char *key, int keybytes,
        char *value, int valuebytes);

    void output(FILE *fp, ElemType key, ElemType val);

    void set_key_length();
    void set_value_length();

    void set_combiner(UserCombiner combiner){
        mycombiner = combiner;
    }


    static void output_stat(const char *filename);

  //static void print_memsize();

  /**
    Set <key,value> type. The KV type can be GeneralKV, StringKV, FixedKV.

    @param[in]  _kvtype <key,value> type (GeneralKV, StringKV, FixedKV)
    @param[in]  _ksize  key size (only valid for FixedKV type)
    @param[in]  _vsize  value size (only valid for Fixed type)
  */
#if 0
  void set_KVtype(enum KVType _kvtype, int _ksize=-1, int _vsize=-1){
    kvtype = _kvtype;
    ksize = _ksize;
    vsize = _vsize;
    if(kvtype==FixedKV || kvtype==StringKFixedV){
      maxvaluebytes=vsize;
    }else{
      maxvaluebytes=MAX_VALUE_SIZE;
    }
  }
  void set_blocksize(const char *_blocksize){
    blocksize = _stringtoint(_blocksize);
  }
  void set_nbucket(int _estimate, int _nbucket, int _factor){
    estimate = _estimate;
    nbucket = (int)pow(2,_nbucket);
    nset = nbucket;
    factor = _factor;
  }
  void set_inputsize(const char *_inputsize){
    inputsize = _stringtoint(_inputsize);
  }
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
  void set_hash(int64_t (*_myhash)(char *, int)){
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
#endif

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
  enum OpPhase{NonePhase, MapPhase, LocalMapPhase, ReducePhase, ScanPhase};

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

  struct UniqueCPS
  {
    //char      *key;
    //int        keybytes;
    //char      *value;
    //int        valuebytes;
    UniqueCPS *next;
  };

  /// \brief UniqueInfo is used to convert KVs to KMVs
  struct UniqueInfo
  {
    int        nunique;       ///< number of unique key
    int        nset;          ///< number of set
    char       *ubuf;
    char       *ubuf_end;
    Unique   **ubucket;       ///< hash bucket
    Spool     *unique_pool;   ///< memory pool of unique keys
    Spool     *set_pool;      ///< memory pool of sets
  };

private:
  // Statatics Variables
    //uint64_t global_kvs_count, local_kvs_count;
private:
    // Configuable parameters
    //enum KVType kvtype;        ///< KV type
    //int ksize, vsize;          ///< length of key or value in KV
    //int commmode;              ///< communication mode: a2a or p2p
    //int nmaxpage;              ///< max number of pages
    //int nbucket,nset;          ///< number of bucket
    //int maxstringbytes;        ///< maximum bytes of string
    //int64_t comm_buf_size;     ///< communication buffer size
    //int64_t data_page_size;    ///< the page size
    //int64_t input_buf_size;    ///< input buffer size

    MPI_Comm comm;                   ///< MPI communicator
    int me,nprocs;                   ///< MPI communicator information

    OpPhase phase;                   ///< operation mode
    KeyValue  *kv;                   ///< KV container
    Communicator *c;                 ///< communicator
    std::vector<std::pair<std::string,int64_t> > ifiles;

    UserHash     myhash;             ///< user-define hash function
    UserCombiner mycombiner;         ///< user-defined combiner function

public:
    char *newkey, *newval;
    int newkeysize, newvalsize;
    void *myptr;

private:

    ////////////////// internal functions //////////////////////

    // Get default values
    void _get_default_values();

    // Get number of KVs
    uint64_t _get_kv_count();

    // Convert string to int64_t
    int64_t _convert_to_int64(const char *);

    // Get input file list
    void _get_input_files(const char *, int, int);

    // distribute input file list
    void _dist_input_files(const char *, int, int);


    //int ukeyoffset;
    //int 

    //uint64_t nitem;
    //int       blockid; 

    //UniqueCPS *cur_ukey;  ///< number of unique key
    //UniqueInfo *u;
    //User
    //void *ptr;


    //MapReduce::Unique* _findukey(Unique **, int, char *, int, Unique *&pre, int cps=0);
    //void _unique2set(UniqueInfo *);

    //int  _kv2unique(int, KeyValue *, UniqueInfo *, DataObject *,
    //UserReduce myreduce, void *, int shared=1);

    //void _unique2mv(int, KeyValue *, Partition *, UniqueInfo *, DataObject *, int shared=1);
    //void _unique2kmv(int, KeyValue *, UniqueInfo *, DataObject *,
    //UserReduce myreduce, void*, int shared=1);

    //void _mv2kmv(DataObject *,UniqueInfo *,int,
    //UserReduce myreduce, void *);

  //uint64_t _cps_kv2unique(UniqueInfo *u, char *key, int keybytes, char *value, int valuebytes, UserBiReduce, void *);
  //uint64_t _cps_unique2kv(UniqueInfo *u);

  //uint64_t _reduce(KeyValue *, UserReduce, void*);
  //uint64_t _reduce_compress(KeyValue *, UserReduce, void*);

};//class MapReduce

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

}//namespace

#endif
