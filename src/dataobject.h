/**
 * @file   dataobject.h
 * @Author Tao Gao (taogao@udel.edu)
 * @date   September 1st, 2016
 * @brief  This file provides interfaces to handle data objects.
 *
 * Detail description.
 * \todo 1. Out-of-core sopport isn't tested.
 * \todo 2. Multithreading support isn't tested.
 * \todo 3. Out-of-core support is deleted.
 */
#ifndef DATA_OBJECT_H
#define DATA_OBJECT_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <omp.h>
#include <string>

namespace MAPREDUCE_NS {

/// Datatype
enum DataType{ByteType, KVType, KMVType};

class DataObject{
public:
  /**
    Constructor function.

    @param[in]  type data type
    @param[in]  blocksize page size
    @param[in]  maxblock maximum number of pages
    @param[in]  memsize maximum memory size
    @param[in]  outofcore if support outofcore
    @param[in]  tmpdir temp directory to exchange intermedia data
    @param[in]  threadsafe support thread safety?
    @return MapReduce object pointer
  */
  DataObject(DataType type,
    int64_t blocksize=1, 
    int maxblock=4, 
    int memsize=4, 
    int outofcore=0, 
    std::string tmpdir=std::string(""),
    int threadsafe=1);

  /**
    Destructor function.
  */
  virtual ~DataObject();

  /**
    Acquire a block. If the block isn't in-memory, ensure it's in-memmory after invoking this function.

    @param[in]  blockid block id
    @return 0 if success, otherwise error. 
  */
  int  acquire_block(int blockid){
    return 0;
  }

  /**
    Release a block. If the memory isn't enough, this block may be spilled into disk.

    @param[in]  blockid block id
    @return no return
  */ 
  void release_block(int blockid){
  }

  /**
    Delete a block. Free the buffer of this block. 
    Note: this function doesn't support out-of-core and multithreading currently.

    @param[in]  blockid block id
    @return no return
  */ 
  void delete_block(int blockid);

  /**
    Add en empty block.

    @return added block id
  */  
  int add_block();


  void clear(){
    if(outofcore){
      for(int i = 0; i < nblock; i++){
        std::string filename;
        _get_filename(i, filename);
        remove(filename.c_str());
      }
    }
    nblock=0;
  }

  /**
    Output DataObject. 

    @param[in] type output type
    @param[in] fp output file pointer, also can be stdout, stderr
    @param[in] format output format
    @return added block id
  */
  virtual void print(int type = 0, FILE *fp=stdout, int format=0); 

  /**
    Set key and value size for FixedKV type

    @param[in] _ksize key size
    @param[in] _vsize value size
    @return no return
  */
  void setKVsize(int _ksize, int _vsize){
    ksize = _ksize;
    vsize = _vsize;
  }

  /**
    Get data size in a page.

    @param[in] blockid block id
    @return data size in this page
  */
  int getdatasize(int blockid){
    return blocks[blockid].datasize;
  }

  /**
    Get buffer pointer of a page.

    @param[in] blockid block id
    @return buffer pointer
  */ 
  char *getblockbuffer(int blockid){
    int bufferid = blocks[blockid].bufferid; 
    return buffers[bufferid].buf;
  }

  /**
    Set data size of a page.

    @param[in] blockid block id
    @param[in] data size in this page
    @return no return
  */ 
  void setblockdatasize(int blockid, int datasize){
    blocks[blockid].datasize = datasize;
  }

  uint64_t gettotalsize(){
    totalsize=0;
    for(int i=0;i<nblock;i++)
      totalsize+=blocks[i].datasize;
    return totalsize;
  }

  DataType datatype;    ///< data type
  int ksize, vsize;     ///< key and value size
  int nblock;           ///< number of page
  int64_t blocksize;    ///< page size
  uint64_t totalsize;   ///< datasize
  uint64_t maxmemsize;  ///< maximum memory size

//protected:
public:
  void _get_filename(int, std::string &);

  int            id;            ///< id of data object
  int          nbuf;            ///< number of buffer
  int         nitem;            ///< number of item in this data object
  int      maxblock;            ///< maximum number of page
  int        maxbuf;            ///< maximum number of buffer
  std::string filepath;         ///< intermediate file path
  int outofcore;                ///< if support out-of-core
  int threadsafe;               ///< if support thread safety
  int ref;                      ///< reference counter
  omp_lock_t lock_t;            ///< lock for multithreading

  struct Block{
    uint64_t  datasize;   
    int       bufferid; 
  };
 
  struct Buffer{
    char    *buf; 
    int threadid;
    int  blockid; 
    int      ref;
  };

  Block  *blocks;
  Buffer *buffers;

public:
  static int object_id;
  static void addRef(DataObject *);
  static void subRef(DataObject *);
};
 
}

#endif
