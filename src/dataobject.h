#ifndef DATA_OBJECT_H
#define DATA_OBJECT_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <omp.h>
#include <string>

namespace MAPREDUCE_NS {

// Datatype
enum DataType{ByteType, KVType, KMVType};

class DataObject{
public:
  /**** Create and Destory DataObject****/
  DataObject(DataType,
    int64_t blocksize=1, 
    int maxblock=4, 
    int memsize=4, 
    int outofcore=0, 
    std::string a5=std::string(""),
    int threadsafe=1);

  virtual ~DataObject();

  /**** Core Interfaces ****/
  int  acquire_block(int);
  void release_block(int);
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

  virtual void print(int type = 0, FILE *fp=stdout, int format=0); 

  /**** Set and Get Information ****/
  void setKVsize(int _ksize, int _vsize){
    ksize = _ksize;
    vsize = _vsize;
  }

  //uint64_t getfreespace(int blockid){
  //  return (blocksize - blocks[blockid].datasize);
  //}

  int getdatasize(int blockid){
    return blocks[blockid].datasize;
  }

  char *getblockbuffer(int blockid){
    int bufferid = blocks[blockid].bufferid; 
    return buffers[bufferid].buf;
  }

  void setblockdatasize(int blockid, int datasize){
    blocks[blockid].datasize = datasize;
  }

  DataType datatype;    // 0 for bytes, 1 for kv, 2 for kmv
  int ksize, vsize;
  int nblock;
  int64_t blocksize;   // block size
  uint64_t maxmemsize;  // max memory size

protected:
  void _get_filename(int, std::string &);

  // Internal state
  int            id;
  int          nbuf;
  int         nitem;     // item count
  int      maxblock;     // max block
  int        maxbuf;
  std::string filepath;
  int outofcore, threadsafe, ref;
  omp_lock_t lock_t;

  // Internal Structure
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
