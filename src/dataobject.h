#ifndef DATA_OBJECT_H
#define DATA_OBJECT_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>
#include <string>

namespace MAPREDUCE_NS {

// Datatype
enum DataType{ByteType, KVType, KMVType};

class DataObject{
public:
  int nblock;      // block count

  // interfaces  
  DataObject(DataType,
    int blocksize=1, 
    int maxblock=4, 
    int memsize=4, 
    int outofcore=0, 
    std::string a5=std::string(""),
    int threadsafe=1);

  virtual ~DataObject();

  // acquire and release a block
  int  acquireblock(int);
  void releaseblock(int);

  // add an empty block
  int addblock();
  // add an block with data
  int addblock(char *, int);
  
  // add data into a block
  int adddata(int, char *, int);


  int getblockspace(int blockid){
    return (blocksize - blocks[blockid].datasize);
  }

  void clear(){
    if(outofcore){
      for(int i = 0; i < nblock; i++){
        std::string filename;
        getfilename(i, filename);
        remove(filename.c_str());
      }
    }
    nblock=0;
  }

  int getblocktail(int blockid){
    return blocks[blockid].datasize;
  }

  int getblocksize(){
    return blocksize;
  }
  
  // get bytes from a block
  int getbytes(int, int, char **);

  // add bytes to a block
  int addbytes(int, char *, int);

  // get data type
  DataType getDatatype(){
    return datatype;
  }

  char *getblockbuffer(int blockid){
    int bufferid = blocks[blockid].bufferid; 
    return buffers[bufferid].buf;
  }

  void setblockdatasize(int blockid, int datasize){
    blocks[blockid].datasize = datasize;
  }

  // print out the bytes data
  virtual void print(int type = 0, FILE *fp=stdout, int format=0);
 
private:
  int acquirebuffer(int);
  void getfilename(int, std::string &);

public:
  DataType datatype;    // 0 for bytes, 1 for kv, 2 for kmv

  // information of block
  struct Block{
    int       datasize;   // datasize in this block
    int       bufferid;   // buffer id
  };
 
  // information of buffer
  struct Buffer{
    char *buf;        // buffer pointer
    int blockid;      // block id
    int  ref;         // uses reference
  };

  Block  *blocks;
  Buffer *buffers;
  int     nbuf;
  int     maxbuf;

  int nitem;       // item count
  int blocksize;   // block size
  int maxblock;    // max block
  int maxmemsize;  // max memory size

  // out of core file name
  int id;
  std::string filepath;
  //std::string filename;
  int outofcore;   // out of core

  omp_lock_t lock_t;

  int threadsafe;  // multi-thread safe

public:
  static int oid;    // data object id
};
 
}

#endif
