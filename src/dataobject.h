#ifndef DATA_OBJECT_H
#define DATA_OBJECT_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
    std::string a5=std::string(""));

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

  // get block left space
  int getblockspace(int);

  // get offset of block tail
  int getblocktail(int);

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

  // print out the bytes data
  virtual void print(int type = 0, FILE *fp=stdout, int format=0);
 
private:
  int findbuffer();

protected:
  DataType datatype;    // 0 for bytes, 1 for kv, 2 for kmv

  // information of block
  struct Block{
    int       datasize;
    int       bufferid;
    int       infile;
    int64_t   fileoff;
  };
 
  // information of buffer
  struct Buffer{
    char *buf;
    int blockid;
    int  ref;
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
  std::string filename;
  int outofcore;   // out of core
};
 
}

#endif
