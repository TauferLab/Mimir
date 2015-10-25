#ifndef DATA_OBJECT_H
#define DATA_OBJECT_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <string>

namespace MAPREDUCE_NS {

class DataObject{
public:
  int nitem;       // item count
  int nblock;      // block count
  int blocksize;   // block size
  int maxblock;    // max block
  int maxmemsize;  // max memory size

  // interfaces  

  DataObject(int, int a1=1, int a2=1, int a3=1, int a4=0, 
    std::string a5=std::string(""));
  ~DataObject();

  // acquire and release a block
  int  acquireblock(int);
  void releaseblock(int);

  // add an empty block
  int addblock();
  // add an block with data
  int addblock(char *, int);

  // get block left space
  int getblockspace(int);

  // get offset of block tail
  int getblocktail(int);
  
  // get bytes from a block
  int getbytes(int, int, char **);
  // add bytes to a block
  int addbytes(int, char *, int);
 
private:
  int findbuffer();

protected:
  int datatype;    // 0 for bytes, 1 for kv, 2 for kmv

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
  int     nbuf;  // current buffer number
  int     maxbuf;

  std::string filename;
  int outofcore;

  //Block   *headers;
  //char    **blocks;
};
 
}

#endif
