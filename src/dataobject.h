#ifndef DATA_OBJECT_H
#define DATA_OBJECT_H

#include <stdio.h>
#include <stdlib.h>

namespace MAPREDUCE_NS {

class DataObject{
public:
  int nitem;
  int nblock;
  int maxblock;
  int blocksize;

  // interfaces  

  DataObject(int, int);
  ~DataObject();

  int add_block(void);

  char *acquire_block(int);
  void release_block(int);

private:
  struct Block{
    int item_tail;
    int remain_bytes;
    int flag;
  };

  Block   *headers;
  char    **blocks;
};
 
}

#endif
