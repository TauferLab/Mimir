#ifndef SPOOL_H
#define SPOOL_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "config.h"

namespace MAPREDUCE_NS {

class Spool{
public:
  Spool(int,int _maxblocks=1024);
  ~Spool();

  char *addblock(){
    //if(nblock >= maxblocks) 
    //  LOG_ERROR("Error: Spool is overflow! max block count=%d\n", maxblocks);
    blocks[nblock] = (char*)malloc(blocksize);
    memset(blocks[nblock], 0, blocksize);
    nblock++;
    return blocks[nblock-1];
  }

  int getblocksize(){
    return blocksize;
  }

  char *getblockbuffer(int i){
    return blocks[i];
  }

  void clear(){
    for(int i=0; i<nblock;i++) free(blocks[i]);
    nblock=0;
  }

public:
  int nblock;

private:
  char **blocks;
  int blocksize;
  int maxblocks;
};

}

#endif
