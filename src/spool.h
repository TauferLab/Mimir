#ifndef SPOOL_H
#define SPOOL_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "log.h"
#include "config.h"

namespace MAPREDUCE_NS {

class Spool{
public:
  Spool(int,int _maxblocks=1024);
  ~Spool();

  char *addblock(){
    blocks[nblock] = (char*)malloc(blocksize);
#if SAFE_CHECK
    if(blocks[nblock]==NULL){
      LOG_ERROR("%s", "Error: malloc memory for block error!\n");
    }
#endif
    return blocks[nblock++];
  }

  int getblocksize(){
    return blocksize;
  }

  char *getblockbuffer(int i){
    return blocks[i];
  }

  void clear(){
    for(int i=0; i<nblock;i++){
      free(blocks[i]);
      blocks[i]=NULL;
    }
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
