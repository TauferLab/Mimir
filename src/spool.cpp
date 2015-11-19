#include <stdio.h>
#include <stdlib.h>
#include "spool.h"
#include "log.h"

using namespace MAPREDUCE_NS;

Spool::Spool(int _blocksize,int _maxblocks){
  blocksize = _blocksize*UNIT_SIZE;
  maxblocks = _maxblocks;

  blocks = new char*[maxblocks];
  for(int i = 0; i < maxblocks; i++) blocks[i]=NULL;

  nblock = 0;
}

Spool::~Spool(){
  delete [] blocks;
  for(int i = 0; i < maxblocks; i++) {
    if(blocks[i]) free(blocks[i]);
  }
}

char* Spool::addblock(){
  if(nblock >= maxblocks) 
    LOG_ERROR("Error: Spool is overflow! max block count=%d\n", maxblocks);
  blocks[nblock] = (char*)malloc(blocksize);
  nblock++;
  return blocks[nblock-1];
}

