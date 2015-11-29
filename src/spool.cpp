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
  for(int i = 0; i < maxblocks; i++) {
    if(blocks[i]) free(blocks[i]);
  }
  delete [] blocks;
}

