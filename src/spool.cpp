#include <stdio.h>
#include <stdlib.h>
#include "spool.h"
#include "log.h"
#include "const.h"
#include "memory.h"

using namespace MIMIR_NS;

Spool::Spool(int _blocksize,int _maxblocks){
  blocksize = _blocksize;
  maxblocks = _maxblocks;

  blocks = new char*[maxblocks];
  for(int i = 0; i < maxblocks; i++) blocks[i]=NULL;

  nblock = 0;

  mem_bytes=0;
}

Spool::~Spool(){
  for(int i = 0; i < maxblocks; i++) {
    if(blocks[i]) mem_aligned_free(blocks[i]);
  }
  delete [] blocks;
}

