/**
 * @file   mapreduce.h
 * @Author Tao Gao (taogao@udel.edu)
 * @date   September 1st, 2016
 * @brief  This file provides interfaces to application programs.
 *
 * This file includes two classes: MapReduce and MultiValueIter.
 */
#ifndef SPOOL_H
#define SPOOL_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include "log.h"
//#include "config.h"

#include "const.h"
#include "memory.h"

namespace MAPREDUCE_NS {

class Spool{
public:
  Spool(int,int _maxblocks=1024);
  ~Spool();

  char *add_block(){
    char *buf = (char*)mem_aligned_malloc(MEMPAGE_SIZE, blocksize);
    mem_bytes += blocksize;

#if SAFE_CHECK
    if(buf==NULL){
      LOG_ERROR("%s", "Error: malloc memory for block error!\n");
    }
#endif

    blocks[nblock] = buf;
    nblock++;

    if(nblock >= maxblocks){
      LOG_ERROR("Error: spool buffer is larger than the max blocks %d\n", maxblocks);
    }

    return buf;
  }

  int64_t getblocksize(){
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
  int64_t blocksize;

  char **blocks;
  int maxblocks;

  int64_t mem_bytes;
};

}

#endif
