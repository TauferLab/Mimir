#include "dataobject.h"

using namespace MAPREDUCE_NS;

DataObject::DataObject(int _blocksize, int _maxblock){
  nitem = 0;
  nblock = 0;
  blocksize = _blocksize;
  maxblock  = _maxblock;

  blocks = new char*[maxblock];
  headers = new Block[maxblock]; 

  for(int i=0;i<maxblock;i++) {
    headers[i].item_tail=0;
    headers[i].remain_bytes=0;
    headers[i].flag = 0;
    blocks[i]=NULL;
  }
}

DataObject::~DataObject(){
  for(int i=0;i<maxblock;i++){
    if(blocks[i] == NULL)
      delete blocks[i];
  }

  delete [] blocks;
  delete [] headers;
}

/* add a block */
int DataObject::add_block(void){
  int blockid;
  blockid = __sync_fetch_and_add(&nblock, 1);
  if(blockid < maxblock){
    blocks[blockid] = (char*)malloc(blocksize);
    headers[blockid].item_tail = 0;
    headers[blockid].remain_bytes=blocksize;
    headers[blockid].flag = 0;
    return blockid;
  }
  return -1;
}

/* get a block */
char * DataObject::acquire_block(int blockid){
  if(blockid >= nblock || blocks[blockid] ==  NULL)
    return NULL;
  headers[blockid].flag = 1;
  return blocks[blockid];
}

/* release a block */
void DataObject::release_block(int blockid){
  if(blockid < nblock)
    headers[blockid].flag = 0;
}
