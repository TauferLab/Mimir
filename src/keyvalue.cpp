#include <string.h>
#include <string>
#include "keyvalue.h"

using namespace MAPREDUCE_NS;

KeyValue::KeyValue(
  int _kvtype,
  int blocksize, 
  int maxblock,
  int maxmemsize,
  int outofcore,
  std::string filename):
  DataObject(1, blocksize, 
    maxblock, maxmemsize, outofcore, filename){
  kvtype = _kvtype;
}

KeyValue::~KeyValue(){
}

int KeyValue::getNextKV(int blockid, int offset, char **key, int &keybytes, char **value, int &valuebytes, int *kff, int *vff){
  if(offset >= blocks[blockid].datasize) return -1;
  
  int bufferid = blocks[blockid].bufferid;
  char *buf = buffers[bufferid].buf + offset;

  if(kvtype == 0){
    *key = buf;
    keybytes = strlen(*key)+1;
    buf += keybytes;
    *value = buf;
    valuebytes = strlen(*value)+1;
    if(kff != NULL) *kff = offset;
    if(vff != NULL) *vff = (offset+keybytes);
    offset += keybytes+valuebytes;
  }else if(kvtype == 1){
    keybytes = *(int*)buf;
    buf += sizeof(int);
    *key = buf;
    buf += keybytes;
    valuebytes = *(int*)buf;
    buf += sizeof(int);
    *value = buf;
    if(kff != NULL) *kff = offset+sizeof(int);
    if(vff != NULL) *vff = offset+sizeof(int)+keybytes+sizeof(int);
    offset += 2*sizeof(int)+keybytes+valuebytes;
  }

  return offset;
}

