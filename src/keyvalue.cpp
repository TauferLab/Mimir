#include <string.h>
#include <string>
#include "keyvalue.h"

#include "log.h"

using namespace MAPREDUCE_NS;

KeyValue::KeyValue(
  int _kvtype,
  int blocksize, 
  int maxblock,
  int maxmemsize,
  int outofcore,
  std::string filename):
  DataObject(KVType, blocksize, 
    maxblock, maxmemsize, outofcore, filename){
  kvtype = _kvtype;

  ksize = vsize = 0;

  LOG_PRINT(DBG_DATA, "%s", "DATA: KV Create.\n");
}

KeyValue::~KeyValue(){
  LOG_PRINT(DBG_DATA, "%s", "DATA: KV Destroy.\n");
}

int KeyValue::getNextKV(int blockid, int offset, char **key, int &keybytes, char **value, int &valuebytes, int *kff, int *vff){
  if(offset >= blocks[blockid].datasize) return -1;
  
  int bufferid = blocks[blockid].bufferid;
  char *buf = buffers[bufferid].buf + offset;

  // view KV as string pair
  if(kvtype == 0){
    *key = buf;
    keybytes = strlen(*key)+1;
    buf += keybytes;
    *value = buf;
    valuebytes = strlen(*value)+1;
    if(kff != NULL) *kff = offset;
    if(vff != NULL) *vff = (offset+keybytes);
    offset += keybytes+valuebytes;
  // view KV as bytes pair
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
  // view KV as constant bytes pair
  }else if(kvtype == 2){
    keybytes = ksize;
    *key = buf;
    valuebytes = vsize;
    buf += ksize;
    *value = buf;
    if(kff != NULL) *kff = offset;
    if(vff != NULL) *vff = offset+ksize;
    offset += ksize+vsize;
  }else{
    LOG_ERROR("Error: undefined KV type %d!\n", kvtype);
  }

  return offset;
}

/*
 * Add a KV
 * return 0 if success, else return -1
 */
int KeyValue::addKV(int blockid, char *key, int &keybytes, char *value, int &valuebytes){
  int kvbytes = 0;
  if(kvtype == 0) kvbytes = keybytes+valuebytes;
  else if(kvtype == 1) kvbytes = 2*sizeof(int)+keybytes+valuebytes;
  else if(kvtype == 2) kvbytes = ksize + vsize;
  else LOG_ERROR("Error: undefined KV type %d.\n", kvtype);

  if(kvbytes > blocksize){
     LOG_ERROR("Error: KV size is larger than block size. (KV size=%d, block size=%d)\n", kvbytes, blocksize);
  }

  int datasize = blocks[blockid].datasize;
  if(kvbytes+datasize > blocksize) return -1;

  int bufferid = blocks[blockid].bufferid;
  char *buf = buffers[bufferid].buf;

  if(kvtype == 0){
    memcpy(buf+datasize, key, keybytes);
    datasize += keybytes;
    memcpy(buf+datasize, value, valuebytes);
    datasize += valuebytes;
  }else if(kvtype == 1){
    memcpy(buf+datasize, &keybytes, sizeof(int));
    datasize += sizeof(int);
    memcpy(buf+datasize, key, keybytes);
    datasize += keybytes;
    memcpy(buf+datasize, &valuebytes, sizeof(int));
    datasize += sizeof(int);
  }else if(kvtype == 2){
    if(keybytes != ksize || valuebytes != vsize){
      LOG_ERROR("Error: key (%d) or value (%d) size mismatch for KV type 2\n", keybytes, valuebytes);
    }
    //if(valuebytes == 0){
    //  fprintf(stdout, "addKV: %ld\n", *(int64_t*)key);
    //}
    memcpy(buf+datasize, key, keybytes);
    datasize += keybytes;
    memcpy(buf+datasize, value, valuebytes);
    datasize += valuebytes;
  }else{
    LOG_ERROR("Error: undefined KV type %d\n", kvtype);
  }

  blocks[blockid].datasize = datasize;

  return 0;
}


void KeyValue::print(int type, FILE *fp, int format){
  char *key, *value;
  int keybytes, valuebytes;

  fprintf(fp, "KV Object:\n");

  for(int i = 0; i < nblock; i++){
    int offset = 0;

    acquireblock(i);

    offset = getNextKV(i, offset, &key, keybytes, &value, valuebytes);

    while(offset != -1){
      if(type == 0) fprintf(fp, "%s", key);
      else if(type == 1) fprintf(fp, "%d", *(int*)key);
      else if(type == 2) fprintf(fp, "%ld", *(int64_t*)key);
      else LOG_ERROR("%s", "Error undefined output type\n");

      if(valuebytes != 0){
        if(type == 0) fprintf(fp, ",%s", value);
        else if(type == 1) fprintf(fp, ",%d", *(int*)value);
        else if(type == 2) fprintf(fp, ",%ld", *(int64_t*)value);
        else LOG_ERROR("%s", "Error undefined output type\n");
     }
     fprintf(fp, "\n");

      offset = getNextKV(i, offset, &key, keybytes, &value, valuebytes);
    }

    releaseblock(i);

  }
}
