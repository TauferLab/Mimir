#include <string.h>
#include <string>
#include "keyvalue.h"
#include "log.h"
#include "const.h"

using namespace MIMIR_NS;

KeyValue::KeyValue(
    int me,
    int nprocs,
    int64_t pagesize,
    int maxpages):
    DataObject(me, nprocs, KVType, pagesize, maxpages)
{
    kvtype = GeneralKV;
    ksize = vsize = 0;
    local_kvs_count = 0;
    global_kvs_count = 0;
    mr = NULL;
    combiner = NULL; 
    LOG_PRINT(DBG_DATA, me, nprocs, "DATA: KV Create (id=%d).\n", id);
}

KeyValue::~KeyValue()
{
    LOG_PRINT(DBG_DATA, me, nprocs, "DATA: KV Destroy (id=%d).\n", id);
}

int KeyValue::getNextKV(char **pkey, int &keybytes, char **pvalue, int &valuebytes)
{
    if(off>=pages[ipage].datasize) return -1;
    int kvsize;
    GET_KV_VARS(kvtype, ptr, *pkey, keybytes, *pvalue, valuebytes, kvsize, this);
    off+=kvsize;
    return kvsize;
}

void KeyValue::set_combiner(MapReduce *_mr, UserCombiner _combiner){
    mr = _mr;
    combiner = _combiner;

     
}

// Add KVs one by one
int KeyValue::addKV(char *key, int keybytes, char *value, int valuebytes){
    int kvsize=0;
    GET_KV_SIZE(kvtype, keybytes, valuebytes, kvsize);
    if(combiner == NULL){
        for(int i=0; i< npages; i++){
            if(kvsize<pagesize-pages[i].datasize){
                char *ptr=pages[i].buffer+pages[i].datasize;
                GET_KV_VARS(kvtype, ptr, key, keybytes, value, valuebytes, kvsize, this);
                pages[i].datasize+=kvsize;
            }
        } 
    }else{
        
    }
}


// Add KVs one by one
//int addKV(char *, int, char *, int);



#if 0
int64_t KeyValue::getNextKV(int blockid, int64_t offset, char **key, int &keybytes, char **value, int &valuebytes, int *kff, int *vff){
  if(offset >= blocks[blockid].datasize) return -1;

  int bufferid = blocks[blockid].bufferid;
  char *buf = buffers[bufferid].buf + offset;

  int kvsize=0;
  GET_KV_VARS(kvtype,buf,*key,keybytes,*value,valuebytes,kvsize,this);

  offset+=kvsize;

  return offset;
}
#endif

#if 0
/*
 * Add a KV
 * return 0 if success, else return -1
 */
int KeyValue::addKV(int blockid, char *key, int &keybytes, char *value, int &valuebytes){
  int kvbytes = 0;

  GET_KV_SIZE(kvtype, keybytes, valuebytes, kvbytes);

#if SAFE_CHECK
  if(kvbytes > blocksize){
     LOG_ERROR("Error: KV size is larger than block size. (KV size=%d, block size=%ld)\n", kvbytes, blocksize);
  }
#endif

  int64_t datasize = blocks[blockid].datasize;
  if(kvbytes+datasize > blocksize) return -1;

  int bufferid = blocks[blockid].bufferid;
  char *buf = buffers[bufferid].buf+datasize;

  PUT_KV_VARS(kvtype, buf, key, keybytes, value, valuebytes, kvbytes);
  blocks[blockid].datasize += kvbytes;

  return 0;
}
#endif

#if 0
void KeyValue::print(int type, FILE *fp, int format){
  char *key, *value;
  int keybytes, valuebytes;

  for(int i = 0; i < npages; i++){
    int64_t offset = 0;

    acquire_page(i);

    offset = getNextKV(i, offset, &key, keybytes, &value, valuebytes);

    while(offset != -1){
      //fprintf(fp, "%s", key);

      //if(type == 0) fprintf(fp, "%s", key);
      //else if(type == 1) fprintf(fp, "%d", *(int*)key);
      //else if(type == 2) fprintf(fp, "%ld", *(int64_t*)key);
      //else LOG_ERROR("%s", "Error undefined output type\n");

      //if(valuebytes != 0){
      //  if(type == 0) fprintf(fp, "%s", value);
      //  else if(type == 1) fprintf(fp, "%d", *(int*)value);
      //  else if(type == 2) fprintf(fp, "%ld", *(int64_t*)value);
      //  else LOG_ERROR("%s", "Error undefined output type\n");
     //}
     fprintf(fp, "\n");

      offset = getNextKV(i, offset, &key, keybytes, &value, valuebytes);
    }

    release_page(i);

  }
}
#endif
