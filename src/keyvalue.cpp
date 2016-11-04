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
    bucket = NULL;
    LOG_PRINT(DBG_DATA, me, nprocs, "DATA: KV Create (id=%d).\n", id);
}

KeyValue::~KeyValue()
{
    if(bucket != NULL) delete bucket;
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

    if(combiner != NULL)
        bucket = new CombinerHashBucket(this);
}

// Add KVs one by one
int KeyValue::addKV(char *key, int keybytes, char *value, int valuebytes){
    if(ipage==-1) add_page();

    int kvsize=0;
    GET_KV_SIZE(kvtype, keybytes, valuebytes, kvsize);

    if(kvsize > pagesize)
        LOG_ERROR("Error: KV size (%d) is larger than one page (%ld)\n", kvsize, pagesize);

    if(combiner == NULL){
        if(kvsize>pagesize-pages[ipage].datasize) add_page();
        char *ptr=pages[ipage].buffer+pages[ipage].datasize;
        PUT_KV_VARS(kvtype, ptr, key, keybytes, value, valuebytes, kvsize);
        pages[ipage].datasize+=kvsize;
    }else{ 
        int kvsize;
        GET_KV_SIZE(kvtype, keybytes, valuebytes, kvsize);
        CombinerUnique *u = bucket->findElem(key, keybytes);
        // The first one
        if(u == NULL){
            std::unordered_map<char*,int>::iterator iter;
            for(iter=slices.begin(); iter!=slices.end(); iter++){
                if(iter->second >= kvsize){
                    char *ptr = (char*)iter->first+(iter->second-kvsize);
                    PUT_KV_VARS(kvtype, ptr, key, keybytes, value, valuebytes, kvsize);
                    slices[iter->first]-=kvsize;
                    break;
                }
            }
            if(iter==slices.end()){
                if(kvsize>pagesize-pages[ipage].datasize) add_page();
                char *ptr=pages[ipage].buffer+pages[ipage].datasize;
                PUT_KV_VARS(kvtype, ptr, key, keybytes, value, valuebytes, kvsize);
                pages[ipage].datasize+=kvsize;
            }
        }else{
            int prekvsize;
            char *kvbuf=u->kv, *ukey, *uvalue;
            int  ukeybytes, uvaluebytes, kvsize;
            GET_KV_VARS(kvtype,kvbuf,ukey,ukeybytes,uvalue,uvaluebytes,prekvsize, this);
            combiner(mr,key,keybytes,uvalue,uvaluebytes,value,valuebytes, ptr);
            if(kvsize<=prekvsize){
                PUT_KV_VARS(kvtype, kvbuf, key, keybytes, value, valuebytes, kvsize);
                if(kvsize < prekvsize)
                    slices.insert(std::make_pair(kvbuf,prekvsize-kvsize));
            }else{
                slices.insert(std::make_pair(u->kv, prekvsize));
                if( kvsize>(pagesize-pages[ipage].datasize) ) add_page();
                char *ptr=pages[ipage].buffer+pages[ipage].datasize;
                u->kv=ptr;
                PUT_KV_VARS(kvtype, ptr, key, keybytes, value, valuebytes, kvsize);
                pages[ipage].datasize+=kvsize;
            }
        }
    }
}


void KeyValue::gc(){
    if(combiner!=NULL && slices.empty()==false){
        int dst_pid=0,src_pid=0;
        int64_t dst_off=0,src_off=0;
        char *dst_buf=NULL;
        char *src_buf=pages[0].buffer;
        while(src_pid<npages){
            while(src_off<pages[src_pid].datasize){
                src_buf=pages[src_pid].buffer+src_off;
                std::unordered_map<char*,int>::iterator iter=slices.find(src_buf);
                if(iter != slices.end()){
                    if(dst_buf==NULL){
                        dst_pid=src_pid;
                        dst_off=src_off;
                        dst_buf=iter->first;
                    }
                    src_off+=iter->second;
                }else{
                    char *key, *value;
                    int  keybytes, valuebytes, kvsize;
                    GET_KV_VARS(kvtype,src_buf,key,keybytes,value,valuebytes,kvsize,this);
                    if(dst_buf!=NULL && src_buf != dst_buf){
                        if(pagesize-dst_off<kvsize){
                            pages[dst_pid].datasize=dst_off;
                            dst_pid+=1;
                            dst_off=0;
                            dst_buf=pages[dst_pid].buffer;
                        }
                        memcpy(dst_buf, src_buf-kvsize, kvsize);
                        dst_off+=kvsize;
                        dst_buf+=kvsize;
                    }
                    src_off+=kvsize;
                }
            }
        }
        for(int i=dst_pid+1; i<npages; i++){
           mem_aligned_free(pages[i].buffer); 
           pages[i].buffer=NULL;
           pages[i].datasize=0;
        }
        npages=dst_pid+1;
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
