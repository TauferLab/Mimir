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
    //kvtype = GeneralKV;
    ksize = vsize = KVGeneral;
    local_kvs_count = 0;
    global_kvs_count = 0;
    mr = NULL;
    mycombiner = NULL; 
    bucket = NULL;
    LOG_PRINT(DBG_DATA, me, nprocs, "DATA: KV Create (id=%d).\n", id);
}

KeyValue::~KeyValue()
{
    if(bucket != NULL) delete bucket;
    LOG_PRINT(DBG_DATA, me, nprocs, "DATA: KV Destroy (id=%d).\n", id);
}

int KeyValue::getNextKV(char **pkey, int &keybytes, \
    char **pvalue, int &valuebytes)
{
    if(off>=pages[ipage].datasize)
        return -1;

    int kvsize;
    GET_KV_VARS(ksize, vsize, ptr, *pkey, keybytes, \
        *pvalue, valuebytes, kvsize);
    ptr+=kvsize;
    off+=kvsize;

    return kvsize;
}

void KeyValue::set_combiner(MapReduce *_mr, UserCombiner _combiner){
    mr = _mr;
    mycombiner = _combiner;

    if(mycombiner != NULL)
        bucket = new CombinerHashBucket(this);
}

// add KVs one by one
int KeyValue::addKV(const char *key,int keybytes,const char *value,int valuebytes){
 
    // add the first page
    if(ipage==-1) add_page();

    // get the size of the KV
    int kvsize=0;
    GET_KV_SIZE(ksize, vsize, keybytes, valuebytes, kvsize);

    // KV size should be smaller than page size.
    if(kvsize > pagesize)
        LOG_ERROR("Error: KV size (%d) is larger \
            than one page (%ld)\n", kvsize, pagesize);
 
    if(mycombiner == NULL){
        // add another page
        if(kvsize>pagesize-pages[ipage].datasize)
            add_page();

        // put KV data in
        char *ptr=pages[ipage].buffer+pages[ipage].datasize;

        //printf("add: key=%s, buffer=%p\n", key, pages[ipage].buffer); fflush(stdout); 

        PUT_KV_VARS(ksize, vsize, ptr, key, keybytes, value, valuebytes, kvsize);
        pages[ipage].datasize+=kvsize;

        local_kvs_count +=1;
    }else{

        //printf("add kv (key=%s) with combiner\n", key); fflush(stdout);
 
        // find the key
        CombinerUnique *u = bucket->findElem(key, keybytes);

        //printf("find: key=%s, u=%p\n",key, u); 
        //fflush(stdout);

        // the first one
        if(u == NULL){
            CombinerUnique tmp;
            tmp.next=NULL; 
            // find a space in the slices space
            std::unordered_map<char*,int>::iterator iter;
            for(iter=slices.begin(); iter!=slices.end(); iter++){
                if(iter->second >= kvsize){
                    tmp.kv = (char*)iter->first+(iter->second-kvsize);

                    PUT_KV_VARS(ksize, vsize, tmp.kv, \
                        key, keybytes, value, valuebytes, kvsize);

                    if(iter->second == kvsize)
                        slices.erase(iter);
                    else
                        slices[iter->first]-=kvsize;
                    break;
                }
            }
            // Cannot find a memory slice, then add the KV at end
            if(iter==slices.end()){
                if(kvsize>pagesize-pages[ipage].datasize)
                    add_page();
                
                tmp.kv=pages[ipage].buffer+pages[ipage].datasize;

                PUT_KV_VARS(ksize,vsize,tmp.kv,\
                    key,keybytes,value,valuebytes,kvsize);

                pages[ipage].datasize+=kvsize;
            }

            //printf("insert<start>: key=%s, kv=%p\n",key, u.kv); 
            //fflush(stdout);

            bucket->insertElem(&tmp);

            //printf("insert end: key=%s, u=%p, kv=%p\n",key, u, u.kv); 
            //fflush(stdout);

            local_kvs_count+=1;
        }else{

            //printf("find: key=%s, u=%p, kv=%p\n",key, u, u->kv); 
            //fflush(stdout);

            // get previous KV information
            int  ukvsize;
            char *ukey, *uvalue;
            int  ukeybytes, uvaluebytes;
            GET_KV_VARS(ksize,vsize,u->kv,ukey,ukeybytes,uvalue,uvaluebytes,ukvsize);

            // invoke KV information
            mycombiner(mr,key,keybytes,uvalue,uvaluebytes,value,valuebytes, mr->myptr);

            // check if the key is same 
            if(mr->newkeysize!=keybytes || \
                memcmp(mr->newkey, ukey, keybytes)!=0)
                LOG_ERROR("%s", "Error: the result key of combiner is different!\n");
            
            // get key size
            GET_KV_SIZE(ksize, vsize, mr->newkeysize, mr->newvalsize, kvsize);

            // update the value of the key
            if(kvsize<=ukvsize){
                //printf("u->kv=%p\n", u->kv);
                PUT_KV_VARS(ksize, vsize, u->kv, key, keybytes, mr->newval, mr->newvalsize, kvsize);
                if(kvsize < ukvsize)
                    slices.insert(std::make_pair(u->kv,ukvsize-kvsize));
            }else{
                // add memory slice information
                slices.insert(std::make_pair(u->kv, ukvsize));
                // add at the end of buffers
                if( kvsize>(pagesize-pages[ipage].datasize) ) add_page();
                u->kv=pages[ipage].buffer+pages[ipage].datasize;
                PUT_KV_VARS(vsize, vsize, u->kv, key, keybytes, value, valuebytes, kvsize);
                pages[ipage].datasize+=kvsize;
            }
        }
    }
}


void KeyValue::gc(){
    //printf("gc: combiner=%p, npages=%d, %d\n", mycombiner, npages, slices.empty());
    if(mycombiner!=NULL && npages>0 && slices.empty()==false){
        int dst_pid=0,src_pid=0;
        int64_t dst_off=0,src_off=0;
        char *dst_buf=NULL;
        char *src_buf=pages[0].buffer;
        while(src_pid<npages){
            src_off=0;
            while(src_off<pages[src_pid].datasize){
                src_buf=pages[src_pid].buffer+src_off;
                std::unordered_map<char*,int>::iterator iter=slices.find(src_buf);
                // skip the memory slice
                if(iter != slices.end()){
                    if(dst_buf==NULL){
                        dst_pid=src_pid;
                        dst_off=src_off;
                        dst_buf=src_buf;
                    }
                    src_off+=iter->second;
                }else{
                    // get the kv
                    char *key, *value;
                    int  keybytes, valuebytes, kvsize;
                    GET_KV_VARS(ksize,vsize,src_buf,key,keybytes,value,valuebytes,kvsize);
                    src_buf+=kvsize;
                    // need copy
                    if(dst_buf!=NULL && src_buf != dst_buf){
                        // jump to the next page
                        if(pagesize-dst_off<kvsize){
                            pages[dst_pid].datasize=dst_off;
                            dst_pid+=1;
                            dst_off=0;
                            dst_buf=pages[dst_pid].buffer;
                        }
                        // copy the KV
                        memcpy(dst_buf, src_buf-kvsize, kvsize);
                        dst_off+=kvsize;
                        dst_buf+=kvsize;
                    }
                    src_off+=kvsize;
                }
            }
        }
        // free extra space
        for(int i=dst_pid+1; i<npages; i++){
           mem_aligned_free(pages[i].buffer); 
           pages[i].buffer=NULL;
           pages[i].datasize=0;
        }
        npages=dst_pid+1;
        slices.clear();
    } 
}

void KeyValue::print(FILE *fp, ElemType ktype, ElemType vtype){

  char *key, *value;
  int keybytes, valuebytes;

  printf("key\tvalue\n");

  for(int i = 0; i < npages; i++){
    acquire_page(i);

    int offset = getNextKV(&key, keybytes, &value, valuebytes);

    while(offset != -1){

      if(ktype==StringType) fprintf(fp, "%s", key);
      else if(ktype==Int32Type) fprintf(fp, "%d", *(int*)key);
      else if(ktype==Int64Type) fprintf(fp, "%ld", *(int64_t*)key);

      if(vtype==StringType) fprintf(fp, "\t%s", value);
      else if(vtype==Int32Type) fprintf(fp, "\t%d", *(int*)value);
      else if(vtype==Int64Type) fprintf(fp, "\t%ld", *(int64_t*)value);
       
      fprintf(fp, "\n");

      offset = getNextKV(&key, keybytes, &value, valuebytes);
    }

    release_page(i);

  }
}
