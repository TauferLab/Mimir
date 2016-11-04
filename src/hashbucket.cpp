#include "hashbucket.h"
#include "const.h"
#include "hash.h"
#include "memory.h"

using namespace MIMIR_NS;

template<class ElemType>
HashBucket<ElemType>::HashBucket(KeyValue *_kv){
    kv = _kv;

    nbucket = BUCKET_COUNT;
    usize = nbucket*sizeof(ElemType);
    maxbuf = MAX_PAGE_COUNT;
    buckets = (ElemType**)mem_aligned_malloc(\
        MEMPAGE_SIZE, sizeof(ElemType*)*BUCKET_COUNT);
    buffers = (char**)mem_aligned_malloc(\
        MEMPAGE_SIZE, maxbuf*sizeof(char*));

    for(int i=0; i<nbucket; i++)
        buckets[i] = NULL;

    nbuf = 0;
    for(int i=0; i<maxbuf; i++)
        buffers[i] = NULL;
}

template<class ElemType>
HashBucket<ElemType>::~HashBucket(){
    for(int i=0; i< maxbuf; i++){
        if(buffers[i] != NULL)
            mem_aligned_free(buffers[i]);
    }
    mem_aligned_free(buffers);
    mem_aligned_free(buckets);
}

template<class ElemType>
ElemType* HashBucket<ElemType>::findElem(char *key, int keybytes){    
    int ibucket = hashlittle(key, keybytes, 0) % nbucket;

    ElemType *ptr = buckets[ibucket];
    while(ptr!=NULL){
        if(compare(key, keybytes, ptr) != 0)
            break;
        ptr=ptr->next;
    }
    return ptr;
}

template<class ElemType>
ElemType* HashBucket<ElemType>::insertElem(ElemType *elem){
    char *key;
    int  keybytes;

    getkey(elem, &key, &keybytes);

    int ibucket = hashlittle(key, keybytes, 0) % nbucket;

    ElemType *ptr = buckets[ibucket];

    // New unique key
    if(ptr==NULL){
        buckets[ibucket] = elem;
        return NULL;
    }
    else{
        // Compare
        while(ptr!=NULL){
            if(compare(key, keybytes, ptr) != 0)
                break;
            ptr=ptr->next;
        }
        // New unique key
        if(ptr==NULL){
            ElemType *tmp = buckets[ibucket];
            buckets[ibucket] = elem;
            elem->next = tmp;
            return NULL;
        }
    }
    return ptr;
}

CombinerHashBucket::CombinerHashBucket(KeyValue *_kv)\
:HashBucket<CombinerUnique>(_kv){
}

int CombinerHashBucket::compare(char *key, int keybytes, CombinerUnique *u){
    char *kvbuf = u->kv;
    char *ukey, *uvalue;
    int  ukeybytes, uvaluebytes, kvsize;
    
    GET_KV_VARS(kv->kvtype,kvbuf,ukey,ukeybytes,uvalue,uvaluebytes,kvsize,kv);
    if(keybytes==ukeybytes && memcmp(key, ukey, keybytes)==0)
        return 1;

    return 0;
}

int CombinerHashBucket::getkey(CombinerUnique *u, char **pkey, int *pkeybytes){

    char *kvbuf = u->kv;
    char *ukey, *uvalue;
    int  ukeybytes, uvaluebytes, kvsize;
    
    GET_KV_VARS(kv->kvtype,kvbuf,ukey,ukeybytes,uvalue,uvaluebytes,kvsize,kv);
    *pkey=ukey;
    *pkeybytes=ukeybytes;

    return 0;
 
}


