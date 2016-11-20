#include <stdio.h>
#include <stdlib.h>
#include "hashbucket.h"
#include "const.h"
#include "hash.h"
#include "memory.h"
#include "stat.h"

using namespace MIMIR_NS;

int64_t CombinerHashBucket::mem_bytes=0;
int64_t ReducerHashBucket::mem_bytes=0;

CombinerUnique* CombinerHashBucket::insertElem(CombinerUnique *elem){
    char *key;
    int  keybytes;

    getkey(elem, &key, &keybytes);

    if(nbuf == (nunique/nbucket)){
        buffers[nbuf]=(char*)mem_aligned_malloc(\
            MEMPAGE_SIZE, usize);
        CombinerHashBucket::mem_bytes+=usize;

        PROFILER_RECORD_COUNT(COUNTER_MEM_BUCKET, CombinerHashBucket::mem_bytes, OPMAX);
 
        nbuf+=1;
    }

    CombinerUnique *newelem=\
        (CombinerUnique*)buffers[nunique/nbucket]\
        +nunique%nbucket;
    memcpy(newelem, elem, sizeof(CombinerUnique));

    nunique+=1;

    int ibucket = hashlittle(key, keybytes, 0) % nbucket;

    CombinerUnique *ptr = buckets[ibucket];

    // New unique key
    if(ptr==NULL){
        buckets[ibucket] = newelem;
        return NULL;
    }else{
        CombinerUnique *tmp = buckets[ibucket];
        buckets[ibucket] = newelem;
        elem->next = tmp;
        return NULL;
    }
    return ptr;
}

int CombinerHashBucket::compare(char *key, int keybytes, CombinerUnique *u){
    char *ukey, *uvalue;
    int  ukeybytes, uvaluebytes, kvsize;

    //printf("compare: u=%p, kv=%p\n", u, u->kv);

    GET_KV_VARS(kv->ksize,kv->vsize,u->kv,\
        ukey,ukeybytes,uvalue,uvaluebytes,kvsize);

    if(keybytes==ukeybytes && memcmp(key, ukey, keybytes)==0)
        return 1;

    return 0;
}

int CombinerHashBucket::getkey(CombinerUnique *u, char **pkey, int *pkeybytes){

    char *ukey, *uvalue;
    int  ukeybytes, uvaluebytes, kvsize;
   
    GET_KV_VARS(kv->ksize,kv->vsize,u->kv,
        ukey,ukeybytes,uvalue,uvaluebytes,kvsize);

    *pkey=ukey;
    *pkeybytes=ukeybytes;

    return 0; 
}

ReducerUnique* ReducerHashBucket::insertElem(ReducerUnique *elem){
    char *key;
    int  keybytes;

    getkey(elem, &key, &keybytes);

    int ibucket = hashlittle(key, keybytes, 0) % nbucket;

    ReducerUnique *ptr = buckets[ibucket];

    // Compare
    while(ptr != NULL){
        if(compare(key, keybytes, ptr) != 0)
            break;
        ptr=ptr->next;
    }

    // New unique key
    if(ptr==NULL){
        nunique+=1;

        if(cur_buf == NULL || \
            (usize-cur_off)<sizeof(ReducerUnique)+elem->keybytes){

            buffers[nbuf]=(char*)mem_aligned_malloc(MEMPAGE_SIZE, usize);
            ReducerHashBucket::mem_bytes+=usize;

            PROFILER_RECORD_COUNT(COUNTER_MEM_BUCKET, ReducerHashBucket::mem_bytes, OPMAX);
            
            if(cur_buf!=NULL)  memset(cur_buf+cur_off, 0, usize-cur_off);

            cur_buf=buffers[nbuf];
            cur_off=0;
            nbuf+=1;
        }

        ReducerUnique *newelem=(ReducerUnique*)(cur_buf+cur_off);
        cur_off += sizeof(ReducerUnique);
        newelem->key=cur_buf+cur_off;
        newelem->keybytes=elem->keybytes;
        newelem->nvalue=1;
        newelem->mvbytes=elem->mvbytes;
        newelem->firstset=NULL;
        newelem->lastset=NULL;
        newelem->next=NULL;
        memcpy(cur_buf+cur_off, elem->key, elem->keybytes);
        cur_off += elem->keybytes;

        if(buckets[ibucket]==NULL)
            buckets[ibucket] = newelem;
        else{
            ReducerUnique *tmp = buckets[ibucket];
            buckets[ibucket] = newelem;
            newelem->next = tmp;
        }

        if(nsetbuf == (nset/nbucket)){
            sets[nsetbuf]=(char*)mem_aligned_malloc(\
                MEMPAGE_SIZE, setsize);
            //printf("sets[0]=%p\n", sets[0]);
            ReducerHashBucket::mem_bytes+=setsize;
        
            PROFILER_RECORD_COUNT(COUNTER_MEM_BUCKET, ReducerHashBucket::mem_bytes, OPMAX);
 
            nsetbuf+=1;
        }

        ReducerSet *set=(ReducerSet*)sets[nset/nbucket]+nset%nbucket;
        nset+=1;

        set->pid=0;
        set->ivalue=0;
        set->nvalue=1;
        set->mvbytes=elem->mvbytes;
        set->soffset=NULL;
        set->voffset=NULL;
        set->next=NULL;

        newelem->firstset=set;
        newelem->lastset=set;

        return newelem;
    }else{        
        ptr->nvalue+=1;
        ptr->mvbytes+=elem->mvbytes;

        int64_t onemv=sizeof(int)+elem->mvbytes;
        if(kv->vsize==KVGeneral){
            onemv+=sizeof(int);
        }
        int64_t onemvbytes=elem->mvbytes;
        ReducerSet *set=NULL;
        if(mvbytes+onemv>kv->pagesize){
            if(nsetbuf == (nset/nbucket)){
                sets[nsetbuf]=(char*)mem_aligned_malloc(\
                    MEMPAGE_SIZE, setsize);
                ReducerHashBucket::mem_bytes+=setsize;

                PROFILER_RECORD_COUNT(COUNTER_MEM_BUCKET, ReducerHashBucket::mem_bytes, OPMAX);

                nsetbuf+=1;
            }

            set=(ReducerSet*)sets[nset/nbucket]+nset%nbucket;
            nset+=1;

            set->pid=0;
            set->ivalue=0;
            set->nvalue=1;
            set->mvbytes=elem->mvbytes;
            set->soffset=NULL;
            set->voffset=NULL;
            set->next=NULL;

            ptr->lastset->next=set;
            ptr->lastset=set;

            mvbytes = onemvbytes;
        }else{
            mvbytes += onemvbytes;
            set=ptr->lastset;
        }

        set->nvalue+=1;
        set->mvbytes+=elem->mvbytes;
       
        return ptr;
    }
}

int ReducerHashBucket::compare(char *key,int keybytes,ReducerUnique *u){
    if(keybytes==u->keybytes && memcmp(key, u->key, keybytes)==0)
        return 1;

    return 0;
}

int ReducerHashBucket::getkey(ReducerUnique *u, char **pkey, int *pkeybytes){
    *pkey=u->key;
    *pkeybytes=u->keybytes;

    return 0; 
}


