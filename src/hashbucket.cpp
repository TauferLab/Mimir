#include <stdio.h>
#include <stdlib.h>
#include "hashbucket.h"
#include "const.h"
#include "hash.h"
#include "memory.h"

using namespace MIMIR_NS;

CombinerUnique* CombinerHashBucket::insertElem(CombinerUnique *elem){
    char *key;
    int  keybytes;

    getkey(elem, &key, &keybytes);

    nunique+=1;

    if(nbuf == (nunique/nbucket)){
        buffers[nbuf]=(char*)mem_aligned_malloc(\
            MEMPAGE_SIZE, usize);
        nbuf+=1;
    }

    CombinerUnique *newelem=(CombinerUnique*)buffers[nunique/nbucket]\
        +nunique%nbucket;
    memcpy(newelem, elem, sizeof(ElemType));

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
    char *kvbuf = u->kv;
    char *ukey, *uvalue;
    int  ukeybytes, uvaluebytes, kvsize;

    printf("comprare: key=%s, ptr=%p, kv=%p\n", key, u, kvbuf);

    //KeyValue *kv=(KeyValue*)data; 
    GET_KV_VARS(kv->kvtype,kvbuf,ukey,ukeybytes,uvalue,uvaluebytes,kvsize,kv);
    if(keybytes==ukeybytes && memcmp(key, ukey, keybytes)==0)
        return 1;

    return 0;
}

int CombinerHashBucket::getkey(CombinerUnique *u, char **pkey, int *pkeybytes){

    char *kvbuf = u->kv;
    char *ukey, *uvalue;
    int  ukeybytes, uvaluebytes, kvsize;
   
    //KeyValue *kv=(KeyValue*)data; 
    GET_KV_VARS(kv->kvtype,kvbuf,ukey,ukeybytes,uvalue,uvaluebytes,kvsize,kv);
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
            if(cur_buf!=NULL) 
                memset(cur_buf+cur_off, 0, usize-cur_off);
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
            elem->next = tmp;
        }

        if(nsetbuf == (nset/nbucket)){
            sets[nsetbuf]=(char*)mem_aligned_malloc(\
                MEMPAGE_SIZE, setsize);
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
        if(kv->kvtype==GeneralKV ||\
          kv->kvtype==StringKGeneralV || \
          kv->kvtype==FixedKGeneralV){
            onemv+=sizeof(int);
        }
        int64_t onemvbytes=elem->mvbytes;
        ReducerSet *set=NULL;
        if(mvbytes+onemv>kv->pagesize){
            if(nsetbuf == (nset/nbucket)){
                sets[nsetbuf]=(char*)mem_aligned_malloc(\
                    MEMPAGE_SIZE, setsize);
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


