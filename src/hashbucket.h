#ifndef HASH_BUCKET_H
#define HASH_BUCKET_H

#include <stdio.h>
#include <stdlib.h>
#include "const.h"
#include "hash.h"
#include "memory.h"
#include "keyvalue.h"

namespace MIMIR_NS {

template<typename ElemType>
class HashBucket{
public:
    HashBucket(KeyValue *_kv){
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

        cur_buf=NULL;
        cur_off=0;

        nunique=0;
    }

    virtual ~HashBucket(){
        for(int i=0; i< maxbuf; i++){
            if(buffers[i] != NULL)
                mem_aligned_free(buffers[i]);
        }
        mem_aligned_free(buffers);
        mem_aligned_free(buckets);
    }

    // Comapre key with elem
    virtual int compare(char *key, int keybytes, ElemType *)=0;
    virtual int getkey(ElemType *, char **pkey, int *pkeybytes)=0;
    virtual ElemType* insertElem(ElemType *elem)=0;

    ElemType* findElem(char *key, int keybytes){    

        int ibucket = hashlittle(key, keybytes, 0) % nbucket;

        ElemType *ptr = buckets[ibucket];

        while(ptr!=NULL){
            if(compare(key, keybytes, ptr) != 0)
                break;
            ptr=ptr->next;
        }
        return ptr;
    }

protected:
    int        nbucket;
    ElemType **buckets;
  
    int     usize, maxbuf, nbuf, ibuf;
    char  **buffers;
    char   *cur_buf;
    int     cur_off;

    int64_t nunique;

    KeyValue *kv;
};

struct CombinerUnique{
    char *kv;
    CombinerUnique *next;
};

class CombinerHashBucket : public HashBucket<CombinerUnique>{
public:
    CombinerHashBucket(KeyValue *_kv) : \
        HashBucket<CombinerUnique>(_kv){
    }

    CombinerUnique* insertElem(CombinerUnique *elem);
 
    int compare(char *key, int keybytes, CombinerUnique *);

    int getkey(CombinerUnique *, char **pkey, int *pkeybytes);
};

struct ReducerSet{
    int       pid;
    int64_t   ivalue;
    int64_t   nvalue;
    int64_t   mvbytes;
    int      *soffset; 
    char     *voffset;
    ReducerSet *next;
};

struct ReducerUnique{
    char *key;
    int keybytes;
    int64_t nvalue;
    int64_t mvbytes;
    ReducerSet *firstset;
    ReducerSet *lastset;
    ReducerUnique *next;
};

class ReducerHashBucket : public HashBucket<ReducerUnique>{
public:
    ReducerHashBucket(KeyValue *_kv) : \
        HashBucket<ReducerUnique>(_kv){

        maxset = MAX_PAGE_COUNT;
        setsize = nbucket*sizeof(ReducerUnique);

        sets = (char**)mem_aligned_malloc(\
            MEMPAGE_SIZE, maxset*sizeof(char*));
        isetbuf = nsetbuf = 0;

        iset = nset = 0;

        mvbytes=0;

        cur_unique=NULL;
    }

    ~ReducerHashBucket(){
        for(int i=0; i< maxset; i++){
            if(sets[i] != NULL)
                mem_aligned_free(sets[i]);
        }

        mem_aligned_free(sets);
    }

    ReducerUnique* insertElem(ReducerUnique *elem);
 
    int compare(char *key, int keybytes, ReducerUnique *);

    int getkey(ReducerUnique *, char **pkey, int *pkeybytes);

    ReducerUnique* BeginUnique(){
        if(nbuf>0){
            ibuf=0;
            cur_buf=buffers[ibuf];
            cur_off=0;
            cur_unique=(ReducerUnique*)(cur_buf+cur_off);
            cur_off+=sizeof(ReducerUnique);
            cur_off+=cur_unique->keybytes;
        }else{
            cur_unique=NULL;
            return NULL;
        }
        return cur_unique;
    }

    ReducerUnique* NextUnique(){
        cur_unique=(ReducerUnique*)(cur_buf+cur_off);
        if((usize-cur_off)<sizeof(ReducerUnique) || \
            cur_unique->key==NULL){
            ibuf+=1;
            if(ibuf<nbuf){
                cur_buf=buffers[ibuf];
                cur_off=0;
            }else{
                cur_unique=NULL;
                return NULL;
            }
        }
        cur_off+=sizeof(ReducerUnique);
        cur_off+=cur_unique->keybytes;
        return cur_unique;
    }

    ReducerSet* BeginSet(){
        ReducerSet *pset=NULL;
        if(nset<=0) return NULL;
        else{
            iset = 0;
            pset=(ReducerSet*)sets[iset/nbucket]+iset%nbucket;
            return pset;
        }
        return pset;
    }

    ReducerSet* NextSet(){
        ReducerSet *pset=NULL;
        iset += 1;
        if(iset>=nset) return NULL;
        else{
            pset=(ReducerSet*)sets[iset/nbucket]+iset%nbucket;
        }
        return pset;
    }


private:
    ReducerUnique *cur_unique;

    int64_t nset, iset;
    int  setsize, maxset, nsetbuf, isetbuf;
    char **sets;

    int64_t mvbytes;
};

}

#endif
