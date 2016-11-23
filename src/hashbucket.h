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

        nbucket = pow(2,BUCKET_COUNT);
        usize = nbucket*sizeof(ElemType);
        maxbuf = MAX_PAGE_COUNT;
        buckets = (ElemType**)mem_aligned_malloc(\
            MEMPAGE_SIZE, sizeof(ElemType*)*nbucket);
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
        mem_aligned_free(buffers);
        mem_aligned_free(buckets);
    }

    //static int64_t get_mem_bytes(){
    //    return 0;
        //return HashBucket<ElemType>::mem_bytes;
    //}

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
            //printf("find: ibucket=%d, ptr=%p\n", ibucket, ptr);
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

    int64_t iunique;
    int64_t nunique;

    KeyValue *kv;

//public:
    //static int64_t mem_bytes;
};

//template<typename ElemType>
//int64_t HashBucket<ElemType>::mem_bytes=0;

struct CombinerUnique{
    char *kv;
    CombinerUnique *next;
};

class CombinerHashBucket : public HashBucket<CombinerUnique>{
public:
    CombinerHashBucket(KeyValue *_kv) : \
        HashBucket<CombinerUnique>(_kv){
    }

    ~CombinerHashBucket(){
        for(int i=0; i< maxbuf; i++){
            if(buffers[i] != NULL){
                CombinerHashBucket::mem_bytes-=usize;
                mem_aligned_free(buffers[i]);
            }
        } 
    }

    CombinerUnique* insertElem(CombinerUnique *elem);
 
    int compare(char *key, int keybytes, CombinerUnique *);

    int getkey(CombinerUnique *, char **pkey, int *pkeybytes);

public:
    static int64_t mem_bytes;
};

struct ReducerSet{
    int       pid;
    int64_t   ivalue;
    int64_t   nvalue;
    int64_t   mvbytes;
    int      *soffset; 
    char     *voffset;
    char     *curoff;
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
        for(int i=0; i<maxset; i++) sets[i]=NULL;

        isetbuf = nsetbuf = 0;

        iset = nset = 0;

        mvbytes=0;

        cur_unique=NULL;

        pid=0;
    }

    ~ReducerHashBucket(){
        //printf("test1\n"); fflush(stdout);
        for(int i=0; i< maxbuf; i++){
            if(buffers[i] != NULL){
                ReducerHashBucket::mem_bytes-=usize;
                mem_aligned_free(buffers[i]);
            }
        } 

        //printf("test2, maxset=%d, nsetbuf=%d, sets[0]=%p\n", maxset, nsetbuf, sets[0]); fflush(stdout);
        for(int i=0; i< maxset; i++){
            if(sets[i] != NULL){
                ReducerHashBucket::mem_bytes-=setsize;
                mem_aligned_free(sets[i]);
            }
        }

        //printf("test3\n"); fflush(stdout);
        mem_aligned_free(sets);
        //printf("test4\n"); fflush(stdout);
    }

    ReducerUnique* insertElem(ReducerUnique *elem);
 
    int compare(char *key, int keybytes, ReducerUnique *);

    int getkey(ReducerUnique *, char **pkey, int *pkeybytes);

    ReducerUnique* BeginUnique(){

        iunique=0;
        if(iunique>=nunique) return NULL;
        if(nbuf>0){
            ibuf=0;
            cur_buf=buffers[ibuf];
            cur_off=0;
            cur_unique=(ReducerUnique*)(cur_buf+cur_off);
            cur_off+=sizeof(ReducerUnique);
            cur_off+=cur_unique->keybytes;
            iunique++;
        }

        //printf("iunique=%ld, nunique=%ld, cur_unique=%p\n", iunique, nunique, cur_unique);
        //fflush(stdout);

        if(cur_unique==NULL)
             LOG_ERROR("%s", "Error: unique strcuture is NULL!\n");

        return cur_unique;
    }

    ReducerUnique* NextUnique(){

        if(iunique>=nunique) return NULL; 
        cur_unique=(ReducerUnique*)(cur_buf+cur_off);
        //printf("usize=%d, cur_off=%d,ibuf=%d,nbuf=%d\n",\
            usize,cur_off,ibuf,nbuf);fflush(stdout);
        if((usize-cur_off)<sizeof(ReducerUnique) || \
            cur_unique->key==NULL){
            if(ibuf<nbuf){
                ibuf+=1;
                cur_buf=buffers[ibuf];
                cur_off=0;
                //printf("iunique=%d\n", iunique);
                cur_unique=(ReducerUnique*)(cur_buf+cur_off);
                iunique++;
            } 
        }else{
            cur_unique=(ReducerUnique*)(cur_buf+cur_off);
            iunique++;
        }
        //printf("iunique=%ld, nunique=%ld, cur_unique=%p\n", iunique, nunique, cur_unique); 
        //fflush(stdout);
        cur_off+=sizeof(ReducerUnique);
        cur_off+=cur_unique->keybytes;

        if(cur_unique==NULL)
             LOG_ERROR("%s", "Error: unique strcuture is NULL!\n");

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
            //printf("iset=%ld, nbucket=%d, pset=%p\n", iset, nbucket, pset); 
            //fflush(stdout);
        }
        return pset;
    }


private:
    ReducerUnique *cur_unique;

    int pid;

    int64_t nset, iset;
    int  setsize, maxset, nsetbuf, isetbuf;
    char **sets;

    int64_t mvbytes;

public:
    static int64_t mem_bytes;
};

}

#endif
