#ifndef HASH_BUCKET_H
#define HASH_BUCKET_H

#include <stdio.h>
#include <stdlib.h>
//#include "hashbucket.h"
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

        nunique=0;
    }

    ~HashBucket(){
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

    ElemType* findElem(char *key, int keybytes){    

        int ibucket = hashlittle(key, keybytes, 0) % nbucket;

        ElemType *ptr = buckets[ibucket];

        //printf("find: key=%s,elem=%p\n", key, ptr); fflush(stdout);

        while(ptr!=NULL){
            if(compare(key, keybytes, ptr) != 0)
                break;
            ptr=ptr->next;
        }
        return ptr;
    }


    ElemType* insertElem(ElemType *elem){
        char *key;
        int  keybytes;

        getkey(elem, &key, &keybytes);

        nunique+=1;

        if(nbuf == (nunique/nbucket)){
            buffers[nbuf]=(char*)mem_aligned_malloc(\
                MEMPAGE_SIZE, usize);
            nbuf+=1;
        }

        ElemType *newelem=(ElemType*)buffers[nunique/nbucket]+nunique%nbucket;
        memcpy(newelem, elem, sizeof(ElemType));

        int ibucket = hashlittle(key, keybytes, 0) % nbucket;

        ElemType *ptr = buckets[ibucket];

        // New unique key
        if(ptr==NULL){
            buckets[ibucket] = newelem;
            printf("insert: key=%s, elem=%p, kv=%p\n", key, newelem, newelem->kv);
            fflush(stdout);
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
                buckets[ibucket] = newelem;
                elem->next = tmp;
                return NULL;
            }
        }
        return ptr;
    }

protected:
    int        nbucket;
    ElemType **buckets;
    
    int     maxbuf;
    int     usize, nbuf;
    char  **buffers;

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
 
    int compare(char *key, int keybytes, CombinerUnique *);

    int getkey(CombinerUnique *, char **pkey, int *pkeybytes);
};

}

#endif
