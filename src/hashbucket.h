#ifndef HASH_BUCKET_H
#define HASH_BUCKET_H

#include <stdio.h>
#include <stdlib.h>
#include "const.h"
#include "hash.h"
#include "memory.h"
//#include "kvcontainer.h"
#include "log.h"
#include "config.h"

namespace MIMIR_NS {

template <typename ElemType>
class HashBucket {
public:
    HashBucket() {
        //kv = _kv;

        nbucket = (uint32_t) pow(2, BUCKET_COUNT);

        //printf("nbucket=%d, BUCKET_COUNT=%d\n", nbucket, BUCKET_COUNT);

        usize = nbucket * (int) sizeof(ElemType);
        maxbuf = MAX_PAGE_COUNT;

        buckets = (ElemType**) mem_aligned_malloc(MEMPAGE_SIZE, sizeof(ElemType*) *nbucket);
        buffers = (char**) mem_aligned_malloc(MEMPAGE_SIZE, maxbuf * sizeof(char*));

        for (int i = 0; i < nbucket; i++)
            buckets[i] = NULL;

        nbuf = 0;
        for (int i = 0; i < maxbuf; i++)
            buffers[i] = NULL;

        cur_buf = NULL;
        cur_off = 0;

        nunique = 0;

        LOG_PRINT(DBG_GEN, "HashBucket: create nbucket=%d\n", nbucket);
    }
    virtual ~ HashBucket() {
        mem_aligned_free(buffers);
        mem_aligned_free(buckets);

        LOG_PRINT(DBG_GEN, "HashBucket: destroy.\n");
    }

    // Comapre key with elem
    virtual int compare(const char *key, int keybytes, ElemType*) = 0;
    virtual ElemType *insertElem(ElemType *elem) = 0;

    virtual void clear() {
        for (int i = 0; i < nbucket; i++)
            buckets[i] = NULL;
        nunique = 0;
    }

    ElemType* findElem(const char *key, int keybytes) {

        uint32_t ibucket = hashlittle(key, keybytes, 0) % nbucket;

        ElemType* ptr = buckets[ibucket];

         while (ptr != NULL) {
            if (compare(key, keybytes, ptr) != 0)
                break;
            ptr = ptr->next;
        }
        return ptr;
    }

    int64_t get_nunique() {
        return nunique;
    }

protected:
    int nbucket;
    ElemType **buckets;

    int usize, maxbuf, nbuf, ibuf;
    char **buffers;
    char *cur_buf;
    int cur_off;

    int64_t iunique;
    int64_t nunique;

};

struct CombinerUnique {
    char *kv;
    CombinerUnique *next;
};

class CombinerHashBucket:public HashBucket <CombinerUnique> {
public:
    CombinerHashBucket()
        : HashBucket <CombinerUnique> () {}
    ~CombinerHashBucket() {
        for (int i = 0; i < maxbuf; i++) {
            if (buffers[i] != NULL) {
                CombinerHashBucket::mem_bytes -= usize;
                mem_aligned_free(buffers[i]);
            }
        }
    }

    CombinerUnique* insertElem(CombinerUnique *elem);

    int compare(const char *key, int keybytes, CombinerUnique*);

public:
    static int64_t mem_bytes;
};

struct ReducerSet {
    int pid;
    int64_t ivalue;
    int64_t nvalue;
    int64_t mvbytes;
    int *soffset;
    char *voffset;
    char *curoff;
    ReducerSet *next;
};

struct ReducerUnique {
    char *key;
    int keybytes;
    int64_t nvalue;
    int64_t mvbytes;
    ReducerSet *firstset;
    ReducerSet *lastset;
    ReducerUnique *next;
};

class ReducerHashBucket:public HashBucket <ReducerUnique> {
public:
    ReducerHashBucket()
        : HashBucket <ReducerUnique> () {
        maxset = MAX_PAGE_COUNT;
        setsize = nbucket * (int) sizeof(ReducerSet);

        sets = (char**) mem_aligned_malloc(MEMPAGE_SIZE, maxset * sizeof(char*));
        for (int i = 0; i < maxset; i++)
            sets[i] = NULL;

        isetbuf = nsetbuf = 0;
        iset = nset = 0;
        mvbytes = 0;
        cur_unique = NULL;
        pid = 0;
    }
    ~ReducerHashBucket() {
        for (int i = 0; i < maxbuf; i++) {
            if (buffers[i] != NULL) {
                ReducerHashBucket::mem_bytes -= usize;
                mem_aligned_free(buffers[i]);
            }
        }
        for (int i = 0; i < maxset; i++) {
            if (sets[i] != NULL) {
                ReducerHashBucket::mem_bytes -= setsize;
                mem_aligned_free(sets[i]);
            }
        }
        mem_aligned_free(sets);
    }

    ReducerUnique* insertElem(ReducerUnique *elem);

    int compare(const char *key, int keybytes, ReducerUnique*);

    ReducerUnique* BeginUnique() {
        iunique = 0;
        if (iunique >= nunique)
            return NULL;
        if (nbuf > 0) {
            ibuf = 0;
            cur_buf = buffers[ibuf];
            cur_off = 0;
            cur_unique = (ReducerUnique*) (cur_buf + cur_off);
            cur_off += (int) sizeof(ReducerUnique);
            cur_off += cur_unique->keybytes;
            iunique++;
        }

        if (cur_unique == NULL)
            LOG_ERROR("Error: unique strcuture is NULL!\n");

        return cur_unique;
    }

    ReducerUnique* NextUnique() {
        if (iunique >= nunique)
            return NULL;
        cur_unique = (ReducerUnique*) (cur_buf + cur_off);
        if ((usize - cur_off) < (int) sizeof(ReducerUnique)
            || cur_unique->key == NULL) {
            if (ibuf < nbuf) {
                ibuf += 1;
                cur_buf = buffers[ibuf];
                cur_off = 0;
                cur_unique = (ReducerUnique*) (cur_buf + cur_off);
                iunique++;
            }
        }
        else {
            cur_unique = (ReducerUnique*) (cur_buf + cur_off);
            iunique++;
        }
        cur_off += (int) sizeof(ReducerUnique);
        cur_off += cur_unique->keybytes;

        if (cur_unique == NULL)
            LOG_ERROR("Error: unique strcuture is NULL!\n");

        return cur_unique;
    }

    ReducerSet *BeginSet() {
        ReducerSet *pset = NULL;
        if (nset <= 0)
            return NULL;
        else {
            iset = 0;
            pset = (ReducerSet*) sets[iset / nbucket] + iset % nbucket;
            return pset;
        }
        return pset;
    }

    ReducerSet *NextSet() {
        ReducerSet *pset = NULL;
        iset += 1;
        if (iset >= nset)
            return NULL;
        else {
            pset = (ReducerSet*) sets[iset / nbucket] + iset % nbucket;
        }
        return pset;
    }


public:
    ReducerUnique *cur_unique;

    int pid;

    int64_t nset, iset;
    int setsize, maxset, nsetbuf, isetbuf;
    char **sets;

    int64_t mvbytes;

public:
    static int64_t mem_bytes;
};

}

#endif
