#include <stdio.h>
#include <stdlib.h>
#include "hashbucket.h"
#include "const.h"
#include "hash.h"
#include "memory.h"
#include "stat.h"
#include "recordformat.h"

using namespace MIMIR_NS;

int64_t CombinerHashBucket::mem_bytes = 0;
int64_t ReducerHashBucket::mem_bytes = 0;

CombinerUnique* CombinerHashBucket::insertElem(CombinerUnique *elem)
{
    char *key = NULL, *value = NULL;
    int keybytes = 0, valuebytes = 0, kvsize = 0;

    KVRecord record(ksize, vsize);
    record.set_buffer(elem->kv);
    key = record.get_key();
    keybytes = record.get_key_size();
    value = record.get_val();
    valuebytes = record.get_val_size();
    kvsize = record.get_record_size();
    //GET_KV_VARS(kv->ksize, kv->vsize, elem->kv, key, keybytes,
    //            value, valuebytes, kvsize);

    if (nbuf == (nunique / nbucket) && buffers[nbuf] == NULL) {
        buffers[nbuf] = (char*) mem_aligned_malloc(MEMPAGE_SIZE, usize);
        CombinerHashBucket::mem_bytes += usize;
        PROFILER_RECORD_COUNT(COUNTER_COMBINE_BUCKET,
                              (uint64_t) CombinerHashBucket::mem_bytes, OPMAX);
        nbuf += 1;
    }

    CombinerUnique *newelem = (CombinerUnique*) buffers[nunique / nbucket]
        + nunique % nbucket;

    nunique += 1;

    newelem->kv = elem->kv;
    newelem->next = NULL;

    uint32_t ibucket = hashlittle(key, keybytes, 0) % nbucket;

    CombinerUnique *ptr = buckets[ibucket];

    // New unique key
    if (ptr == NULL) {
        buckets[ibucket] = newelem;
    }
    else {
        CombinerUnique *tmp = buckets[ibucket];
        buckets[ibucket] = newelem;
        newelem->next = tmp;
        return NULL;
    }
    return newelem;
}

int CombinerHashBucket::compare(const char *key, int keybytes, CombinerUnique *u)
{
    char *ukey = NULL, *uvalue = NULL;
    int ukeybytes = 0, uvaluebytes = 0, kvsize = 0;

    KVRecord record(ksize, vsize);
    record.set_buffer(u->kv);
    ukey = record.get_key();
    ukeybytes = record.get_key_size();
    uvalue = record.get_val();
    uvaluebytes = record.get_val_size();
    kvsize = record.get_record_size();
    //GET_KV_VARS(kv->ksize, kv->vsize, u->kv, ukey, ukeybytes,
    //            uvalue, uvaluebytes, kvsize);

    if (keybytes == ukeybytes && memcmp(key, ukey, keybytes) == 0)
        return 1;

    return 0;
}

ReducerUnique* ReducerHashBucket::insertElem(ReducerUnique *elem)
{
    char *key = elem->key;
    int keybytes = elem->keybytes;

    // Get the bucket index
    uint32_t ibucket = hashlittle(key, keybytes, 0) % nbucket;

    // Get the bucket header
    ReducerSet *set = NULL;
    ReducerUnique *ptr = buckets[ibucket];

    // Find if there is a unique structure existing
    while (ptr != NULL) {
        if (compare(key, keybytes, ptr) != 0)
            break;
        ptr = ptr->next;
    }

    // Get the MV size
    int64_t onemvbytes = elem->mvbytes;
    if (vsize == KVGeneral)
        onemvbytes += sizeof(int);

    // Add a new partition if nessary
    if (mvbytes + onemvbytes > DATA_PAGE_SIZE) {
        mvbytes = 0;
        pid++;
    }

    // New unique key
    if (ptr == NULL) {

        nunique += 1;

        // Insert a new buffer
        if (cur_buf == NULL
            || (usize - cur_off) < (int) sizeof(ReducerUnique) + elem->keybytes) {

            buffers[nbuf] = (char*) mem_aligned_malloc(MEMPAGE_SIZE, usize);
            ReducerHashBucket::mem_bytes += usize;

            PROFILER_RECORD_COUNT(COUNTER_REDUCE_BUCKET,
                                  (uint64_t) ReducerHashBucket::mem_bytes, OPMAX); 

            if (cur_buf != NULL)
                memset(cur_buf + cur_off, 0, usize - cur_off);

            cur_buf = buffers[nbuf];
            cur_off = 0;

            nbuf += 1;
        }

        // Get the ReducerUnique structure
        ReducerUnique *newelem = (ReducerUnique*) (cur_buf + cur_off);
        cur_off += (int) sizeof(ReducerUnique);

        // Set the element information
        newelem->key = cur_buf + cur_off;
        newelem->keybytes = elem->keybytes;
        newelem->nvalue = 1;
        newelem->mvbytes = elem->mvbytes;

        newelem->firstset = NULL;
        newelem->lastset = NULL;
        newelem->next = NULL;

        memcpy(cur_buf + cur_off, elem->key, elem->keybytes);
        cur_off += elem->keybytes;

        // Insert unique to the bucket
        if (buckets[ibucket] == NULL)
            buckets[ibucket] = newelem;
        else {
            ReducerUnique *tmp = buckets[ibucket];
            buckets[ibucket] = newelem;
            newelem->next = tmp;
        }

        // Insert new set buffer
        if (nsetbuf == (nset / nbucket)) {
            sets[nsetbuf] = (char*) mem_aligned_malloc(MEMPAGE_SIZE, setsize);

            ReducerHashBucket::mem_bytes += setsize;
            PROFILER_RECORD_COUNT(COUNTER_REDUCE_BUCKET,
                                  (uint64_t) ReducerHashBucket::mem_bytes, OPMAX);

            nsetbuf += 1;
        }

        // Set the information
        set = (ReducerSet*) sets[nset / nbucket] + nset % nbucket;
        nset += 1;

        // Set set information
        set->pid = pid;
        set->ivalue = 0;
        set->nvalue = 1;
        set->mvbytes = elem->mvbytes;
        set->soffset = NULL;
        set->voffset = NULL;
        set->next = NULL;

        // Insert set to the unique structure
        newelem->firstset = set;
        newelem->lastset = set;

        ptr = newelem;
    }
    else {
        // Add the MV information
        ptr->nvalue += 1;
        ptr->mvbytes += elem->mvbytes;

        if (ptr->lastset->pid != pid) {
            // Insert set buffer
            if (nsetbuf == (nset / nbucket)) {
                sets[nsetbuf] = (char*) mem_aligned_malloc(MEMPAGE_SIZE, setsize);

                ReducerHashBucket::mem_bytes += setsize;
                PROFILER_RECORD_COUNT(COUNTER_REDUCE_BUCKET,
                                      (uint64_t) ReducerHashBucket::mem_bytes, OPMAX);

                nsetbuf += 1;
            }

            // Get a new set
            set = (ReducerSet*) sets[nset / nbucket] + nset % nbucket;
            nset += 1;

            // Set information
            set->pid = pid;
            set->ivalue = 0;
            set->nvalue = 0;
            set->mvbytes = 0;

            set->soffset = NULL;
            set->voffset = NULL;
            set->next = NULL;

            // Move the set to next

            ptr->lastset->next = set;
            ptr->lastset = set;

        }
        else {
            set = ptr->lastset;
        }

        // Set the set information
        set->nvalue += 1;
        set->mvbytes += elem->mvbytes;
    }

    mvbytes += onemvbytes;

    return ptr;
}

int ReducerHashBucket::compare(const char *key, int keybytes, ReducerUnique *u)
{
    if (keybytes == u->keybytes && memcmp(key, u->key, keybytes) == 0)
        return 1;

    return 0;
}
