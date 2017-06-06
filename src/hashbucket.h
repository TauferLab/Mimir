/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef HASH_BUCKET_H
#define HASH_BUCKET_H

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <vector>
#include "globals.h"
#include "hash.h"
#include "memory.h"
#include "log.h"
#include "config.h"
#include "stat.h"
#include "hash.h"
#include "memory.h"
#include "hashbucket.h"
#include "serializer.h"

namespace MIMIR_NS {

struct CombinerVal {
    char *kv;
};

struct ReducerVal {
    int   valbytes;
    char *values_start;
    char *values_end;
};

template <typename ValType>
class HashBucket {
  public:
    struct HashEntry {
        char *key;             // key bytes
        int   keysize;         // key size
        ValType val;           // val
        HashEntry *next;
    };

    HashBucket(bool iscopykey = false) {

        this->iscopykey = iscopykey;

        nbucket = (uint32_t) pow(2, BUCKET_COUNT);
        buckets = (HashEntry**) mem_aligned_malloc(MEMPAGE_SIZE,
                                                   sizeof(HashEntry*) * nbucket);
        for (int i = 0; i < nbucket; i++) buckets[i] = NULL;

        buf_size = DATA_PAGE_SIZE;
        buf_idx = 0;
        buf_off = 0;

        nunique = 0;

        LOG_PRINT(DBG_GEN, "HashBucket: create nbucket=%d\n", nbucket);
    }

    virtual ~HashBucket() {

        mem_aligned_free(buckets);

        for (size_t i = 0; i < buffers.size(); i++) {
            if (buffers[i] != NULL)
                mem_aligned_free(buffers[i]);
        }

        LOG_PRINT(DBG_GEN, "HashBucket: destroy.\n");
    }

    ValType* findEntry(char *key, int keysize) {

        // Compute bucket index
        uint32_t ibucket = hashlittle(key, keysize, 0) % nbucket;

        // Search the key
        HashEntry* ptr = this->buckets[ibucket];
        while (ptr != NULL) {
            if (ptr->keysize == keysize 
                && memcmp(ptr->key, key, keysize) == 0) {
                break;
            }
            ptr = ptr->next;
        }

        if (ptr) return &(ptr->val);

        return NULL;
    }

    virtual void insertEntry(char *key, int keysize, ValType *val) {

        // Add a new buffer
        if (buf_idx == buffers.size()) {
            char *buffer = (char*) mem_aligned_malloc(MEMPAGE_SIZE, buf_size);
            buffers.push_back(buffer);
            buf_off = 0;
        }

        // Add a new buffer
        int entry_size = sizeof(HashEntry);
        if (iscopykey) entry_size += keysize;
        if (entry_size > buf_size) LOG_ERROR("Entry is too long!\n");
        if (buf_size - buf_off < entry_size) {
            memset(buffers[buf_idx] + buf_off, 0, buf_size - buf_off);
            buf_idx += 1;
            buf_off = 0;
            if (buf_idx == buffers.size()) {
                char *buffer = (char*) mem_aligned_malloc(MEMPAGE_SIZE, buf_size);
                buffers.push_back(buffer);
            }
        }

        // Add entry
        HashEntry* entry = (HashEntry*)(buffers[buf_idx] + buf_off);
        entry->keysize = keysize;
        entry->val = *val;
        entry->next = NULL;
        buf_off += sizeof(HashEntry);
        if (iscopykey) {
            memcpy(buffers[buf_idx] + buf_off, key, keysize);
            entry->key = buffers[buf_idx] + buf_off;
            buf_off += keysize;
        } else {
            entry->key = key;
        }

        // Add to the list
        uint32_t ibucket = hashlittle(key, keysize, 0) % nbucket;
        if (buckets[ibucket] == NULL) {
            buckets[ibucket] = entry;
        } else {
            HashEntry *tmp = buckets[ibucket];
            buckets[ibucket] = entry;
            entry->next = tmp;
        }

        nunique ++;
    }

    void open() {
        iter_buf_idx = 0;
        iter_buf_off = 0;
        iunique = 0;
    }

    void close() {
    }

    HashEntry *next() {

        if (iunique >= nunique) return NULL;

        if (iter_buf_idx == buf_idx && iter_buf_off >= buf_off)
            return NULL;

        if ((buf_size - iter_buf_off) < sizeof(HashEntry)) {
            iter_buf_idx += 1;
            iter_buf_off = 0;
        } else {
            HashEntry *entry = (HashEntry*)(buffers[iter_buf_idx] + iter_buf_off);
            if (entry->key == NULL) {
                iter_buf_idx += 1;
                iter_buf_off = 0;
            }
        }

        if (iter_buf_idx == buf_idx && iter_buf_off >= buf_off)
            return NULL;

        HashEntry *entry = (HashEntry*)(buffers[iter_buf_idx] + iter_buf_off);
        if (iscopykey) iter_buf_off += (sizeof(HashEntry) + entry->keysize);
        else iter_buf_off += sizeof(HashEntry);

        iunique += 1;

        return entry;
    }

    int64_t get_nunique() {
        return nunique;
    }

    virtual void clear() {
        for (int i = 0; i < nbucket; i++)
            buckets[i] = NULL;
        nunique = 0;
        buf_idx = 0;
        buf_off = 0;
    }

protected:
    bool iscopykey;

    int nbucket;
    HashEntry **buckets;

    int buf_size, buf_idx, buf_off;
    std::vector<char*> buffers;

    int iter_buf_idx, iter_buf_off;

    uint64_t iunique, nunique;
};

}

#endif
