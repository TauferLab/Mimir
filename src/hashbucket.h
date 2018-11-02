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

struct CombinerVal
{
    char *kv;
};

struct ReducerVal
{
    int64_t valbytes;
    char *values_start;
    char *values_end;
};

struct EmptyVal
{
};

template <typename ValType = EmptyVal>
class HashBucket
{
  public:
    struct HashEntry
    {
        char *key;   // key bytes
        int keysize; // key size
        ValType val; // val
        HashEntry *next;
    };

    HashBucket(int scale = 1, bool iscopykey = false, bool isremove = false)
    {
        this->scale = scale;
        this->iscopykey = iscopykey;
        this->isremove = isremove;

        nbucket = BUCKET_COUNT;
        buckets = (HashEntry **) mem_aligned_malloc(
            MEMPAGE_SIZE, sizeof(HashEntry *) * nbucket);
        for (int i = 0; i < nbucket; i++) buckets[i] = NULL;

        mem_bytes += sizeof(HashEntry *) * nbucket;

        PROFILER_RECORD_COUNT(COUNTER_HASH_BUCKET, this->mem_bytes, OPMAX);

        buf_size = (int) DATA_PAGE_SIZE;
        buf_idx = 0;
        buf_off = 0;

        nunique = 0;

        LOG_PRINT(DBG_GEN, "HashBucket: create nbucket=%d\n", nbucket);
    }

    virtual ~HashBucket()
    {
        mem_bytes -= sizeof(HashEntry *) * nbucket;

        mem_aligned_free(buckets);

        for (size_t i = 0; i < buffers.size(); i++) {
            mem_bytes -= buf_size;
            if (buffers[i] != NULL) mem_aligned_free(buffers[i]);
        }

        LOG_PRINT(DBG_GEN, "HashBucket: destroy.\n");
    }

    void print()
    {
        for (int i = 0; i < nbucket; i++) {
            int64_t nitem = 0;
            HashEntry *ptr = this->buckets[i];
            while (ptr != NULL) {
                nitem++;
                ptr = ptr->next;
            }
            LOG_PRINT(DBG_GEN, "Hash entry %d: %ld\n", i, nitem);
        }
    }

    ValType *findEntry(char *key, int keysize)
    {
        // Compute bucket index
        uint32_t ibucket = (hashlittle(key, keysize, 0) / scale) % nbucket;

        // Search the key
        HashEntry *ptr = this->buckets[ibucket];
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

    ValType *updateEntry(char *key, int keysize, char *newkey)
    {
        // Compute bucket index
        uint32_t ibucket = (hashlittle(key, keysize, 0) / scale) % nbucket;

        // Search the key
        HashEntry *ptr = this->buckets[ibucket];
        while (ptr != NULL) {
            if (ptr->keysize == keysize
                && memcmp(ptr->key, key, keysize) == 0) {
                break;
            }
            ptr = ptr->next;
        }

        if (ptr) {
            ptr->key = newkey;
            return &(ptr->val);
        }

        LOG_ERROR("Cannot find the entry key=%s, ibucket=%d!\n", key, ibucket);

        return NULL;
    }

    virtual void removeEntry(char *key, int keysize)
    {
        if (!isremove) {
            LOG_ERROR("This hash bucket does not support remove function!\n");
        }

        // Compute bucket index
        uint32_t ibucket = (hashlittle(key, keysize, 0) / scale) % nbucket;

        // Search the key
        HashEntry *ptr = this->buckets[ibucket];
        HashEntry *pre_ptr = ptr;
        while (ptr != NULL) {
            if (ptr->keysize == keysize
                && memcmp(ptr->key, key, keysize) == 0) {
                break;
            }
            pre_ptr = ptr;
            ptr = ptr->next;
        }
        if (ptr) {
            int ssize = (int) sizeof(HashEntry);
            if (iscopykey) ssize += ptr->keysize;
            slices.insert(std::make_pair((char *) ptr, ssize));
            if (ptr == buckets[ibucket])
                buckets[ibucket] = ptr->next;
            else
                pre_ptr->next = ptr->next;
            nunique--;
        }
    }

    virtual void insertEntry(char *key, int keysize, ValType *val)
    {
        HashEntry *entry = NULL;
        int entry_size = sizeof(HashEntry);
        if (iscopykey) entry_size += keysize;

        if (isremove) {
            // Find a slice to store the entry
            std::unordered_map<char *, int>::iterator iter;
            for (iter = this->slices.begin(); iter != this->slices.end();
                 iter++) {
                char *sbuf = iter->first;
                int ssize = iter->second;

                if (ssize >= entry_size) {
                    entry = (HashEntry *) (sbuf + (ssize - entry_size));
                    entry->keysize = keysize;
                    entry->val = *val;
                    entry->next = NULL;

                    if (iscopykey) {
                        memcpy((char *) entry + sizeof(HashEntry), key,
                               keysize);
                        entry->key = (char *) entry + sizeof(HashEntry);
                    }
                    else {
                        entry->key = key;
                    }

                    if (iter->second == entry_size)
                        this->slices.erase(iter);
                    else
                        this->slices[iter->first] -= entry_size;

                    // Add to the list
                    uint32_t ibucket
                        = (hashlittle(key, keysize, 0) / scale) % nbucket;
                    if (buckets[ibucket] == NULL) {
                        buckets[ibucket] = entry;
                    }
                    else {
                        HashEntry *tmp = buckets[ibucket];
                        buckets[ibucket] = entry;
                        entry->next = tmp;
                    }

                    nunique++;

                    return;
                }
            }
        }

        // Add a new buffer
        if (buf_idx == (int) buffers.size()) {
            char *buffer = (char *) mem_aligned_malloc(MEMPAGE_SIZE, buf_size);
            mem_bytes += buf_size;
            PROFILER_RECORD_COUNT(COUNTER_HASH_BUCKET, this->mem_bytes, OPMAX);
            buffers.push_back(buffer);
            buf_off = 0;
        }

        // Add a new buffer
        if (entry_size > buf_size) LOG_ERROR("Entry is too long!\n");
        if (buf_size - buf_off < entry_size) {
            memset(buffers[buf_idx] + buf_off, 0, buf_size - buf_off);
            buf_idx += 1;
            buf_off = 0;
            if (buf_idx == (int) buffers.size()) {
                char *buffer
                    = (char *) mem_aligned_malloc(MEMPAGE_SIZE, buf_size);
                mem_bytes += buf_size;
                PROFILER_RECORD_COUNT(COUNTER_HASH_BUCKET, this->mem_bytes,
                                      OPMAX);
                buffers.push_back(buffer);
            }
        }

        // Add entry
        entry = (HashEntry *) (buffers[buf_idx] + buf_off);
        entry->keysize = keysize;
        entry->val = *val;
        entry->next = NULL;
        buf_off += (int) sizeof(HashEntry);
        if (iscopykey) {
            memcpy(buffers[buf_idx] + buf_off, key, keysize);
            entry->key = buffers[buf_idx] + buf_off;
            buf_off += keysize;
        }
        else {
            entry->key = key;
        }

        // Add to the list
        uint32_t ibucket = (hashlittle(key, keysize, 0) / scale) % nbucket;
        if (buckets[ibucket] == NULL) {
            buckets[ibucket] = entry;
        }
        else {
            HashEntry *tmp = buckets[ibucket];
            buckets[ibucket] = entry;
            entry->next = tmp;
        }

        nunique++;
    }

    void open()
    {
        iter_buf_idx = 0;
        iter_buf_off = 0;
        iunique = 0;
    }

    void close() {}

    HashEntry *next()
    {
        if (iunique >= nunique) return NULL;

        if (iter_buf_idx == buf_idx && iter_buf_off >= buf_off) return NULL;

        while (1) {
            auto iter = this->slices.find(buffers[iter_buf_idx] + iter_buf_off);
            if (iter != this->slices.end()) {
                iter_buf_off += iter->second;
                continue;
            }

            if ((buf_size - iter_buf_off) < (int) sizeof(HashEntry)) {
                iter_buf_idx += 1;
                iter_buf_off = 0;
                continue;
            }
            else {
                HashEntry *entry
                    = (HashEntry *) (buffers[iter_buf_idx] + iter_buf_off);
                if (entry->key == NULL) {
                    iter_buf_idx += 1;
                    iter_buf_off = 0;
                    continue;
                }
            }
            if (iter_buf_idx == buf_idx && iter_buf_off >= buf_off) return NULL;

            break;
        }

        HashEntry *entry = (HashEntry *) (buffers[iter_buf_idx] + iter_buf_off);
        if (iscopykey)
            iter_buf_off += ((int) sizeof(HashEntry) + entry->keysize);
        else
            iter_buf_off += (int) sizeof(HashEntry);

        iunique += 1;

        return entry;
    }

    int64_t get_nunique() { return nunique; }

    virtual void clear()
    {
        for (int i = 0; i < nbucket; i++) buckets[i] = NULL;
        nunique = 0;
        buf_idx = 0;
        buf_off = 0;
        slices.clear();
    }

  protected:
    int scale;
    bool iscopykey;
    bool isremove;

    int nbucket;
    HashEntry **buckets;

    int buf_size, buf_idx, buf_off;
    std::vector<char *> buffers;

    int iter_buf_idx, iter_buf_off;

    uint64_t iunique, nunique;

    std::unordered_map<char *, int> slices;

  public:
    static uint64_t mem_bytes;
};

template <typename ValType>
uint64_t HashBucket<ValType>::mem_bytes = 0;

} // namespace MIMIR_NS

#endif
