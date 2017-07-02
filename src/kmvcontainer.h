/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_KMV_CONTAINER_H
#define MIMIR_KMV_CONTAINER_H

#include "config.h"
#include "hashbucket.h"
#include "container.h"
#include "interface.h"
#include "serializer.h"

namespace MIMIR_NS {

template <typename KeyType, typename ValType>
class KMVItem;

template <typename KeyType, typename ValType>
class KMVContainer {
  public:
    KMVContainer(int keycount, int valcount) {
        this->keycount = keycount;
        this->valcount = valcount;

        kmvcount = 0;

        keyarray = (char*) mem_aligned_malloc(MEMPAGE_SIZE, MAX_RECORD_SIZE);
        h = new HashBucket<ReducerVal>(true);
        ser = new Serializer<KeyType, ValType>(keycount, valcount);

        mem_bytes = 0;
    }

    virtual ~KMVContainer() {
        delete ser;
        delete h;
        mem_aligned_free(keyarray);
        PROFILER_RECORD_COUNT(COUNTER_MAX_KMV_PAGES, this->mem_bytes, OPMAX);
    }

    virtual int open() {
        kmv = new KMVItem<KeyType, ValType>(keycount, valcount);
        h->open();
        return 0;
    }

    virtual void close() {
        delete kmv;
        h->close();
    }

    virtual KMVItem<KeyType, ValType>* read() {
        HashBucket<ReducerVal>::HashEntry *entry = h->next();
        if (entry != NULL) {
            kmv->set_entry(entry);
            return kmv;
        }
        return NULL;
    }

    virtual uint64_t get_record_count() { return kmvcount; }

    void convert(Readable<KeyType,ValType> *kv) {
        int valbytes = 0;
        typename SafeType<KeyType>::type key[keycount];
        typename SafeType<ValType>::type val[valcount];
        ReducerVal *rdc_val = NULL;

        LOG_PRINT(DBG_GEN, "MapReduce: convert start.\n");

        kv->open();
        while ((kv->read(key, val)) == 0) {
            keybytes = ser->key_to_bytes(key, keyarray, MAX_RECORD_SIZE);
            if ((rdc_val = h->findEntry(keyarray, keybytes)) == NULL) {
                valbytes = ser->get_val_bytes(val);
                ReducerVal tmpval;
                tmpval.valbytes = valbytes;
                h->insertEntry(keyarray, keybytes, &tmpval);
            } else {
                rdc_val->valbytes += valbytes;
            }
        }
        kv->close();

        std::vector<HashBucket<ReducerVal>::HashEntry*> entries;
        h->open();
        HashBucket<ReducerVal>::HashEntry *entry = NULL;
        int totalsize = 0;
        while ((entry = h->next()) != NULL) {
            totalsize += entry->val.valbytes;
            entries.push_back(entry);
            if (totalsize >= DATA_PAGE_SIZE) {
                char *buffer = (char*) mem_aligned_malloc(MEMPAGE_SIZE, totalsize);
                mem_bytes += totalsize;
                buffers.push_back(buffer);
                int bufoff = 0;
                for (size_t i = 0; i < entries.size(); i++) {
                    entries[i]->val.values_start = buffer + bufoff;
                    entries[i]->val.values_end = buffer + bufoff;
                    bufoff += entries[i]->val.valbytes;
                }
                entries.clear();
                totalsize = 0;
            }
        }
        h->close();

        char *buffer = (char*) mem_aligned_malloc(MEMPAGE_SIZE, totalsize);
        mem_bytes += totalsize;
        buffers.push_back(buffer);
        int bufoff = 0;
        for (size_t i = 0; i < entries.size(); i++) {
            entries[i]->val.values_start = buffer + bufoff;
            entries[i]->val.values_end = buffer + bufoff;
            bufoff += entries[i]->val.valbytes;
        }

        kv->open();
        while ((kv->read(key, val)) == 0) {
            keybytes = ser->key_to_bytes(key, keyarray, MAX_RECORD_SIZE);
            if ((rdc_val = h->findEntry(keyarray, keybytes)) != NULL) {
                int vsize = ser->val_to_bytes(val, rdc_val->values_end,
                    rdc_val->valbytes - (int)(rdc_val->values_end - rdc_val->values_start));
                rdc_val->values_end += vsize;
            } else {
                LOG_ERROR("Error in convert!\n");
            }
        }
        kv->close();

        PROFILER_RECORD_COUNT(COUNTER_MAX_KMVS, (uint64_t)(h->get_nunique()), OPMAX);
        LOG_PRINT(DBG_GEN, "MapReduce: convert end (KMVs=%ld).\n", h->get_nunique());
    }

  private:
    HashBucket<ReducerVal> *h;
    uint64_t kmvcount;

    int keycount, valcount;
    char* keyarray;
    int keybytes;

    std::vector<char*> buffers;

    KMVItem<KeyType, ValType>* kmv;

    Serializer<KeyType, ValType> *ser;

    uint64_t  mem_bytes;
};

template <typename KeyType, typename ValType>
class KMVItem : public Readable<KeyType, ValType>   {
  public:
    KMVItem(int keycount, int valcount) {
        this->keycount = keycount;
        this->valcount = valcount;
        this->entry = NULL;
        this->valptr = NULL;
        ser = new Serializer<KeyType, ValType>(keycount, valcount);
    }

    ~KMVItem() {
        delete ser;
    }

    void set_entry(HashBucket<ReducerVal>::HashEntry* entry) {
        this->entry = entry;
    }

    int open() {
        valptr = entry->val.values_start;
        return 0;
    }

    void close() {
        return;
    }

    int read(KeyType *key, ValType *val) {
        if (valptr == entry->val.values_end) return -1;
        ser->key_from_bytes(key, entry->key, entry->keysize);
        int vsize = ser->val_from_bytes(val, valptr, 
                                        (int)(entry->val.values_end - valptr));
        valptr += vsize;
        return 0;
    }

    uint64_t get_record_count() {
        return 0;
    }

  private:
    HashBucket<ReducerVal>::HashEntry* entry;
    char *valptr;
    int keycount, valcount;
    Serializer<KeyType, ValType> *ser;
};

}

#endif
