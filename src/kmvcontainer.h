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
#include "recordformat.h"
#include "container.h"
#include "interface.h"

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
    }

    ~KMVContainer() {
        delete h;
        mem_aligned_free(keyarray);
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
        KeyType key[keycount];
        ValType val[valcount];
        ReducerVal *rdc_val = NULL;

        LOG_PRINT(DBG_GEN, "MapReduce: convert start.\n");

        kv->open();
        while ((kv->read(key, val)) == 0) {
            keybytes = Serializer::to_bytes<KeyType>(key, keycount,
                                                     keyarray, MAX_RECORD_SIZE);
            if ((rdc_val = h->findEntry(keyarray, keybytes)) == NULL) {
                valbytes = Serializer::get_bytes(val, valcount);
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
        buffers.push_back(buffer);
        int bufoff = 0;
        for (size_t i = 0; i < entries.size(); i++) {
            entries[i]->val.values_start = buffer + bufoff;
            entries[i]->val.values_end = buffer + bufoff;
            bufoff += entries[i]->val.valbytes;
        }

        kv->open();
        while ((kv->read(key, val)) == 0) {
            keybytes = Serializer::to_bytes<KeyType>(key, keycount,
                                                     keyarray, MAX_RECORD_SIZE);
            if ((rdc_val = h->findEntry(keyarray, keybytes)) != NULL) {
                int vsize = Serializer::to_bytes<ValType>(val, valcount,
                    rdc_val->values_end,
                    rdc_val->valbytes - (int)(rdc_val->values_end - rdc_val->values_start));
                rdc_val->values_end += vsize;
            } else {
                LOG_ERROR("Error in convert!\n");
            }
        }
        kv->close();

        LOG_PRINT(DBG_GEN, "MapReduce: convert end (kmvs=%ld).\n", h->get_nunique());
    }

  private:
    HashBucket<ReducerVal> *h;
    uint64_t kmvcount;

    int keycount, valcount;
    char* keyarray;
    int keybytes;

    std::vector<char*> buffers;

    KMVItem<KeyType, ValType>* kmv;
};

template <typename KeyType, typename ValType>
class KMVItem : public Readable<KeyType, ValType>   {
  public:
    KMVItem(int keycount, int valcount) {
        this->keycount = keycount;
        this->valcount = valcount;
        this->entry = NULL;
        this->valptr = NULL;
    }

    ~KMVItem() {
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
        Serializer::from_bytes<KeyType>(key, keycount,
                                        entry->key, entry->keysize);
        int vsize = Serializer::from_bytes<ValType>(val, valcount,
                                                    valptr, 
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
};

}

#endif
