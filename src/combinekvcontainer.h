/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_COMBINE_KV_CONTAINER_H
#define MIMIR_COMBINE_KV_CONTAINER_H

#include <stdio.h>
#include <stdlib.h>
#include <unordered_map>
#include "kvcontainer.h"
#include "hashbucket.h"

namespace MIMIR_NS {

template <typename KeyType, typename ValType>
class CombineKVContainer : public KVContainer<KeyType, ValType>,
      public Combinable<KeyType, ValType> {
public:
    CombineKVContainer(void (*user_combine)(Combinable<KeyType,ValType> *output,
                                            KeyType *key,
                                            ValType *val1, ValType *val2,
                                            void *ptr),
                       void *user_ptr, int keycount = 1, int valcount = 1)
        : KVContainer<KeyType,ValType>(keycount, valcount) {

        this->user_combine = user_combine;
        this->user_ptr = user_ptr;
        bucket = NULL;
    }

    ~CombineKVContainer()
    {
    }

    int open() {

        bucket = new HashBucket<CombinerVal>();

        KVContainer<KeyType,ValType>::open();

        keyarray = (char*) mem_aligned_malloc(MEMPAGE_SIZE, MAX_RECORD_SIZE);

        LOG_PRINT(DBG_GEN, "CombineKVContainer open!\n");

        return 0;
    }

    void close() {

        garbage_collection();

        mem_aligned_free(keyarray);

        delete bucket;

        KVContainer<KeyType,ValType>::close();

        LOG_PRINT(DBG_GEN, "CombineKVContainer close.\n");
    }

    int write(KeyType *key, ValType *val)
    {
        if (this->page == NULL) this->page = this->add_page();

        int kvsize = this->ser->get_kv_bytes(key, val);
        if (kvsize > this->pagesize)
            LOG_ERROR("Error: KV size (%d) is larger \
                      than one page (%ld)\n", kvsize, this->pagesize);

        keybytes = this->ser->key_to_bytes(key, keyarray, MAX_RECORD_SIZE);
        u = bucket->findEntry(keyarray, keybytes);

        if (u == NULL) {
            CombinerVal tmp;

            std::unordered_map < char *, int >::iterator iter;
            for (iter = slices.begin(); iter != slices.end(); iter++) {
                char *sbuf = iter->first;
                int ssize = iter->second;

                if (ssize >= kvsize) {
                    tmp.kv = sbuf + (ssize - kvsize);
                    this->ser->kv_to_bytes(key, val, tmp.kv, kvsize);

                    if (iter->second == kvsize)
                        slices.erase(iter);
                    else
                        slices[iter->first] -= kvsize;

                    break;
                }
            }
            if (iter == slices.end()) {
                if (kvsize + this->page->datasize > this->pagesize )
                    this->page = this->add_page();
                tmp.kv = this->page->buffer + this->page->datasize;
                this->ser->kv_to_bytes(key, val, tmp.kv, kvsize);
                this->page->datasize += kvsize;
            }
            bucket->insertEntry(tmp.kv, keybytes, &tmp);
            this->kvcount += 1;
        }
        else {
            KeyType u_key[this->keycount];
            ValType u_val[this->valcount];
            this->ser->kv_from_bytes(u_key, u_val, u->kv, MAX_RECORD_SIZE);
            user_combine(this, u_key, u_val, val, user_ptr);
        }
        return 0;
    }

    void update(KeyType *key, ValType *val)
    {
        KeyType u_key[this->keycount];
        ValType u_val[this->valcount];
        int ukvsize = this->ser->kv_from_bytes(u_key, u_val, u->kv, MAX_RECORD_SIZE);

        int kvsize = this->ser->get_kv_bytes(key, val);

        if (this->ser->compare_key(key, u_key) != 0)
            LOG_ERROR("Error: the result key of combiner is different!\n");

        if (kvsize <= ukvsize) {
            this->ser->kv_to_bytes(key, val, u->kv, kvsize);
            if (kvsize < ukvsize) {
                slices.insert(std::make_pair(u->kv + ukvsize - kvsize, ukvsize - kvsize));
            }
        }
        else {
            slices.insert(std::make_pair(u->kv, ukvsize));
            if (kvsize + this->page->datasize > this->pagesize)
                this->page = this->add_page();
            u->kv = this->page->buffer + this->page->datasize;
            this->ser->kv_to_bytes(key, val, u->kv, kvsize);
            this->page->datasize += kvsize;
        }

        return;
    }


private:
    void garbage_collection()
    {
        KeyType key[this->keycount];
        ValType val[this->valcount];
        ContainerIter dst_iter(this), src_iter(this);
        Page *dst_page = NULL, *src_page = NULL;
        int64_t dst_off = 0, src_off = 0;
        int kvsize;

        if (!slices.empty()) {

            LOG_PRINT(DBG_GEN, "KVContainer garbage collection: slices=%ld\n",
                      slices.size());

            dst_page = dst_iter.next();
            while ((src_page = src_iter.next()) != NULL) {
                src_off = 0;
                while (src_off < src_page->datasize) {
                    char *src_buf = src_page->buffer + src_off;
                    std::unordered_map < char *, int >::iterator iter = slices.find(src_buf);
                    if (iter != slices.end()) {
                        src_off += iter->second;
                    }
                    else {
                        int kvsize = this->ser->kv_from_bytes(key, val,
                                        src_buf, src_page->datasize - src_off);
                        if (dst_page != src_page || dst_off != src_off) {
                            if (dst_off + kvsize > this->pagesize) {
                                dst_page->datasize = dst_off;
                                dst_page = dst_iter.next();
                                dst_off = 0;
                            }
                            for (int kk = 0; kk < kvsize; kk++) {
                                dst_page->buffer[dst_off + kk] = src_page->buffer[src_off + kk];
                            }
                        }
                        src_off += kvsize;
                        dst_off += kvsize;
                    }
                }
                if (src_page == dst_page && src_off == dst_off) {
                    dst_page = dst_iter.next();
                    dst_off = 0;
                }
            }
            if (dst_page != NULL) dst_page->datasize = dst_off;
            while ((dst_page = dst_iter.next()) != NULL) {
                dst_page->datasize = 0;
            }
            slices.clear();
        }
        bucket->clear();
    }

    void (*user_combine)(Combinable<KeyType,ValType> *output,
                         KeyType *key, ValType *val1, ValType *val2, void *ptr);
    void *user_ptr;
    std::unordered_map<char*, int> slices;
    HashBucket<CombinerVal> *bucket;
    CombinerVal *u;
    char* keyarray;
    int keybytes;
};

}

#endif
