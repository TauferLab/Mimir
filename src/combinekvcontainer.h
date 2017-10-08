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
                                            ValType *val1, ValType *val2, ValType *val3,
                                            void *ptr),
                       void *user_ptr,
                       //uint32_t bincount = 0,
                       int keycount = 1, int valcount = 1,
                       int hashscale = 1)
        : BaseObject(true), KVContainer<KeyType,ValType>(keycount, valcount) {

        this->user_combine = user_combine;
        this->user_ptr = user_ptr;
        this->hashscale = hashscale;
        bucket = NULL;
    }

    virtual ~CombineKVContainer()
    {
    }

    virtual int open() {

        bucket = new HashBucket<CombinerVal>(hashscale, false, true);

        KVContainer<KeyType,ValType>::open();

        LOG_PRINT(DBG_GEN, "CombineKVContainer open!\n");

        return true;
    }

    virtual void close() {

        this->garbage_collection();
        bucket->clear();

        delete bucket;

        KVContainer<KeyType,ValType>::close();

        LOG_PRINT(DBG_GEN, "CombineKVContainer close.\n");
    }

    virtual int write(KeyType *key, ValType *val)
    {
        int ret = true;
        int kvsize = this->ser->get_kv_bytes(key, val);
        if (kvsize > this->pagesize)
            LOG_ERROR("Error: KV size (%d) is larger \
                      than one page (%ld)\n", kvsize, this->pagesize);

        int keysize = this->ser->get_key_bytes(key);
        char *keyptr = this->ser->get_key_ptr(key);
        u = bucket->findEntry(keyptr, keysize);

        if (u == NULL) {
            CombinerVal tmp;

            if (!(this->ispointer)) {
                std::unordered_map < char *, int >::iterator iter;
                for (iter = this->slices.begin(); iter != this->slices.end(); iter++) {
                    char *sbuf = iter->first;
                    int ssize = iter->second;

                    if (ssize >= kvsize) {
                        tmp.kv = sbuf + (ssize - kvsize);
                        this->ser->kv_to_bytes(key, val, tmp.kv, kvsize);

                        if (iter->second == kvsize)
                            this->slices.erase(iter);
                        else
                            this->slices[iter->first] -= kvsize;

                        break;
                    }
                }

                if (iter == this->slices.end()) {

                    if (this->pageid >= this->pages.size() 
                        || this->pagesize - this->pages[this->pageid].datasize < kvsize) {
                        this->pageid = this->add_page(); 
                    }

                    tmp.kv = this->pages[this->pageid].buffer + this->pages[this->pageid].datasize;
                    this->ser->kv_to_bytes(key, val, tmp.kv, kvsize);
                    this->pages[this->pageid].datasize += kvsize;
                }
            } else {

                if (this->pageid >= this->pages.size() 
                    || this->pagesize - this->pages[this->pageid].datasize < kvsize) {
                    this->pageid = this->add_page(); 
                }

                tmp.kv = this->pages[this->pageid].buffer + this->pages[this->pageid].datasize;
                this->ser->kv_to_bytes(key, val, tmp.kv, kvsize);
                this->pages[this->pageid].datasize += kvsize;

            }
            bucket->insertEntry(tmp.kv, keysize, &tmp);
            this->kvcount += 1;
            ret = 2;
        }
        else {
            typename SafeType<KeyType>::ptrtype u_key = NULL;
            typename SafeType<ValType>::ptrtype u_val = NULL;
            typename SafeType<ValType>::type r_val[this->valcount];

            int ukvsize = this->ser->kv_from_bytes(&u_key, &u_val, u->kv, MAX_RECORD_SIZE);

            user_combine(this, u_key, u_val, val, r_val, user_ptr);

            int ukeysize = this->ser->get_key_bytes(u_key);
            int uvalsize = this->ser->get_val_bytes(u_val);
            int rvalsize = this->ser->get_val_bytes(r_val);

            if (rvalsize <= uvalsize) {
                this->ser->val_to_bytes(r_val, u->kv + ukeysize, uvalsize);
                if (rvalsize < uvalsize) {
                    char *ptr = u->kv + ukvsize - (uvalsize - rvalsize);
                    this->slices.insert(std::make_pair(ptr, uvalsize - rvalsize));
                }
            } else {
                this->slices.insert(std::make_pair(u->kv, ukvsize));
                if (ukeysize + rvalsize + this->pages[this->pageid].datasize > this->pagesize)
                    this->pageid = this->add_page();
                u->kv = this->pages[this->pageid].buffer + this->pages[this->pageid].datasize;
                this->ser->kv_to_bytes(u_key, r_val, u->kv, ukeysize + rvalsize);
                this->pages[this->pageid].datasize += ukeysize + rvalsize;
            }
            ret = 1;
        }
        return ret;
    }

#if 0
    void update(KeyType *key, ValType *val)
    {
        int kvsize = this->ser->get_kv_bytes(key, val);

        if (this->ser->compare_key(key, u_key) != 0)
            LOG_ERROR("Error: the result key of combiner is different!\n");

        if (kvsize <= ukvsize) {
            this->ser->kv_to_bytes(key, val, u->kv, kvsize);
            if (kvsize < ukvsize) {
                this->slices.insert(std::make_pair(u->kv + ukvsize - kvsize, ukvsize - kvsize));
            }
        }
        else {
            this->slices.insert(std::make_pair(u->kv, ukvsize));
            if (kvsize + this->pages[this->pageid].datasize > this->pagesize)
                this->pageid = this->add_page();
            u->kv = this->pages[this->pageid].buffer + this->pages[this->pageid].datasize;
            this->ser->kv_to_bytes(key, val, u->kv, kvsize);
            this->pages[this->pageid].datasize += kvsize;
        }

        return;
    }
#endif

    virtual int remove() {
        typename SafeType<KeyType>::ptrtype key = NULL;
        typename SafeType<ValType>::ptrtype val = NULL;

        if (this->ptr == NULL) return false;

        this->ser->kv_from_bytes(&key, &val, this->ptr, this->kvsize);

        int ret = KVContainer<KeyType, ValType>::remove();
        if (ret) {
            int keysize = this->ser->get_key_bytes(key);
            char *keyptr = this->ser->get_key_ptr(key);
            bucket->removeEntry(keyptr, keysize);
        }

        return ret;
    }

    virtual void garbage_collection()
    {
        typename SafeType<KeyType>::ptrtype key = NULL;
        typename SafeType<ValType>::ptrtype val = NULL;
        size_t dst_pid = 0, src_pid = 0;
        Page *dst_page = NULL, *src_page = NULL;
        int64_t dst_off = 0, src_off = 0;

        if (!(this->slices.empty())) {

            LOG_PRINT(DBG_GEN, "CombineKVContainer garbage collection: slices=%ld\n",
                      this->slices.size());

            if (this->kvcount == 0) {
                for (auto iter : this->pages) {
                    mem_aligned_free(iter.buffer);
                    BaseDatabase<KeyType, ValType>::mem_bytes -= this->pagesize;
                }
                this->pages.clear();
                std::unordered_map<char*,int> empty;
                this->slices.swap(empty);
                this->gbmem = 0;
                return;
            }

            if (dst_pid < this->pages.size()) dst_page = &this->pages[dst_pid++];
            while (src_pid < this->pages.size() ) {
                src_page = &this->pages[src_pid++];
                src_off = 0;
                while (src_off < src_page->datasize) {
                    char *src_buf = src_page->buffer + src_off;
                    std::unordered_map < char *, int >::iterator slice = this->slices.find(src_buf);
                    if (slice != this->slices.end()) {
                        src_off += slice->second;
                    }
                    else {
                        int kvsize = this->ser->kv_from_bytes(&key, &val,
                                        src_buf, (int)(src_page->datasize - src_off));
                        if (dst_page != src_page || dst_off != src_off) {
                            if (dst_off + kvsize > this->pagesize) {
                                dst_page->datasize = dst_off;
                                dst_page = &this->pages[dst_pid++];
                                dst_off = 0;
                            }
                            // Update key entry
                            int keysize = this->ser->get_key_bytes(key);
                            char *keyptr = this->ser->get_key_ptr(key);
                            u = bucket->updateEntry(keyptr, keysize, dst_page->buffer + dst_off);
                            u->kv = dst_page->buffer + dst_off;
                            for (int kk = 0; kk < kvsize; kk++) {
                                dst_page->buffer[dst_off + kk] = src_page->buffer[src_off + kk];
                            }
                        }
                        src_off += kvsize;
                        dst_off += kvsize;
                    }
                }
                if (src_page == dst_page && src_off == dst_off) {
                    dst_page = &this->pages[dst_pid++];
                    dst_off = 0;
                }
            }
            if (dst_page != NULL) dst_page->datasize = dst_off;
            this->pageid = dst_pid;
            this->pageoff = dst_off;
            while (dst_pid < this->pages.size()) {
                auto iter = this->pages.back();
                mem_aligned_free(iter.buffer);
                BaseDatabase<KeyType, ValType>::mem_bytes -= this->pagesize;
                this->pages.pop_back();
            }
            std::unordered_map<char*,int> empty;
            this->slices.swap(empty);
            this->gbmem = 0;
        }
    }

private:
    void (*user_combine)(Combinable<KeyType,ValType> *output,
                         KeyType *key, ValType *val1, ValType *val2, ValType *val3, void *ptr);
    void *user_ptr;
    HashBucket<CombinerVal> *bucket;
    int hashscale;
    CombinerVal *u;
};

}

#endif
