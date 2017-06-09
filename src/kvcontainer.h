/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_KV_CONTAINER_H
#define MIMIR_KV_CONTAINER_H

#include <stdio.h>
#include <stdlib.h>
#include "container.h"
#include "containeriter.h"
#include "interface.h"
#include "serializer.h"

namespace MIMIR_NS {

template <typename KeyType, typename ValType>
class KVContainer : virtual public Container,
      virtual public Removable<KeyType, ValType>,
      virtual public BaseDatabase<KeyType, ValType> {
public:
    KVContainer(int keycount, int valcount) : BaseDatabase<KeyType, ValType>(true) {
        this->keycount = keycount;
        this->valcount = valcount;
        kvcount = 0;
        page = NULL;
        pageoff = 0;
        ser = new Serializer<KeyType, ValType>(keycount, valcount);
    }

    virtual ~KVContainer() {
        delete ser;
    }

    virtual int open() {
        page = NULL;
        pageoff = 0;
        iter = new ContainerIter(this);
        LOG_PRINT(DBG_DATA, "KVContainer open.\n");
        return true;
    }

    virtual void close() {
        garbage_collection();
        delete iter;
        page = NULL;
        pageoff = 0;
        LOG_PRINT(DBG_DATA, "KVContainer close.\n");
        return;
    }

    virtual int read(KeyType *key, ValType *val) {
        char *ptr;
        int kvsize;

        while (1) {
            if (page == NULL || pageoff >= page->datasize) {
                page = iter->next();
                pageoff = 0;
                if (page == NULL)
                    return -1;
            }

            ptr = page->buffer + pageoff;

            auto iter = this->slices.find(ptr);
            if (iter == this->slices.end()) break;

            pageoff += iter->second;
        }

        kvsize = this->ser->kv_from_bytes(key, val, ptr, page->datasize - pageoff);

        pageoff += kvsize;

        return 0;
    }

    virtual int write(KeyType *key, ValType *val) {

        if (page == NULL)
            page = add_page();

        int kvsize = ser->get_kv_bytes(key, val);
        if (kvsize > pagesize)
            LOG_ERROR("Error: KV size (%d) is larger \
                      than one page (%ld)\n", kvsize, pagesize);

        // Find a slice to store the <key,value>
        std::unordered_map < char *, int >::iterator iter;
        for (iter = this->slices.begin(); iter != this->slices.end(); iter++) {
            char *sbuf = iter->first;
            int ssize = iter->second;

            if (ssize >= kvsize) {
                char *ptr = sbuf + (ssize - kvsize);
                this->ser->kv_to_bytes(key, val, ptr, kvsize);

                if (iter->second == kvsize)
                    this->slices.erase(iter);
                else
                    this->slices[iter->first] -= kvsize;
                break;
            }
        }

        // Add at the tail
        if (iter == this->slices.end()) {

            if (kvsize > (pagesize - page->datasize))
                page = add_page();

            char *ptr = page->buffer + page->datasize;
            this->ser->kv_to_bytes(key, val, ptr, pagesize - page->datasize);
            page->datasize += kvsize;
        }

        kvcount += 1;

        return 1;
    }

    virtual int remove(KeyType *key, ValType *val,
                       int divisor, std::vector<int>& remainders) {

        char *ptr;
        int kvsize, keysize, ret;

        while (1) {
            while (1) {
                if (page == NULL || pageoff >= page->datasize) {
                    page = iter->next();
                    pageoff = 0;
                    if (page == NULL)
                        return -1;
                }

                ptr = page->buffer + pageoff;

                auto iter = this->slices.find(ptr);
                if (iter == this->slices.end()) break;

                pageoff += iter->second;
            }

            kvsize = this->ser->kv_from_bytes(key, val, ptr, page->datasize - pageoff);
            keysize = this->ser->get_key_bytes(key);

            uint32_t hid = hashlittle(ptr, keysize, 0);
            int bid = (int)(hid % (uint32_t) (divisor));
            if (std::find(remainders.begin(), remainders.end(), bid) != remainders.end()) {
                // make the kv invalid
                this->slices.insert(std::make_pair(ptr, kvsize));
                ret = bid;
                kvcount -= 1;
                break;
            }

            pageoff += kvsize;
        }

        return ret;
    }

    virtual uint64_t get_record_count() { return kvcount; }

protected:
    void garbage_collection()
    {
        KeyType key[this->keycount];
        ValType val[this->valcount];
        ContainerIter dst_iter(this), src_iter(this);
        Page *dst_page = NULL, *src_page = NULL;
        int64_t dst_off = 0, src_off = 0;
        int kvsize;

        if (!(this->slices.empty())) {

            LOG_PRINT(DBG_GEN, "KVContainer garbage collection: slices=%ld\n",
                      this->slices.size());

            dst_page = dst_iter.next();
            while ((src_page = src_iter.next()) != NULL) {
                src_off = 0;
                while (src_off < src_page->datasize) {
                    char *src_buf = src_page->buffer + src_off;
                    std::unordered_map < char *, int >::iterator iter = this->slices.find(src_buf);
                    if (iter != this->slices.end()) {
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
            this->slices.clear();
        }
    }

    Page *page;
    int64_t pageoff;
    ContainerIter *iter;

    uint64_t kvcount;
    int keycount;
    int valcount;

    std::unordered_map<char*, int> slices;

    Serializer<KeyType, ValType> *ser;
};

}

#endif
