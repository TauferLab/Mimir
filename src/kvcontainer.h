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
#include <set>
#include "container.h"
#include "containeriter.h"
#include "interface.h"
#include "serializer.h"
#include "stat.h"

namespace MIMIR_NS {

template <typename KeyType, typename ValType>
class KVContainer : virtual public Removable<KeyType, ValType>,
      virtual public BaseDatabase<KeyType, ValType> {
public:
    KVContainer(uint32_t bincount, int keycount, int valcount) 
        : BaseDatabase<KeyType, ValType>(true) {

        this->keycount = keycount;
        this->valcount = valcount;
        this->bincount = bincount;

        kvcount = 0;
        pagesize = DATA_PAGE_SIZE;

        ser = new Serializer<KeyType, ValType>(keycount, valcount);
    }

    virtual ~KVContainer() {
        delete ser;

        for (size_t i = 0; i < pages.size(); i++) {
            mem_aligned_free(pages[i].buffer);
            BaseDatabase<KeyType, ValType>::mem_bytes -= pagesize;
        }
    }

    virtual int open() {
        pageid = 0;
        pageoff = 0;
        LOG_PRINT(DBG_DATA, "KVContainer open.\n");
        return true;
    }

    virtual void close() {
        garbage_collection();
        LOG_PRINT(DBG_DATA, "KVContainer close.\n");
        return;
    }

    virtual int read(KeyType *key, ValType *val) {

        while (pageid < pages.size() 
               && pageoff >= pages[pageid].datasize) {
            pageid ++;
            pageoff = 0;
        }

        if (pageid >= pages.size()) {
            return -1;
        }

        char* ptr = pages[pageid].buffer + pageoff;

        int kvsize = this->ser->kv_from_bytes(key, val,
                        ptr, pages[pageid].datasize - pageoff);

        pageoff += kvsize;

        return 0;
    }

    virtual int write(KeyType *key, ValType *val) {

        //int kvsize = ser->get_kv_bytes(key, val);
        //if (kvsize > pagesize)
        //    LOG_ERROR("Error: KV size (%d) is larger \
        //              than one page (%ld)\n", kvsize, pagesize);

        if (pageid >= pages.size()) {
            pageid = add_page();
        }

        char *ptr = pages[pageid].buffer + pages[pageid].datasize;
        int kvsize = this->ser->kv_to_bytes(key, val, ptr, pagesize - pages[pageid].datasize);
        if (kvsize == -1) {
            pageid = add_page();
            ptr = pages[pageid].buffer + pages[pageid].datasize;
            kvsize = this->ser->kv_to_bytes(key, val, ptr, pagesize - pages[pageid].datasize);
            if (kvsize == -1)
                LOG_ERROR("Error: KV size (%d) is larger than one page (%ld)\n", kvsize, pagesize);
        }

        pages[pageid].datasize += kvsize;

        kvcount += 1;

        return 1;
    }

    virtual int remove(KeyType *key, ValType *val,
                       std::set<uint32_t>& removed_bins) {

        char *ptr = NULL;
        int kvsize, keysize, ret;
        bool hasfind = false;

        if (!isremove) {
            pageid = 0;
            pageoff = 0;
            isremove = true;
        }

        while (1) {

            while (pageid < pages.size() 
                   && pageoff >= pages[pageid].datasize) {
                pageid ++;
                pageoff = 0;
            }

            if (pageid >= pages.size()) {
                garbage_collection();
                isremove = false;
                return -1;
            }

            while (pageoff < pages[pageid].datasize) {
                char* ptr = pages[pageid].buffer + pageoff;
                int kvsize = this->ser->kv_from_bytes(key, val,
                                ptr, pages[pageid].datasize - pageoff);
                int keysize = this->ser->get_key_bytes(key);
                pageoff += kvsize;
                uint32_t bid = this->ser->get_hash_code(key) % bincount;
                if (removed_bins.find(bid) != removed_bins.end()) {
                    slices.insert(std::make_pair(ptr, kvsize));
                    hasfind = true;
                    break;
                }
            }
            if (hasfind) break;
        }

        kvcount -= 1;

        return 0;
    }

    virtual uint64_t get_record_count() { return kvcount; }

protected:

    uint64_t add_page() {
        Page page;
        page.datasize = 0;
        page.buffer = (char*)mem_aligned_malloc(MEMPAGE_SIZE, pagesize);
        pages.push_back(page);
        BaseDatabase<KeyType, ValType>::mem_bytes  += pagesize;
        PROFILER_RECORD_COUNT(COUNTER_MAX_KV_PAGES,
                              this->mem_bytes, OPMAX);
        return pages.size() - 1;
    }

    void garbage_collection()
    {
        typename SafeType<KeyType>::ptrtype key = NULL;
        typename SafeType<ValType>::ptrtype val = NULL;
        size_t dst_pid = 0, src_pid = 0;
        Page *dst_page = NULL, *src_page = NULL;
        int64_t dst_off = 0, src_off = 0;
        int kvsize;

        if (!(this->slices.empty())) {

            LOG_PRINT(DBG_GEN, "KVContainer garbage collection: slices=%ld\n",
                      this->slices.size());

            if (dst_pid < pages.size()) dst_page = &pages[dst_pid++];
            while (src_pid < pages.size() ) {
                src_page = &pages[src_pid++];
                src_off = 0;
                while (src_off < src_page->datasize) {
                    char *src_buf = src_page->buffer + src_off;
                    std::unordered_map < char *, int >::iterator slice = this->slices.find(src_buf);
                    if (slice != this->slices.end()) {
                        src_off += slice->second;
                    }
                    else {
                        int kvsize = this->ser->kv_from_bytes(&key, &val,
                                        src_buf, src_page->datasize - src_off);
                        if (dst_page != src_page || dst_off != src_off) {
                            if (dst_off + kvsize > this->pagesize) {
                                dst_page->datasize = dst_off;
                                dst_page = &pages[dst_pid++];
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
                    dst_page = &pages[dst_pid++];
                    dst_off = 0;
                }
            }
            if (dst_page != NULL) dst_page->datasize = dst_off;
            pageid = dst_pid;
            pageoff = dst_off;
            while (dst_pid < pages.size()) {
                dst_page = &pages[dst_pid++];
                dst_page->datasize = 0;
            }
            this->slices.clear();
        }
    }

    int64_t  pagesize;

    size_t            pageid;
    uint64_t          pageoff;
    std::vector<Page> pages;

    int     keycount, valcount;
    uint64_t           kvcount;
    uint32_t          bincount;

    bool isremove;
    std::unordered_map<char*, int> slices;
    Serializer<KeyType, ValType> *ser;
};

}

#endif
