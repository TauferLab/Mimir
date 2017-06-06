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
#include "recordformat.h"
#include "interface.h"
#include "serializer.h"

namespace MIMIR_NS {

template <typename KeyType, typename ValType>
class KVContainer : public Container, public BaseDatabase<KeyType, ValType> {
public:
    KVContainer(int keycount, int valcount) : BaseDatabase<KeyType, ValType>(true) {
        this->keycount = keycount;
        this->valcount = valcount;
        kvcount = 0;
        page = NULL;
        pageoff = 0;
        ser = new Serializer<KeyType, ValType>(keycount, valcount);
    }

    ~KVContainer() {
        delete ser;
    }

    virtual int open() {
        page = NULL;
        pageoff = 0;
        iter = new ContainerIter(this);
        LOG_PRINT(DBG_DATA, "KVContainer open.\n");
        return true;
    }

    void close() {
        delete iter;
        page = NULL;
        pageoff = 0;
        LOG_PRINT(DBG_DATA, "KVContainer close.\n");
        return;
    }

    int read(KeyType *key, ValType *val) {
        char *ptr;
        int kvsize;

        if (page == NULL || pageoff >= page->datasize) {
            page = iter->next();
            pageoff = 0;
            if (page == NULL)
                return -1;
        }

        ptr = page->buffer + pageoff;

        kvsize = this->ser->kv_from_bytes(key, val, ptr, page->datasize - pageoff);

        pageoff += kvsize;

        return 0;
    }

    int write(KeyType *key, ValType *val) {

        if (page == NULL)
            page = add_page();

        int kvsize = ser->get_kv_bytes(key, val);
        if (kvsize > pagesize)
            LOG_ERROR("Error: KV size (%d) is larger \
                      than one page (%ld)\n", kvsize, pagesize);

        if (kvsize > (pagesize - page->datasize))
            page = add_page();

        char *ptr = page->buffer + page->datasize;

        this->ser->kv_to_bytes(key, val, ptr, pagesize - page->datasize);

        page->datasize += kvsize;

        kvcount += 1;
    }

    virtual uint64_t get_record_count() { return kvcount; }

protected:
    Page *page;
    int64_t pageoff;
    ContainerIter *iter;

    uint64_t kvcount;
    int keycount;
    int valcount;

    Serializer<KeyType, ValType> *ser;
};

}

#endif
