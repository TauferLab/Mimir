#ifndef MIMIR_KV_CONTAINER_H
#define MIMIR_KV_CONTAINER_H

#include <stdio.h>
#include <stdlib.h>
#include "container.h"
#include "recordformat.h"
#include "interface.h"

namespace MIMIR_NS {

class KVContainer : public Container, public Readable, public Writable {
public:
    KVContainer(int ksize = KVGeneral, int vsize = KVGeneral) {
        this->ksize = ksize;
        this->vsize = vsize;
        record.set_kv_size(ksize, vsize);
        kvcount = 0;
        page = NULL;
        pageoff = 0;
    }

    virtual ~KVContainer() {
    }

    virtual bool open() {
        page = NULL;
        return true;
    }

    virtual void close() {
        page = NULL;
    }

    KVRecord* next(){
        char *ptr;
        int kvsize;

        printf("page = %p\n", page);

        if (page == NULL || pageoff >= page->datasize) {
            page = get_next_page();
            pageoff = 0;
            if (page == NULL)
                return NULL;
        }

        ptr = page->buffer + pageoff;
        record.set_buffer(ptr);
        kvsize = record.get_record_size();

        printf("getkv: kvsize=%d, pageoff=%d\n", kvsize, pageoff);

        pageoff += kvsize;
        return &record;
    }

    virtual void add(const char* key, int keysize, const char* val, int valsize) {
        if (page == NULL)
            page = add_page();
        printf("add: key=%s\n", key);
        int kvsize = record.get_head_size() + keysize + valsize;
        if (kvsize > pagesize)
            LOG_ERROR("Error: KV size (%d) is larger \
                      than one page (%ld)\n", kvsize, pagesize);

        if (kvsize > (pagesize - page->datasize))
            page = add_page();

        char *ptr = page->buffer + page->datasize;
        record.set_buffer(ptr);
        record.set_key_value(key, keysize, val, valsize);
        page->datasize += kvsize;

        kvcount += 1;
    }

    uint64_t get_kv_count(){
        return kvcount;
    }

    void print(FILE *fp) {
    }

public:
    Page *page;
    int64_t pageoff;

    int ksize, vsize;
    uint64_t kvcount;

    KVRecord record;
};

}

#endif
