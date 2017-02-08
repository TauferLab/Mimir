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
    KVContainer(int ksize = KVGeneral, int vsize = KVGeneral);
    virtual ~KVContainer();

    virtual bool open();
    virtual void close();
    virtual KVRecord* read();
    virtual void write(BaseRecordFormat *record);

    uint64_t get_kv_count(){
        return kvcount;
    }

    void print(FILE *fp) {
    }

    int ksize, vsize;

protected:
    Page *page;
    int64_t pageoff;

    uint64_t kvcount;

    KVRecord kv;
};

}

#endif
