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

namespace MIMIR_NS {

class KVContainer : public Container, public BaseDatabase {
public:
    KVContainer();
    virtual ~KVContainer();

    std::string get_object_name() { return "KVContainer"; }

    virtual bool open();
    virtual void close();
    virtual BaseRecordFormat* read();
    virtual void write(BaseRecordFormat *record);

    virtual uint64_t get_record_count() { return kvcount; }

    int ksize, vsize;
protected:
    Page *page;
    int64_t pageoff;
    ContainerIter *iter;

    uint64_t kvcount;
    KVRecord kv;
};

}

#endif
