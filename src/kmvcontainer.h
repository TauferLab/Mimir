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

class KMVContainer : public Container, public Readable {
  public:
    KMVContainer() {
        u = NULL;
        this->ksize = KTYPE;
        this->vsize = VTYPE;
        record.set_kv_size(ksize, vsize);
	kmvcount = 0;
    }

    ~KMVContainer() {
    }

    virtual bool open() {
        return true;
    }

    virtual BaseRecordFormat* read() {
        if (u == NULL) {
            u = h.BeginUnique();
            if (u == NULL)
                return NULL;
         } else {
            u = h.NextUnique();
            if (u == NULL)
                return NULL;
        }
        record.set_unique(u);
        return &record;
    }

    virtual void close() {

    }

    virtual uint64_t get_record_count() { return kmvcount; }

    void convert(Readable *kv);

  private:
    int ksize, vsize;
    ReducerHashBucket h;
    ReducerUnique *u;
    KMVRecord record;
    uint64_t kmvcount;
};

}

#endif
