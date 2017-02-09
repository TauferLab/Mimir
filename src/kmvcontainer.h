#ifndef MIMIR_KMV_CONTAINER_H
#define MIMIR_KMV_CONTAINER_H

#include "config.h"
#include "hashbucket.h"
#include "recordformat.h"
#include "kvcontainer.h"

namespace MIMIR_NS {

class KMVContainer : public Container, public Readable {
  public:
    KMVContainer() {
        u = NULL;
        this->ksize = KTYPE;
        this->vsize = VTYPE;
        record.set_kv_size(ksize, vsize);
    }

    ~KMVContainer() {
    }

    virtual bool open() {
        return true;
    }

    virtual KMVRecord* read() {
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

    virtual void add(const char *, int, const char *, int) {
    }

    virtual void close() {

    }

    void convert(KVContainer *kv);

  private:
    int ksize, vsize;
    ReducerHashBucket h;
    ReducerUnique *u;
    KMVRecord record;
};

}

#endif
