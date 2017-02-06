#ifndef MIMIR_KMV_CONTAINER_H
#define MIMIR_KMV_CONTAINER_H

#include "hashbucket.h"
#include "recordformat.h"
#include "kvcontainer.h"

namespace MIMIR_NS {

class KMVContainer : public Container, public BaseInput, public BaseOutput {
  public:
    KMVContainer(int ksize = KVGeneral, int vsize = KVGeneral) {
        u = NULL;
        this->ksize = ksize;
        this->vsize = vsize;
        record.set_kv_size(ksize, vsize);
    }

    ~KMVContainer() {
    }

    virtual bool open() {
        return true;
    }

    virtual KMVRecord* next() {
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
