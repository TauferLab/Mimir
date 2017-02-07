#ifndef MIMIR_INTERFACE_H
#define MIMIR_INTERFACE_H

#include "recordformat.h"

namespace MIMIR_NS {

class Base {
  public:
    virtual bool open() = 0;
    virtual void close() = 0;
};

class Readable : public Base {
  public:
    virtual BaseRecordFormat* read() = 0;
};

class Writable : public Base {
  public:
    virtual void write(BaseRecordFormat *) = 0;
};

class Combinable {
  public:
    virtual void update(BaseRecordFormat *) = 0;
};

typedef void (*MapCallback) (Readable *input, Writable *output, void *ptr);
typedef void (*ReduceCallback) (Readable *input, Writable *output, void *ptr);
typedef void (*CombineCallback) (Combinable *output, KVRecord *kv1, KVRecord *kv2, void*);
typedef int (*HashCallback) (const char*, int);

}

#endif

