#ifndef MIMIR_INTERFACE_H
#define MIMIR_INTERFACE_H

#include "recordformat.h"

namespace MIMIR_NS {

class BaseInput {
  public:
    virtual bool open() = 0;
    virtual BaseRecordFormat* next() = 0;
    virtual void close() = 0;
};

class BaseOutput {
  public:
    virtual bool open() = 0;
    virtual void add(const char *, int, const char *, int) = 0;
    virtual void close() = 0;
};

typedef void (*MapCallback) (BaseInput *input, BaseOutput *output, void *ptr);
typedef void (*ReduceCallback) (BaseInput *input, BaseOutput *output, void *ptr);
typedef void (*CombineCallback) (BaseOutput *output, KVRecord *kv1, KVRecord *kv2, void*);
typedef int (*HashCallback) (const char*, int);

}

#endif

