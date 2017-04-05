/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_INTERFACE_H
#define MIMIR_INTERFACE_H

#include <string>
#include "recordformat.h"

namespace MIMIR_NS {

class Base {
  public:
    virtual ~Base() {}
    virtual bool open() = 0;
    virtual void close() = 0;
    virtual uint64_t get_record_count() = 0; 
    virtual std::string get_object_name() { 
        return std::string("Unknown"); 
    }
};

class Readable : public Base {
  public:
    virtual ~Readable() {}
    virtual BaseRecordFormat* read() = 0;
};

class Writable : public Base {
  public:
    virtual ~Writable() {}
    virtual void write(BaseRecordFormat *) = 0;
};

class Combinable {
  public:
    virtual ~Combinable() {}
    virtual void update(BaseRecordFormat *) = 0;
};

class BaseDatabase : virtual public Readable, virtual public Writable
{
  public:
    BaseDatabase(bool if_inner_object = false) {
        ref = 0;
        this->if_inner_object = if_inner_object;
    }

    virtual bool open() = 0;
    virtual void close() = 0;
    virtual BaseRecordFormat* read() = 0;
    virtual void write(BaseRecordFormat *) = 0;

    uint64_t getRef() {
        return ref;
    }

    void addRef() {
        ref ++;
    }

    void subRef() {
        ref --;
    }

    bool is_inner_object() {
        return if_inner_object;
    }

    static void addRef(BaseDatabase *d) {
        if (d != NULL) {
            d->addRef();
        }
    }

    static void subRef(BaseDatabase *d) {
        if (d != NULL) {
            d->subRef();
            if (d->getRef() == 0 
                && d->is_inner_object() ) {
                delete d;
            }
        }
    }

  private:
    uint64_t    ref;
    bool        if_inner_object;
};

typedef void (*MapCallback) (Readable *input, Writable *output, void *ptr);
typedef void (*ReduceCallback) (Readable *input, Writable *output, void *ptr);
typedef void (*CombineCallback) (Combinable *output, KVRecord *kv1, KVRecord *kv2, void *ptr);
typedef int (*HashCallback) (const char*, int);
typedef int (*RepartitionCallback) (const char*, int, bool islast);

}

#endif

