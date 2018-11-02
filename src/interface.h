//
// (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego
//     Supercomputer Center, National University of Defense Technology,
//     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
//
//     See COPYRIGHT in top-level directory.
//

#ifndef MIMIR_INTERFACE_H
#define MIMIR_INTERFACE_H

#include <string>
#include <set>
#include "log.h"

namespace MIMIR_NS {

enum DB_POS { DB_START, DB_END };

class BaseObject
{
  public:
    BaseObject(bool inner = false)
    {
        ref = 0;
        this->inner = inner;
    }
    virtual ~BaseObject() {}
    virtual int open() = 0;
    virtual void close() = 0;
    virtual int seek(DB_POS pos) = 0;
    virtual uint64_t get_record_count() = 0;

    bool is_inner() { return inner; }

    uint64_t getRef() { return ref; }

    void addRef() { ref++; }

    void subRef() { ref--; }

    static void addRef(BaseObject *d)
    {
        if (d != NULL) {
            d->addRef();
        }
    }

    static void subRef(BaseObject *d)
    {
        if (d != NULL && d->is_inner()) {
            d->subRef();
            if (d->getRef() == 0) {
                delete d;
            }
        }
    }

  private:
    uint64_t ref;
    bool inner;
};

template <typename KeyType, typename ValType>
class Readable : virtual public BaseObject
{
  public:
    virtual ~Readable() {}
    virtual int read(KeyType *, ValType *) = 0;
};

template <typename KeyType, typename ValType>
class Removable : virtual public BaseObject
{
  public:
    virtual ~Removable() {}
    virtual int remove() = 0;
};

template <typename KeyType, typename ValType>
class Writable : virtual public BaseObject
{
  public:
    virtual ~Writable() {}
    virtual int write(KeyType *, ValType *) = 0;
};

template <typename KeyType, typename ValType>
class Combinable
{
  public:
    virtual ~Combinable() {}
};

template <typename KeyType, typename ValType>
class BaseDatabase : virtual public Readable<KeyType, ValType>,
                     virtual public Writable<KeyType, ValType>,
                     virtual public Removable<KeyType, ValType>
{
  public:
    BaseDatabase() {}

    virtual ~BaseDatabase() {}

    virtual int open() = 0;
    virtual void close() = 0;
    virtual int read(KeyType *, ValType *) = 0;
    virtual int seek(DB_POS pos) = 0;
    virtual int write(KeyType *, ValType *) = 0;
    virtual int remove() = 0;

  public:
    static uint64_t mem_bytes;
};

template <typename KeyType, typename ValType>
uint64_t BaseDatabase<KeyType, ValType>::mem_bytes = 0;

} // namespace MIMIR_NS

#endif
