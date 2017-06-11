/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_SERIALIZER_H
#define MIMIR_SERIALIZER_H

#include "log.h"
#include "stat.h"
#include "tools.h"
#include "typemode.h"
#include <typeinfo>
#include <memory>
#include <type_traits>

namespace MIMIR_NS {

template<typename Type>
    class SafeType {
      public:
        typedef Type type;
    };

template<>
    class SafeType<void> {
      public:
        typedef char type;
    };

template <typename Type>
class bytestream {
  public:
    static int to_bytes (Type* obj, int count, char *buf) {
        int bytesize = size(obj, count);
        char* begin = reinterpret_cast<char*>(std::addressof(*obj));
        memcpy(buf, begin, bytesize);
        return bytesize;
    }

    static int from_bytes (Type* obj, int count, char *buf) {
        int bytesize = size(obj, count);
        char* begin = reinterpret_cast<char*>(std::addressof(*obj));
        memcpy(begin, buf, bytesize);
        return bytesize;
    }

    static int compare (Type* obj1, Type* obj2, int count) {
        int bytesize = size(obj1, count);
        return memcmp(obj1, obj2, bytesize);
    }

    static int size (Type* obj, int count) {
        return (int)sizeof(Type) * count;
    }
};

template <>
class bytestream<const char*> {
  public:
    static int to_bytes (const char** obj, int count, char *buf) {

        int bytesize = 0;
        for (int i = 0; i < count; i++) {
            int strsize = (int)strlen(obj[i]) + 1;
            memcpy(buf, obj[i], strsize);
            buf += strsize;
            bytesize += strsize;
        }

        return bytesize;
    }

    static int from_bytes (const char** obj, int count, char *buf) {

        int bytesize = 0;
        for (int i = 0; i < count; i++) {
            int strsize = (int)strlen(buf) + 1;
            obj[i] = buf;
            buf += strsize;
            bytesize += strsize;
        }

        return bytesize;
    }

    static int compare(const char** obj1, const char** obj2, int count) {

        int ret = 0;

        for (int i = 0; i < count ; i++) {
            if (strcmp(obj1[i], obj2[i]) != 0) {
                ret = 1;
                break;
            }
        }

        return ret;
    }

    static int size (const char** obj, int count) {
        int strsize = 0;
        for (int i = 0; i < count; i++)
            strsize += (int)strlen(obj[i]) + 1;
        return strsize;
    }
};

template <>
class bytestream<char*> {
  public:
    static int to_bytes (char** obj, int count, char *buf) {

        int bytesize = 0;
        for (int i = 0; i < count; i++) {
            int strsize = (int)strlen(obj[i]) + 1;
            memcpy(buf, obj[i], strsize);
            buf += strsize;
            bytesize += strsize;
        }

        return bytesize;
    }

    static int from_bytes (char** obj, int count, char *buf) {

        int bytesize = 0;
        for (int i = 0; i < count; i++) {
            int strsize = (int)strlen(buf) + 1;
            obj[i] = buf;
            buf += strsize;
            bytesize += strsize;
        }

        return bytesize;
    }

    static int compare (char** obj1, char** obj2, int count) {

        int ret = 0;

        for (int i = 0; i < count ; i++) {
            if (strcmp(obj1[i], obj2[i]) != 0) {
                ret = 1;
                break;
            }
        }

        return ret;
    }

    static int size (char** obj, int count) {
        int strsize = 0;
        for (int i = 0; i < count; i++)
            strsize += (int)strlen(obj[i]) + 1;
        return strsize;
    }
};

template <>
class bytestream<void> {
  public:
    static int to_bytes (void* obj, int count, char *buf) {

        return 0;
    }

    static int from_bytes (void* obj, int count, char *buf) {

        return 0;
    }

    static int compare (void* obj1, void* obj2, int count) {

        return 1;
    }

    static int size (void* obj, int count) {

        return 0;
    }
};

template <typename KeyType, typename ValType>
class Serializer {
  public:
    Serializer(int keycount, int valcount) {
        this->keycount = keycount;
        this->valcount = valcount;
    }

    ~Serializer() {
    }

    int compare_key(KeyType* key1, KeyType* key2) {

        return bytestream<KeyType>::compare(key1, key2, keycount);

    }

    int key_to_bytes (KeyType *key, char *buffer, int bufsize) {

        return bytestream<KeyType>::to_bytes(key, keycount, buffer);

    }

    int val_to_bytes (ValType *val, char *buffer, int bufsize) {

        return bytestream<ValType>::to_bytes(val, valcount, buffer);

    }

    int kv_to_bytes (KeyType *key, ValType *val, char* buffer, int bufsize) {

        int keybytes = 0, valbytes = 0;

        keybytes = key_to_bytes(key, buffer, bufsize);
        buffer += keybytes;
        bufsize -= keybytes;
        valbytes = val_to_bytes(val, buffer, bufsize);

        return keybytes + valbytes;
    }

    int key_from_bytes (KeyType *key, char* buffer, int bufsize) {

        return bytestream<KeyType>::from_bytes(key, keycount, buffer);

    }

    int val_from_bytes (ValType *val, char* buffer, int bufsize) {

        return bytestream<ValType>::from_bytes(val, valcount, buffer);

    }

    int kv_from_bytes (KeyType *key, ValType *val,
                       char* buffer, int bufsize) {

        int keybytes = 0, valbytes = 0;

        keybytes = key_from_bytes(key, buffer, bufsize);
        buffer += keybytes;
        bufsize -= keybytes;

        valbytes = val_from_bytes(val, buffer, bufsize);

        return keybytes + valbytes;
    }

    int get_key_bytes (KeyType *key) {

        return bytestream<KeyType>::size(key, keycount);

    }

    int get_val_bytes (ValType *val) {
        return bytestream<ValType>::size(val, valcount);
    }

    int get_kv_bytes (KeyType *key, ValType *val) {
        return get_key_bytes(key) + get_val_bytes(val);
    }

  private:
    int      keycount, valcount;
    int      keysize, valsize;
    TypeMode keytype, valtype;
};

}

#endif
