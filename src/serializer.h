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
#include "config.h"
//#include "typemode.h"
#include <typeinfo>
#include <memory>
#include <sstream>
#include <type_traits>

namespace MIMIR_NS {

template <typename Type>
class SafeType
{
  public:
    typedef Type type;
    typedef Type* ptrtype;
};

template <>
class SafeType<void>
{
  public:
    typedef char type;
    typedef void* ptrtype;
};

template <typename Type>
class bytestream
{
  public:
    static int to_bytes(Type* obj, int count, char* buf, int bufsize)
    {
        int bytesize = (int) sizeof(Type) * count;
        if (bufsize < bytesize) return -1;
        //char* begin = reinterpret_cast<char*>(std::addressof(*obj));
        char* begin = reinterpret_cast<char*>(obj);
        memcpy(buf, begin, bytesize);
        return bytesize;
    }

    static int from_bytes(Type* obj, int count, char* buf)
    {
        int bytesize = (int) sizeof(Type) * count;
        //char* begin = reinterpret_cast<char*>(std::addressof(*obj));
        char* begin = reinterpret_cast<char*>(obj);
        memcpy(begin, buf, bytesize);
        return bytesize;
    }

    static char* get_ptr(Type* obj, int count)
    {
        return reinterpret_cast<char*>(obj);
    }

    static Type* get_obj(char* buf) { return reinterpret_cast<Type*>(buf); }

    static int compare(Type* obj1, Type* obj2, int count)
    {
        int bytesize = size(obj1, count);
        return memcmp(obj1, obj2, bytesize);
    }

    static int size(Type* obj, int count) { return (int) sizeof(Type) * count; }

    static int psize(int count) { return (int) sizeof(Type) * count; }
};

template <>
class bytestream<const char*>
{
  public:
    static int to_bytes(const char** obj, int count, char* buf, int bufsize)
    {
        int bytesize = 0;
        for (int i = 0; i < count; i++) {
            int strsize = (int) strlen(obj[i]) + 1;
            if (bufsize < strsize) return -1;
            memcpy(buf, obj[i], strsize);
            buf += strsize;
            bytesize += strsize;
            bufsize -= strsize;
        }

        return bytesize;
    }

    static int from_bytes(const char** obj, int count, char* buf)
    {
        int bytesize = 0;
        for (int i = 0; i < count; i++) {
            int strsize = (int) strlen(buf) + 1;
            obj[i] = buf;
            buf += strsize;
            bytesize += strsize;
        }

        return bytesize;
    }

    static char* get_ptr(const char** obj, int count) { return (char*) (*obj); }

    static const char** get_obj(char* buf) { return NULL; }

    static int compare(const char** obj1, const char** obj2, int count)
    {
        int ret = 0;

        for (int i = 0; i < count; i++) {
            if (strcmp(obj1[i], obj2[i]) != 0) {
                ret = 1;
                break;
            }
        }

        return ret;
    }

    static int size(const char** obj, int count)
    {
        int strsize = 0;
        for (int i = 0; i < count; i++) strsize += (int) strlen(obj[i]) + 1;
        return strsize;
    }

    static int psize(int count) { return (int) sizeof(const char*) * count; }
};

template <>
class bytestream<char*>
{
  public:
    static int to_bytes(char** obj, int count, char* buf, int bufsize)
    {
        int bytesize = 0;
        for (int i = 0; i < count; i++) {
            int strsize = (int) strlen(obj[i]) + 1;
            if (bufsize < strsize) return -1;
            memcpy(buf, obj[i], strsize);
            buf += strsize;
            bytesize += strsize;
        }

        return bytesize;
    }

    static int from_bytes(char** obj, int count, char* buf)
    {
        int bytesize = 0;
        for (int i = 0; i < count; i++) {
            int strsize = (int) strlen(buf) + 1;
            obj[i] = buf;
            buf += strsize;
            bytesize += strsize;
        }

        return bytesize;
    }

    static char* get_ptr(char** obj, int count)
    {
        return reinterpret_cast<char*>(*obj);
    }

    static char** get_obj(char* buf) { return NULL; }

    static int compare(char** obj1, char** obj2, int count)
    {
        int ret = 0;

        for (int i = 0; i < count; i++) {
            if (strcmp(obj1[i], obj2[i]) != 0) {
                ret = 1;
                break;
            }
        }

        return ret;
    }

    static int size(char** obj, int count)
    {
        int strsize = 0;
        for (int i = 0; i < count; i++) strsize += (int) strlen(obj[i]) + 1;
        return strsize;
    }

    static int psize(int count) { return (int) sizeof(char*) * count; }
};

template <>
class bytestream<void>
{
  public:
    static int to_bytes(void* obj, int count, char* buf, int bufsize)
    {
        return 0;
    }

    static int from_bytes(void* obj, int count, char* buf) { return 0; }

    static char* get_ptr(void* obj, int count) { return NULL; }

    static void* get_obj(char* buf) { return NULL; }

    static int compare(void* obj1, void* obj2, int count) { return 1; }

    static int size(void* obj, int count) { return 0; }

    static int psize(int count) { return 0; }
};

template <typename Type, typename dummy = Type>
class txtstream
{
  public:
#if 0
    static int size (Type *obj, int count) {
        int bytesize = 0;
        for (int i = 0; i < count; i++) {
            std::stringstream ss;
            ss << obj[i];
            int strsize = (int)ss.str().size();
            bytesize += strsize;
        }
        return bytesize;
    }
#endif

    static int to_txt(Type* obj, int count, char* buf, int bufsize)
    {
        int bytesize = 0;
        for (int i = 0; i < count; i++) {
            std::stringstream ss;
            ss << obj[i];
            const char* strptr = ss.str().c_str();
            int strsize = (int) ss.str().size();
            if (bufsize < strsize) return -1;
            memcpy(buf + bytesize, strptr, strsize);
            bytesize += strsize;
            bufsize -= strsize;
        }
        return bytesize;
    }
};

// the type is a class type
template <typename Type>
class txtstream<Type,
                typename std::enable_if<std::is_class<Type>::value, Type>::type>
{
  public:
    //static int size (void *obj, int count) {
    //    LOG_ERROR("Cannot convert a class to string!\n");
    //    return 0;
    //}

    static int to_txt(Type* obj, int count, char* buf, int bufsize)
    {
        //LOG_ERROR("Cannot convert a class to string!\n");
        //return 0;
        int bytesize = 0;
        for (int i = 0; i < count; i++) {
            std::stringstream ss;
            obj[i] >> ss;
            const char* strptr = ss.str().c_str();
            int strsize = (int) ss.str().size();
            if (bufsize < strsize) return -1;
            memcpy(buf + bytesize, strptr, strsize);
            bytesize += strsize;
            bufsize -= strsize;
        }
        return bytesize;
    }
};

// the type is void
template <>
class txtstream<void, void>
{
  public:
    //static int size (void *obj, int count) {
    //    return 0;
    //}

    static int to_txt(void* obj, int count, char* buf, int bufsize)
    {
        return 0;
    }
};

template <typename KeyType, typename ValType>
class Serializer
{
  public:
    Serializer(int keycount, int valcount)
    {
        this->keycount = keycount;
        this->valcount = valcount;

        if (std::is_pointer<KeyType>::value) {
            if (keycount > 1)
                tmpkey
                    = (char*) mem_aligned_malloc(MEMPAGE_SIZE, MAX_RECORD_SIZE);
            int pkeysize = bytestream<KeyType>::psize(keycount);
            bufkey = (KeyType*) mem_aligned_malloc(MEMPAGE_SIZE, pkeysize);
        }
        else {
            tmpkey = NULL;
            bufkey = NULL;
        }

        if (std::is_pointer<ValType>::value) {
            int pvalsize = bytestream<ValType>::psize(valcount);
            bufval = (ValType*) mem_aligned_malloc(MEMPAGE_SIZE, pvalsize);
        }
        else {
            bufval = NULL;
        }
    }

    ~Serializer()
    {
        if (std::is_pointer<KeyType>::value) {
            if (keycount > 1) mem_aligned_free(tmpkey);
            mem_aligned_free(bufkey);
        }

        if (std::is_pointer<ValType>::value) {
            mem_aligned_free(bufval);
        }
    }

    int compare_key(KeyType* key1, KeyType* key2)
    {
        return bytestream<KeyType>::compare(key1, key2, keycount);
    }

    int key_to_bytes(KeyType* key, char* buffer, int bufsize)
    {
        return bytestream<KeyType>::to_bytes(key, keycount, buffer, bufsize);
    }

    int val_to_bytes(ValType* val, char* buffer, int bufsize)
    {
        return bytestream<ValType>::to_bytes(val, valcount, buffer, bufsize);
    }

    int kv_to_bytes(KeyType* key, ValType* val, char* buffer, int bufsize)
    {
        int keybytes = 0, valbytes = 0;

        keybytes
            = bytestream<KeyType>::to_bytes(key, keycount, buffer, bufsize);
        if (keybytes == -1) return -1;

        buffer += keybytes;
        bufsize -= keybytes;

        valbytes
            = bytestream<ValType>::to_bytes(val, valcount, buffer, bufsize);
        if (valbytes == -1) return -1;

        return keybytes + valbytes;
    }

    int key_from_bytes(KeyType* key, char* buffer, int bufsize)
    {
        return bytestream<KeyType>::from_bytes(key, keycount, buffer);
    }

    int val_from_bytes(ValType* val, char* buffer, int bufsize)
    {
        return bytestream<ValType>::from_bytes(val, valcount, buffer);
    }

    int kv_from_bytes(KeyType** key, ValType** val, char* buffer, int bufsize)
    {
        int keybytes = 0, valbytes = 0;

        if (std::is_pointer<KeyType>::value) {
            keybytes
                = bytestream<KeyType>::from_bytes(bufkey, keycount, buffer);
            *key = bufkey;
        }
        else {
            *key = bytestream<KeyType>::get_obj(buffer);
            keybytes = get_key_bytes(*key);
        }
        buffer += keybytes;
        bufsize -= keybytes;

        if (std::is_pointer<ValType>::value) {
            valbytes
                = bytestream<ValType>::from_bytes(bufval, valcount, buffer);
            *val = bufval;
        }
        else {
            *val = bytestream<ValType>::get_obj(buffer);
            valbytes = get_val_bytes(*val);
        }

        return keybytes + valbytes;
    }

    int kv_from_bytes(KeyType* key, ValType* val, char* buffer, int bufsize)
    {
        int keybytes = 0, valbytes = 0;

        keybytes = bytestream<KeyType>::from_bytes(key, keycount, buffer);
        buffer += keybytes;
        bufsize -= keybytes;

        valbytes = bytestream<ValType>::from_bytes(val, valcount, buffer);

        return keybytes + valbytes;
    }

    int get_key_bytes(KeyType* key)
    {
        return bytestream<KeyType>::size(key, keycount);
    }

    int get_val_bytes(ValType* val)
    {
        return bytestream<ValType>::size(val, valcount);
    }

    int get_kv_bytes(KeyType* key, ValType* val)
    {
        return bytestream<KeyType>::size(key, keycount)
               + bytestream<ValType>::size(val, valcount);
        ;
    }

#if 0
    int get_key_txt_len (KeyType *key) {
        return txtstream<KeyType>::size(key, keycount);
    }

    int get_val_txt_len (ValType *val) {
        return txtstream<ValType>::size(val, valcount);
    }

    int get_kv_txt_len (KeyType *key, ValType *val) {
        return get_key_txt_len(key) + get_val_txt_len(val) + 1;
    }

    int key_to_txt (KeyType *key, char *buffer, int bufsize) {

        return txtstream<KeyType>::to_txt(key, keycount, buffer, bufsize);

    }

    int val_to_txt (ValType *val, char *buffer, int bufsize) {

        return txtstream<ValType>::to_txt(val, valcount, buffer, bufsize);

    }
#endif

    int kv_to_txt(KeyType* key, ValType* val, char* buffer, int bufsize)
    {
        int keybytes = 0, valbytes = 0, sepsize = 0;

        keybytes = txtstream<KeyType>::to_txt(key, keycount, buffer, bufsize);
        if (keybytes == -1) return -1;
        buffer += keybytes;
        bufsize -= keybytes;
        if (!std::is_void<ValType>::value) {
            if (bufsize < 1) return -1;
            *buffer = ' ';
            buffer += 1;
            bufsize -= 1;
            sepsize += 1;
            valbytes
                = txtstream<ValType>::to_txt(val, valcount, buffer, bufsize);
            if (valbytes == -1) return -1;
            buffer += valbytes;
            bufsize -= valbytes;
        }
        if (bufsize < 1) return -1;
        *buffer = '\n';
        buffer += 1;
        bufsize -= 1;
        sepsize += 1;

        return keybytes + valbytes + sepsize;
    }

    uint32_t get_hash_code(KeyType* key)
    {
        int keysize = this->get_key_bytes(key);
        if (keysize > MAX_RECORD_SIZE) LOG_ERROR("The key is too long!\n");

        if (std::is_pointer<KeyType>::value && keycount > 1) {
            bytestream<KeyType>::to_bytes(key, keycount, tmpkey,
                                          MAX_RECORD_SIZE);
            //assert(ret != -1);
        }
        else {
            tmpkey = bytestream<KeyType>::get_ptr(key, keycount);
        }

        uint32_t hid = hashlittle(tmpkey, keysize, 0);
        return hid;
    }

    char* get_key_ptr(KeyType* key)
    {
        if (std::is_pointer<KeyType>::value && keycount > 1) {
            bytestream<KeyType>::to_bytes(key, keycount, tmpkey,
                                          MAX_RECORD_SIZE);
            //assert(ret != -1);
        }
        else {
            tmpkey = bytestream<KeyType>::get_ptr(key, keycount);
        }
        return tmpkey;
    }

  private:
    int keycount, valcount;
    char* tmpkey;
    KeyType* bufkey;
    ValType* bufval;
};

} // namespace MIMIR_NS

#endif
