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
#include "tools.h"
#include <typeinfo>
#include <memory>
#include <type_traits>

using byte = unsigned char;

namespace MIMIR_NS {

enum TypeMode {TypeNULL, TypeString, TypeFixed, TypeClass};

class Serializer {
  public:
    template <typename Type>
    static TypeMode type_mode () {
        // The type is a class
        if (std::is_class<Type>::value) return TypeClass;

        // The type is not a class
        Type tmp;
        std::string typestr = type_name<decltype(tmp)>();
        // void represent empty type
        if (typestr == "void") {
            return TypeNULL;
        // char* and const char * represent string terminated with 0
        } else if (typestr == "char*" || typestr == "const char*") {
            return TypeString;
        // all other types are viewed as fixed type
        } else {
            return TypeFixed;
        }
    }

    template <typename Type>
    static int get_bytes(Type *obj, int count = 1) {

        TypeMode typemode = Serializer::type_mode<Type>();
        if (typemode == TypeNULL) return 0;

        int bytesize = 0;

        for (int i = 0; i < count; i++) {

            const byte * begin = reinterpret_cast< const byte * >( std::addressof(*(obj+i)) ) ;

            int objsize = 0;
            if (typemode == TypeString)
                objsize = (int)strlen((const char*)(*(obj+i))) + 1;
            else if (typemode == TypeFixed)
                objsize = (int)sizeof(Type);

            bytesize += objsize;
        }

        return bytesize;
    }

    template <typename Type>
    static int compare(Type* obj1, Type* obj2, int count) {

        int ret = 0;

        TypeMode typemode = Serializer::type_mode<Type>();
        if (typemode == TypeNULL) return ret;

        for (int i = 0; i < count ; i++) {
            if (typemode == TypeString) {
                if (strcmp((const char*)obj1[i], (const char*)obj2[i]) != 0) {
                    ret = 1;
                    break;
                }
            } else {
                if (obj1[i] == obj2[i]) {
                } else {
                    ret = 1;
                    break;
                }
            }
        }

        return ret;
    }

    template <typename Type>
    static int to_bytes(Type* obj, int count, char* buffer, int bufsize) {

        TypeMode typemode = Serializer::type_mode<Type>();
        if (typemode == TypeNULL) return 0;

        int bytesize = 0;
        for (int i = 0; i < count; i++) {

            int objsize = 0;

            // String
            if (typemode == TypeString) {

                objsize = (int)strlen((const char*)(*(obj+i))) + 1;

                //printf("objsize=%d, %s, buffer=%p\n", objsize, *(obj+i), buffer);

                memcpy(buffer, (void*)(*(obj+i)), objsize);
            }
            // Other fixed-size type
            else if (typemode == TypeFixed) {

                const byte * begin = reinterpret_cast< const byte * >( std::addressof(*(obj+i)) ) ;

                objsize = (int)sizeof(Type);
                memcpy(buffer, begin, objsize);
            }

            if (objsize > bufsize) return -1;

            bytesize += objsize;
            bufsize -= objsize;
            buffer += objsize;
        }

        return bytesize;
    }

    template <typename Type>
    static int from_bytes(Type* obj, int count, char *buffer, int bufsize) {

        TypeMode typemode = Serializer::type_mode<Type>();
        if (typemode == TypeNULL) return 0;

        int bytesize = 0;
        for (int i = 0; i < count; i++) {

            int objsize = 0;
            if (typemode == TypeString) {
                int j = 0;
                for (j = 0; j < bufsize; j++) {
                    if (buffer[j] == '\0') break;
                }
                if (j >= bufsize) return -1;
                objsize = j + 1;
                (*(char**)(obj + i)) = buffer;
            } else if (typemode == TypeFixed) {
                byte * begin = reinterpret_cast< byte * >( std::addressof(*(obj+i)) ) ;
                objsize = (int)sizeof(Type);
                memcpy(begin, buffer, objsize);
            }

            bytesize += objsize;
            bufsize -= objsize;
            buffer += objsize;
        }

        return bytesize;
    }

    template <typename KeyType, typename ValType>
    static int get_bytes(KeyType *key, int keycount, ValType *val, int valcount) {
        return get_bytes<KeyType>(key, keycount) 
            + get_bytes<ValType>(val, valcount);
    }

    template <typename KeyType, typename ValType>
    static int to_bytes(KeyType *key, int keycount, ValType *val, int valcount,
                        char* buffer, int bufsize) {
        int total_bytes = 0;
        int item_bytes = 0;

        item_bytes = to_bytes(key, keycount, buffer, bufsize);
        if (item_bytes == -1) return -1;

        total_bytes += item_bytes;
        buffer += item_bytes;
        bufsize -= item_bytes;

        item_bytes = to_bytes(val, valcount, buffer, bufsize);
        if (item_bytes == -1) return -1;
        total_bytes += item_bytes;

        return total_bytes;
    }

    template <typename KeyType, typename ValType>
    static int from_bytes(KeyType *key, int keycount, ValType *val, int valcount,
                          char* buffer, int bufsize) {
        int total_bytes = 0;
        int item_bytes = 0;

        item_bytes = from_bytes(key, keycount, buffer, bufsize);
        if (item_bytes == -1) return -1;

        total_bytes += item_bytes;
        buffer += item_bytes;
        bufsize -= item_bytes;

        item_bytes = from_bytes(val, valcount, buffer, bufsize);
        if (item_bytes == -1) return -1;
        total_bytes += item_bytes;

        return total_bytes;
    }
};

}

#endif
