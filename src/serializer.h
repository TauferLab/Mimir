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

template <typename KeyType, typename ValType>
class Serializer {
  public:
    Serializer(int keycount, int valcount) {
        this->keycount = keycount;
        this->valcount = valcount;
        keysize = sizeof(KeyType) * keycount;
        valsize = sizeof(ValType) * valcount;
        keytype = type_mode<KeyType>();
        valtype = type_mode<ValType>();
        if (keytype == TypeNULL)
            LOG_ERROR("The key type cannot be empty!\n");
    }

    ~Serializer() {
    }

    int compare_key(KeyType* key1, KeyType* key2) {

        int ret = 0;
        if (keytype == TypeFixed) {
            ret = memcmp(key1, key2, keysize);
        } else if (keytype == TypeString) {
            for (int i = 0; i < keycount ; i++) {
                if (strcmp((const char*)key1[i], (const char*)key2[i]) != 0) {
                    ret = 1;
                    break;
                }
            }
        } else {
            LOG_ERROR("Serializer Error!\n");
        }

        return ret;
    }

    int key_to_bytes (KeyType *key, char *buffer, int bufsize) {

        int bytesize = 0;

        if (keytype == TypeFixed) {
            char *begin = reinterpret_cast<char*>(std::addressof(*key)) ;
            memcpy(buffer, begin, keysize);
            bufsize -= keysize;
            buffer += keysize;
            bytesize = keysize;
        } else if (keytype == TypeString) {
            for (int i = 0; i < keycount; i++) {
                int strsize = strlen(key[i]) + 1;
                memcpy(buffer, key[i], strsize);
                bufsize -= strsize;
                buffer += strsize;
                bytesize += strsize;
            }
        } else {
            LOG_ERROR("Serializer Error!\n");
        }

        return bytesize;
    }

    int val_to_bytes (ValType *val, char *buffer, int bufsize) {

        int bytesize = 0;

        if (valtype == TypeFixed) {
            char* begin = reinterpret_cast<char*>(std::addressof(*val));
            memcpy(buffer, begin, valsize);
            bufsize -= valsize;
            buffer += valsize;
            bytesize = valsize;
        } else if (valtype == TypeString) {
            for (int i = 0; i < valcount; i++) {
                int strsize = strlen(val[i]) + 1;
                memcpy(buffer, val[i], strsize);
                bufsize -= strsize;
                buffer += strsize;
                bytesize += strsize;
            }
        } else {
            LOG_ERROR("Serializer Error!\n");
        }

        return bytesize;
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

        int bytesize = 0;

        // get key
        if (keytype == TypeFixed) {
            char* begin = reinterpret_cast<char*>(std::addressof(*key)) ;
            memcpy(begin, buffer, keysize);
            bufsize -= keysize;
            buffer += keysize;
            bytesize = keysize;
        } else if (keytype == TypeString) {
            for (int i = 0; i < keycount; i++) {
                int strsize = strlen(buffer) + 1;
                key[i] = buffer;
                bufsize -= strsize;
                buffer += strsize;
                bytesize += strsize;
            }
        } else {
            LOG_ERROR("Serializer Error!\n");
        }

        return bytesize;
    }

    int val_from_bytes (ValType *val, char* buffer, int bufsize) {

        int bytesize = 0;

        // get val
        if (valtype == TypeFixed) {
            char* begin = reinterpret_cast<char*>( std::addressof(*val) );
            memcpy(begin, buffer, valsize);
            bufsize -= valsize;
            buffer += valsize;
            bytesize = valsize;
        } else if (keytype == TypeString) {
            for (int i = 0; i < valcount; i++) {
                int strsize = strlen(buffer) + 1;
                val[i] = buffer;
                bufsize -= strsize;
                buffer += strsize;
                bytesize += strsize;
            }
        } else {
            LOG_ERROR("Serializer Error!\n");
        }

        return bytesize;
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

        int keybytes = 0;

        if (keytype == TypeFixed) {
            keybytes = keysize;
        } else if (keytype == TypeString) {
            for (int i = 0; i < keycount; i++) {
                int strsize = (int)strlen(key[i]) + 1;
                keybytes += strsize;
            }
        } else {
            LOG_ERROR("Serializer Error!\n");
        }

        return keybytes;
    }

    int get_val_bytes (ValType *val) {

        int valbytes = 0;

        if (valtype == TypeFixed) {
            valbytes = valsize;
        } else if (valtype == TypeString) {
            for (int i = 0; i < valcount; i++) {
                int strsize = (int)strlen(val[i]) + 1;
                valbytes += strsize;
            }
        }

        return valbytes;
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
