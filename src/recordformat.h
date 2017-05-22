/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef RECORD_FORMAT_H
#define RECORD_FORMAT_H

#include "log.h"
#include "globals.h"
#include "hashbucket.h"
#include "baserecordformat.h"

namespace MIMIR_NS {
#if 0
int text_file_line(char *buffer, int len, bool islast) {

    if (len == 0) return -1;

    int i = 0;
    for (i = 0; i < len; i++) {
        if(*(buffer + i) == '\n')
            break;
    }

    if (i < len) {
        buffer[i] = '\0';
        return i + 1;
    }

    if (islast) {
        buffer[len] = '\0';
        return len + 1;
    }

    return -1;
}
#endif

#if 0
class StringRecord : public BaseRecordFormat {
  public:
    virtual int get_record_size() {
        return (int)strlen(buffer) + 1;
    }

    virtual int get_skip_size(char *buffer, uint64_t len) {
        int prefix = 0;
        while ((uint64_t)prefix < len 
               && StringRecord::is_whitespace(*(buffer+prefix))) {
            prefix ++;
        }

        return prefix;
    }

    virtual int get_next_record_size(char *buffer, uint64_t len, bool islast) {
        if (len == 0) return -1;

        if(StringRecord::is_whitespace(*(buffer))) return -1;

        uint64_t i;
        for (i = 0; i < len; i++) {
            if(StringRecord::is_whitespace(*(buffer + i)))
                break;
        }

        if (i < len) {
            buffer[i] = '\0';
            return (int)i + 1;
        }

        if (islast) {
            buffer[len] = '\0';
            return (int)len + 1;
        }

        return -1;
    }

    //virtual int get_border_size(char *buffer, uint64_t len, bool islast) {
    //    int i;
    //    for (i = 0; (uint64_t)i < len; i++) {
    //        if(StringRecord::is_whitespace(*(buffer + i)))
    //            break;
    //    }
    //    if (!islast && (uint64_t)i == len)
    //        LOG_ERROR("Cannot find whitespace in the partition of a process!");
    //    return i;
    //}

  public:

    static bool is_whitespace(char ch) {
        if (whitespaces.size() == 0)
            return false;

        return is_contain(whitespaces, ch);
    }

    static void set_whitespaces(const char *str) {
        whitespaces = str;
    }

    static std::string whitespaces;
};

class ByteRecord : public BaseRecordFormat {
  public:
    virtual int get_record_size() {
        return 1;
    }

    virtual int get_next_record_size(char *buffer, uint64_t len, bool islast) {
        if (len == 0) return -1;

        if (len == 1 && islast == true)
            iseof = true;
        else
            iseof = false;

        return 1;
    }

    //virtual int get_border_size(char *buffer, uint64_t len, bool islast) {
    //   int i;
    //    for (i = 0; (uint64_t)i < len; i++) {
    //        if(ByteRecord::is_separator(*(buffer + i)))
    //            break;
    //    }
    //    if (!islast && (uint64_t)i == len)
    //        LOG_ERROR("Cannot find separators %s in the partition of a process!", 
    //                  separators.c_str());
    //    return i;
    //}

    bool is_eof() {
        return iseof;
    }

  private:
    bool iseof;

  public:
    static bool is_separator(char ch) {
        if (separators.size() == 0)
            return true;

        return is_contain(separators, ch);
    }

    static void set_separators(const char *str) {
        separators = str;
    }

    static std::string separators;
};

class KVRecord : public BaseRecordFormat {
  public:
    KVRecord()
    {
        ktype = KTYPE;
        vtype = VTYPE;
        key = val = NULL;
        keysize = valsize = 0;
    }

    KVRecord(char *key, int keysize,
             char *val, int valsize) 
    {
        ktype = KTYPE;
        vtype = VTYPE;
        this->key = key;
        this->val = val;
        this->keysize = keysize;
        this->valsize = valsize;
    }

    void set_key_val(char *key, int keysize,
                     char *val, int valsize) {
        ktype = KTYPE;
        vtype = VTYPE;
        this->key = key;
        this->keysize = keysize;
        this->val = val;
        this->valsize = valsize;
    }

    void convert(KVRecord *record) {
        key = record->get_key();
        val = record->get_val();
        keysize = record->get_key_size();
        valsize = record->get_val_size();
        if (this->ktype == KVGeneral)
            *(int*)buffer = keysize;
        if (this->vtype == KVGeneral)
            *(int*)(buffer + (int)sizeof(int)) = valsize;
        memcpy(buffer + get_head_size(), key, keysize);
        memcpy(buffer + get_head_size() + keysize, val, valsize);
    }

    void set_kv_size(int ksize, int vsize) {
        this->ktype = ksize;
        this->vtype = vsize;
    }

    int get_head_size() {
        int headsize = 0;
        if (ktype == KVGeneral) headsize += (int)sizeof(int);
        if (vtype == KVGeneral) headsize += (int)sizeof(int);
        return headsize;
    }

    int get_key_size() {
        if (buffer != NULL) {
            if (ktype == KVGeneral)
                return *(int*)buffer;
            else if (ktype == KVString)
                return (int)strlen(get_key()) + 1;
            else if (ktype == KVVARINT) {
                char *key = get_key();
                int i = 0;
                while ((key[i] & 0x80) != 0) i++;
                return i + 1;
            }
            else
                return ktype;
        } else {
            return keysize;
        }
    }

    int get_val_size() {
        if (buffer != NULL) {
            if (vtype == KVGeneral) {
                if (ktype == KVGeneral)
                    return *(int*)(buffer + (int)sizeof(int));
                else
                    return *(int*)(buffer);
            }
            else if (vtype == KVString)
                return (int)strlen(get_val()) + 1;
            else if (vtype == KVVARINT) {
                char *val = get_val();
                int i = 0;
                while ((val[i] & 0x80) != 0) i++;
                return i + 1;
            }
            else
                return vtype;
        } else {
            return valsize;
        }
    }

    char* get_key() {
        char *key = this->key;
        if (buffer != NULL)
            key = buffer + get_head_size();
        return key;
    }

    char *get_val() {
        char *val = this->val;
        if (buffer != NULL)
            val = buffer + get_head_size() + get_key_size();
        return val;
    }

    virtual int get_record_size() {
        return get_head_size() + get_key_size() + get_val_size();
    }

    virtual int get_next_record_size(char *buffer, uint64_t len, bool islast) {
        return 0;
    }

    //virtual bool has_full_record(char *buffer, uint64_t len, bool islast) {
    //    if (len < (uint64_t)get_head_size()) 
    //        return false;
    //    if (ktype == KVGeneral) {
    //        if (len < (uint64_t)(get_key_size() + get_head_size()))
    //            return false;
    //    }
    //    if (ktype == KVString) {
    //        uint64_t i = 0;
    //        for (i = (uint64_t)get_head_size(); i < len; i++) {
    //            if (buffer[i] == '\0')
    //                break;
    //        }
    //        if (i >= len)
    //            return false;
    //    }
    //    else {
    //        if (len < (uint64_t)(get_head_size() + get_head_size()))
    //            return false;
    //    }
    //    if (vtype == KVString) {
    //        uint64_t i = 0;
    //        for (i = (uint64_t)(get_head_size() + get_key_size()); i < len; i++) {
    //            if (buffer[i] == '\0')
    //                break;
    //        }
    //        if (i >= len)
    //            return false;
    //    }
    //    else {
    //        if (len < (uint64_t)(get_head_size() + get_key_size() + get_val_size()))
    //            return false;
    //    }
    //    return true;
    //}

  protected:
    char       *key, *val;
    int         keysize, valsize;
    int         ktype, vtype;
};

class KMVRecord : public KVRecord {
  public:
    KMVRecord() {
    }

    ~KMVRecord() {
    }

    void set_unique(ReducerUnique *ukey) {
        this->ukey = ukey;
        nvalue = ukey->nvalue;
        ivalue = 0;
        value_start = 0;
        if (ivalue < nvalue){
            pset = ukey->firstset;
            valuebytes = pset->soffset;
            values = pset->voffset;
            value_end = pset->nvalue;
            value = values;
        }
    }

    char *get_key() {
        return ukey->key;
    }

    int get_key_size() {
        return ukey->keybytes;
    }

    char *get_next_val() {
        //printf("mv: ivalue=%d, nvalue=%d\n", ivalue, nvalue);

        char *val = NULL;

        if (ivalue >= nvalue) {
            return NULL;
        }

        if (ivalue >= value_end) {
            value_start += pset->nvalue;
            pset = pset->next;

            valuebytes = pset->soffset;
            values = pset->voffset;

            value = values;
            value_end += pset->nvalue;
        }

        if (vtype == KVGeneral)
            valuesize = valuebytes[ivalue - value_start];
        else if (vtype == KVString)
            valuesize = (int) strlen(value) + 1;
        else
            valuesize = vtype;

        ivalue++;
        val = value;
        value += valuesize;
        return val;
    }

    int get_val_size() {
        return valuesize;
    }

    int get_val_count () {
        return (int)nvalue;
    }

 private:
    int64_t nvalue;
    int *valuebytes;
    char *values;
    int64_t ivalue;
    int64_t value_start;
    int64_t value_end;
    char *value;
    int valuesize;
    ReducerUnique *ukey;
    ReducerSet *pset;
};
#endif

}

#endif
