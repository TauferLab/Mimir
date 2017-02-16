/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef RECORD_FORMAT_H
#define RECORD_FORMAT_H

#include "globals.h"
#include "hashbucket.h"
#include "baserecordformat.h"

namespace MIMIR_NS {

class StringRecord : public BaseRecordFormat {
  public:
    StringRecord() {
    }

    virtual ~StringRecord() {
    }

    virtual int get_record_size() {
        return (int)strlen(buffer) + 1;
    }

    virtual bool has_full_record(char *buffer, uint64_t len, bool islast) {
        if (len == 0) return false;

         if (BaseRecordFormat::is_whitespace(*buffer)) return false;

        uint64_t i;
        for (i = 0; i < len; i++) {
            if(BaseRecordFormat::is_whitespace(*(buffer + i)))
                break;
        }

        if (i < len) {
            buffer[i] = '\0';
            return true;
        }

        if (islast) {
            buffer[len] = '\0';
            return true;
        }

        return false;
    }
};

class ByteRecord : public BaseRecordFormat {
  public:
    ByteRecord() {
    }

    virtual ~ByteRecord() {
    }

    virtual char *get_record() {
        return buffer;
    }

    virtual int get_record_size() {
        return 1;
    }

    virtual bool has_full_record(char *buffer, uint64_t len, bool islast) {
        if (len == 0) return false;

        if (len == 1 && islast == true)
            iseof = true;
        else
            iseof = false;

        return true;
    }

    bool is_eof() {
        return iseof;
    }

  private:
    bool iseof;
};

class KVRecord : public BaseRecordFormat {
  public:
    KVRecord() : BaseRecordFormat() 
    {
        ktype = KTYPE;
        vtype = VTYPE;
        key = val = NULL;
        keysize = valsize = 0;
    }

    //KVRecord(int ksize, int vsize) 
    //    : BaseRecordFormat() 
    //{
    //    this->ktype = ksize;
    //    this->vtype = vsize;
    //}

    KVRecord(char * key, int keysize,
             char *val, int valsize) 
        : BaseRecordFormat() 
    {
        ktype = KTYPE;
        vtype = VTYPE;
        this->key = key;
        this->val = val;
        this->keysize = keysize;
        this->valsize = valsize;
    }

    ~KVRecord() {
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

    virtual bool has_full_record(char *buffer, uint64_t len, bool islast) {
        if (len < (uint64_t)get_head_size()) 
            return false;
        if (ktype == KVGeneral) {
            if (len < (uint64_t)(get_key_size() + get_head_size()))
                return false;
        }
        if (ktype == KVString) {
            uint64_t i = 0;
            for (i = (uint64_t)get_head_size(); i < len; i++) {
                if (buffer[i] == '\0')
                    break;
            }
            if (i >= len)
                return false;
        }
        else {
            if (len < (uint64_t)(get_head_size() + get_head_size()))
                return false;
        }
        if (vtype == KVString) {
            uint64_t i = 0;
            for (i = (uint64_t)(get_head_size() + get_key_size()); i < len; i++) {
                if (buffer[i] == '\0')
                    break;
            }
            if (i >= len)
                return false;
        }
        else {
            if (len < (uint64_t)(get_head_size() + get_key_size() + get_val_size()))
                return false;
        }
        return true;
    }

  protected:
    char       *key, *val;
    int         keysize;
    int         valsize;
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

}

#endif
