#ifndef RECORD_FORMAT_H
#define RECORD_FORMAT_H

#include "const.h"
#include "baserecordformat.h"
#include "hashbucket.h"

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
    KVRecord() {
    }

    KVRecord(int ksize, int vsize) {
        this->ksize = ksize;
        this->vsize = vsize;
    }

    ~KVRecord() {
    }

    void set_kv_size(int ksize, int vsize) {
        this->ksize = ksize;
        this->vsize = vsize;
    }

    int get_head_size() {
        int headsize = 0;
        if (ksize == KVGeneral) headsize += (int)sizeof(int);
        if (vsize == KVGeneral) headsize += (int)sizeof(int);
        return headsize;
    }

    int get_key_size() {
        if (ksize == KVGeneral)
            return *(int*)buffer;
        else if (ksize == KVString)
            return (int)strlen(get_key()) + 1;
        else
            return ksize;
    }

    int get_val_size() {
        if (vsize == KVGeneral) {
            if (ksize == KVGeneral)
                return *(int*)(buffer + (int)sizeof(int));
            else
                return *(int*)(buffer);
        }
        else if (vsize == KVString)
            return (int)strlen(get_val()) + 1;
        else
            return vsize;
    }

    char* get_key() {
        return buffer + get_head_size();
    }

    char *get_val() {
        return buffer + get_head_size() + get_key_size();
    }

    void set_key_value(const char * key, int ksize,
                       const char *val, int vsize) {
        if (this->ksize == KVGeneral)
            *(int*)buffer = ksize;
        if (this->vsize == KVGeneral)
            *(int*)(buffer + (int)sizeof(int)) = vsize;
        memcpy(buffer + get_head_size(), key, ksize);
        memcpy(buffer + get_head_size() + ksize, val, vsize);
    }

    virtual int get_record_size() {
        return get_head_size() + get_key_size() + get_val_size();
    }

    virtual bool has_full_record(char *buffer, uint64_t len, bool islast) {
        if (len < (uint64_t)get_head_size()) 
            return false;
        if (ksize == KVGeneral) {
            if (len < (uint64_t)(get_key_size() + get_head_size()))
                return false;
        }
        if (ksize == KVString) {
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
        if (vsize == KVString) {
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
    int ksize, vsize;
};

class KMVRecord : public KVRecord {
  public:
    KMVRecord() {
    }

    KMVRecord(int ksize, int vsize) 
        : KVRecord(ksize, vsize) {
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
        printf("mv: ivalue=%d, nvalue=%d\n", ivalue, nvalue);

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

        if (vsize == KVGeneral)
            valuesize = valuebytes[ivalue - value_start];
        else if (vsize == KVString)
            valuesize = (int) strlen(value) + 1;
        else
            valuesize = vsize;

        ivalue++;
        val = value;
        printf("value=%ld\n", *(int64_t*)val);
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
