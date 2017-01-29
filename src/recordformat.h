#ifndef RECORD_FORMAT_H
#define RECORD_FORMAT_H

#include "baserecordformat.h"

namespace MIMIR_NS {

class StringRecordFormat : public BaseRecordFormat {
  public:
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

class ByteRecordFormat : public BaseRecordFormat{
  public:
    virtual char *get_record() {
        return buffer;
    }

    virtual int get_record_size() {
        return 1;
    }

    virtual bool has_full_record(char *buffer, uint64_t len, bool islast) {
        if (len == 0) return false;

        return true;
    }
};

}

#endif
