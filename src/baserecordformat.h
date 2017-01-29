#ifndef BASE_RECORD_FORMAT_H
#define BASE_RECORD_FORMAT_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include <string>

namespace MIMIR_NS {

class BaseRecordFormat {
  public:

    char* buffer;

    virtual char *get_record() {
        return buffer;
    }

    virtual void set_buffer(char *buffer) {
        this->buffer = buffer;
    }

    virtual int get_record_size() = 0;

    virtual bool has_full_record(char *, uint64_t, bool) = 0;

  public:
    static bool is_contain(std::string &str, char ch) {

        if (str.size() == 0) return true;

        bool ret = false;

        for (int i =0; i < (int)str.size(); i++) {
            if (ch == str[i]) {
                ret = true;
                break;
            }
        }

        return ret;
    }

    static bool is_whitespace(char ch) {
        return is_contain(whitespaces, ch);
    }

    static bool is_seperator(char ch) {
        return is_contain(seperators, ch);
    }

    static void set_sepeators(const char *str) {
        seperators = str;
    }

    static void set_whitespace(const char *str) {
        whitespaces = str;
    }

    static std::string seperators;
    static std::string whitespaces;
};

}

#endif
