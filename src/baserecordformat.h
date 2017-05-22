/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef BASE_RECORD_FORMAT_H
#define BASE_RECORD_FORMAT_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include <string>

namespace MIMIR_NS {

#if 0
class BaseRecordFormat {
  public:
    BaseRecordFormat() {
        buffer = NULL;
        len = 0;
    }

    BaseRecordFormat(char *buffer, int len = 0) {
        this->buffer = buffer;
        this->len = len;
    }

    virtual ~BaseRecordFormat(){
    }

    virtual char *get_record() {
        return buffer;
    }

    virtual int get_record_size() {
        return len;
    }

    virtual void set_buffer(char *buffer) {
        this->buffer = buffer;
    }

    virtual void set_record(char *buffer, int len = 0) {
        this->buffer = buffer;
        this->len = len;
    }

    virtual int get_skip_size(char *, uint64_t) {
        return 0;
    }

    virtual int get_next_record_size(char *, uint64_t, bool) {
        return -1;
    }

    //virtual int get_border_size(char *, uint64_t, bool) {
    //    return 0;
    //}

  protected:
    char* buffer;
    int   len;

  public:
    static bool is_contain(std::string &str, char ch) {
        if (str.size() == 0) return false;
        bool ret = false;
        for (int i =0; i < (int)str.size(); i++) {
            if (ch == str[i]) {
                ret = true;
                break;
            }
        }
        return ret;
    }
};
#endif

}

#endif
