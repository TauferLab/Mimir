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

class BaseRecordFormat {
  public:
    BaseRecordFormat(){
        buffer = NULL;
        len = 0;
    }
    BaseRecordFormat(char *buffer, int len = 0){
        this->buffer = buffer;
        this->len = len;
    }
    virtual ~BaseRecordFormat(){
    }

    char* buffer;
    int   len;

    virtual char *get_record() {
        return buffer;
    }

    virtual void set_buffer(char *buffer, int len = 0) {
        this->buffer = buffer;
        this->len = len;
    }

    virtual int get_record_size() {
        return len;
    }

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

class InputRecord {
  public:
    virtual ~InputRecord() {}
    virtual int skip_count(char *, uint64_t) = 0;
    virtual int move_count(char *, uint64_t, bool) = 0;
    virtual bool has_full_record(char *, uint64_t, bool) = 0;
    virtual int get_left_border(char *, uint64_t, bool) = 0;
    virtual int process_left_border(int, char*, uint64_t, char*, int) {return 0;}
    virtual int get_right_cmd() { return 0; }
    virtual bool has_right_cmd() { return false; }
};

}

#endif
