#ifndef BASE_RECORD_FORMAT_H
#define BASE_RECORD_FORMAT_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include <string>

namespace MIMIR_NS {

class BaseRecordFormat{
  public:

    char* buffer;

    virtual char *get_record() = 0;

    virtual int   get_record_size() = 0;

    virtual void set_buffer(char *buffer){
        this->buffer = buffer;
    }

    virtual bool  has_full_record(char *buffer, uint64_t len, bool islast) = 0;

  public:
    static bool _is_contain(std::string &str, char ch){

        if(str.size() == 0) return true;

        bool ret = false;

        for(int i =0 ; i < (int)str.size(); i++){
            if(ch == str[i]){
                ret = true;
                break;
            }
        }

        return ret;
    }

    static bool _is_whitespace(char ch){
        return _is_contain(whitespaces, ch);
    }

    static bool _is_seperator(char ch){
        return _is_contain(seperators, ch);
    }

    static void set_sepeators(const char *str){
        seperators = str;
    }

    static void set_whitespace(const char *str){
        whitespaces = str;
    }

    static std::string seperators;
    static std::string whitespaces;
};

class StringRecordFormat : public BaseRecordFormat{
  public:
    virtual char *get_record(){
        return buffer;
    }
    virtual int   get_record_size(){
        return (int)strlen(buffer)+1;
    }
    virtual bool  has_full_record(char *buffer, uint64_t len, bool islast){
        if(len == 0) return false;

         if(_is_contain(whitespaces, *buffer)) return false;

        uint64_t i;
        for(i = 0; i < len; i++){
            if(_is_contain(whitespaces, *(buffer+i)))
                break;
        }

        if(i < len) {
            buffer[i] = '\0';
            return true;
        }

        if(islast){
            buffer[len+1] = '\0';
            return true;
        }

        return false;
    }
};


}

#endif
