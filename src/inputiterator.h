#ifndef INPUT_ITERATOR_H
#define INPUT_ITERATOR_H

#include <string.h>
#include "filereader.h"

namespace MIMIR_NS {

template<typename RecordType>
class InputIterator{
  public:
    InputIterator(FileReader *reader){
        this->reader = reader;
        reader->open_stream();
    }
    virtual ~InputIterator(){
        reader->close_stream();
    }
    virtual RecordType getNextRecord()=0;
    virtual bool isEOF(){
        return reader->is_eof();
    }
    virtual bool isEmpty(){
        return reader->is_empty();
    }
  protected:
    FileReader *reader;
};

// get a byte each time
class ByteIterator : public InputIterator<char>{
  public:
    ByteIterator(FileReader *reader) 
        : InputIterator(reader){
    }

    virtual ~ByteIterator(){
    }

    virtual char getNextRecord(){
        char* ch = reader->get_byte();
        reader->next();
        return (*ch);
    }
};

// get a string each time
// the temp buffer is used to store a string which is 
// not continuous in the input buffer
class StringIterator : public InputIterator<char*>{
  public:
    StringIterator(FileReader *reader, const char *whitespace)
        : InputIterator(reader){
        this->whitespace = whitespace;
        tmpbufsize = MAX_STR_SIZE+1;
        tmpbuf = (char*)mem_aligned_malloc(MEMPAGE_SIZE, tmpbufsize);
        movestep = 0;
    }

    virtual ~StringIterator(){
        mem_aligned_free(tmpbuf);
    }

    virtual char* getNextRecord(){
        char *start_ptr = NULL;
        char *end_ptr = NULL;
        char *cur_ptr = NULL;

        strsize = 0;

        // skip previous string
        // we cannot move the next before the string 
        // is processed by user application
        reader->next(movestep);
        movestep = 0;

        // skip whitespaces
        int isfind = false;
        if(reader->is_eof()) reader->next();
        while(!reader->is_empty()){

            start_ptr = reader->get_byte();
            end_ptr = reader->guard();
            cur_ptr = start_ptr;

            while(cur_ptr < end_ptr){
                if(!_is_whitespace(*cur_ptr)){
                    isfind = true;
                    break;
                }
                cur_ptr++;
            }

            reader->next((int)(cur_ptr - start_ptr));

            if(reader->is_eof()) reader->next();

            if(isfind) break;
        };

        if(reader->is_empty()) return NULL;

        bool istail = false;
        isfind = false;
        while(!reader->is_eof()){

            start_ptr = reader->get_byte();
            end_ptr = reader->guard();
            cur_ptr = start_ptr;
            istail = reader->is_tail();

            // find next whitespace
            while(cur_ptr < end_ptr){
                int off = istail ? 0 : (int)(cur_ptr-start_ptr);
                if(reader->is_eof(off) || _is_whitespace(*cur_ptr)){
                    isfind = true;
                    break;
                }
                cur_ptr++;
            }

            // if we don't find whitespace or some bytes have been copied
            if(!isfind || strsize != 0){
                memcpy(tmpbuf + strsize, start_ptr, cur_ptr - start_ptr);
                strsize += (int)(cur_ptr - start_ptr);
                if(strsize > tmpbufsize)
                    LOG_ERROR("The string length is larger than the buffer size %d!\n", tmpbufsize);
                reader->next((int)(cur_ptr - start_ptr));
            }else{
                movestep += (int)(cur_ptr - start_ptr);
            }

            if(isfind) break;
        }

        // the string is in the original buffer
        if(strsize == 0 && _is_whitespace(*cur_ptr)) {
            *cur_ptr = '\0';
            movestep += 1;
            return start_ptr;
        }

        // there is no place to put the '\0' in the original buffer
        if(strsize == 0){
            memcpy(tmpbuf, start_ptr, cur_ptr - start_ptr);
            strsize = (int)(cur_ptr - start_ptr);
        }
        tmpbuf[strsize] = '\0';
        return tmpbuf;
    }

  private:
    bool _is_whitespace(char ch){
        bool ret = false;

        for(int i =0 ; i < (int)strlen(whitespace); i++){
            if(ch == whitespace[i]){
                ret = true;
                break;
            }
        }

        return ret;
    }

    char *tmpbuf;
    int tmpbufsize;
    int  strsize;
    int  movestep;

    const char *whitespace;
};

}

#endif
