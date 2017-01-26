#ifndef MIMIR_FILE_SPLITER_H
#define MIMIR_FILE_SPLITER_H

#include "inputsplit.h"

namespace MIMIR_NS{

class InputSplit;

enum SplitPolicy { BYSIZE, BYNAME };

class FileSplitter;

class FileSplitter{
  public:
    static FileSplitter* getFileSplitter(){
        if(splitter==NULL){
            splitter = new FileSplitter();
        }
        return splitter;
    }

    static FileSplitter *splitter;

  public:

    ~FileSplitter(){
    }

    InputSplit* split(InputSplit *input, SplitPolicy policy = BYSIZE){

        _bcast_file_list(input);
        _split(input, policy);

        return _get_my_split();
    }

  private:
    void _bcast_file_list(InputSplit *input);
    void _split(InputSplit *input, SplitPolicy policy);
    void _split_by_size(InputSplit *input);
    void _split_by_name(InputSplit *input);
    InputSplit *_get_my_split();
    uint64_t _get_proc_count(int, uint64_t);

    std::vector<InputSplit> files;
};

}

#endif
