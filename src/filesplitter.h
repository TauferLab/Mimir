/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_FILE_SPLITER_H
#define MIMIR_FILE_SPLITER_H

#include "inputsplit.h"

namespace MIMIR_NS{

class InputSplit;

enum SplitPolicy { BYNAME, BYSIZE };

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

    InputSplit* split(InputSplit *input, SplitPolicy policy = BYNAME){

        LOG_PRINT(DBG_IO, "Start bcast file list\n");
        bcast_file_list(input);
        LOG_PRINT(DBG_IO, "Start split file list\n");
        split_files(input, policy);
        LOG_PRINT(DBG_IO, "End split file list\n");

        return get_my_split();
    }

    InputSplit* split(const char *indir, SplitPolicy policy = BYNAME){

        InputSplit input(indir);
        LOG_PRINT(DBG_IO, "Start bcast file list\n");
        bcast_file_list(&input);
        LOG_PRINT(DBG_IO, "Start split file list\n");
        split_files(&input, policy);
        LOG_PRINT(DBG_IO, "End split file list\n");

        return get_my_split();
    }

  private:
    void bcast_file_list(InputSplit *input);
    void split_files(InputSplit *input, SplitPolicy policy);
    void split_by_size(InputSplit *input);
    void split_by_name(InputSplit *input);
    InputSplit *get_my_split();
    uint64_t get_proc_count(int, uint64_t);

    std::vector<InputSplit> files;
};

}

#endif
