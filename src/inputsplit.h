/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_INPUT_SPLIT_H
#define MIMIR_INPUT_SPLIT_H

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>

namespace MIMIR_NS {

#define MAX_GROUPS      2

struct FileSeg {
    std::string filename;
    uint64_t    filesize;
    uint64_t    startpos;
    uint64_t    segsize;
    uint64_t    maxsegsize;
    int         startrank;
    int         endrank;
    int         readorder;   // if -1
};

class InputSplit {
  public:
    InputSplit(const char *filepath) {
        get_file_list(filepath, 1);
        fileidx = 0;
    }

    InputSplit() {
        fileidx = 0;
    }

    ~InputSplit() {
    }

    FileSeg *get_next_file() {
        if (fileidx >= filesegs.size()) {
            fileidx = 0;
            return NULL;
        }

        return &filesegs[fileidx++];
    }

    uint64_t get_max_fsize() {
        uint64_t max_fsize = 0;
        FileSeg *fileseg = NULL;

        while ((fileseg = get_next_file()) != NULL) {
            if (fileseg->maxsegsize > max_fsize)
                max_fsize = fileseg->maxsegsize;
        }

        return max_fsize;
    }

    void add(const char*filepath) { get_file_list(filepath, 1); }

    uint64_t get_file_count() { return filesegs.size(); }

    void add_seg_file(FileSeg *seg) {
        if (seg->readorder == -1)
            filesegs.push_back(*seg);
        else {
            std::vector<FileSeg>::iterator iter = filesegs.begin();
            for (; iter != filesegs.end(); iter++) {
                if (seg->readorder < iter->readorder || iter->readorder == -1) {
                    filesegs.insert(iter, *seg);
                    break;
                }
            }
            if(iter == filesegs.end())
                filesegs.push_back(*seg);
        }
    }

    void clear() { filesegs.clear(); }

    void print();

  private:
    void get_file_list(const char*, int);

    std::vector<FileSeg> filesegs;
    size_t               fileidx;
};

}

#endif
