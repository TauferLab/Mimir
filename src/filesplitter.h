#ifndef MIMIR_FILE_SPLITTER_H
#define MIMIR_FILE_SPLITTER_H

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>

#include <mpi.h>

namespace MIMIR_NS {

// use to schedule the collective communication
// file in GROUP1 and GROUP2 can be read together 
// by collective communication;
// file in GROUPANY do not depond on other processes
// Note that each process at most has one file of GROUP1
// and one file of GROUP2
#define GROUP_SIZE          2
enum CommGroup{GROUP1, GROUP2, GROUPNON};

class FileSplitter {
  public:
    FileSplitter(int64_t blocksize,
                 const char* filepath, 
                 int shared,
                 int recurse, 
                 MPI_Comm comm) {
        this->comm = comm;
        this->blocksize = blocksize;
        this->filepath = filepath;
        this->shared = shared;
        this->recurse = recurse;
        MPI_Comm_rank(comm, &me);
        MPI_Comm_size(comm, &nprocs);
        totalblocks = 0;
        totalsize = 0;

        for(int i = 0; i< GROUP_SIZE; i++) group_to_idx[i] = -1;
    }
    ~FileSplitter() {
    }

    void split();
    void print();

    int64_t     get_file_count();
    const char *get_file_name(int64_t i);
    int64_t     get_start_offset(int64_t i);
    int64_t     get_end_offset(int64_t);
    int64_t     get_file_size(int64_t);
    int64_t     get_total_size();
    int64_t     get_block_size();
    int         get_file_groupid(int64_t i);
    bool        is_left_sharefile(int64_t i);
    bool        is_right_sharefile(int64_t i);

    const char* get_group_filename(int group_id);
    void        get_group_ranks(int group_id, int &low, int &high);
    int64_t     get_group_maxblocks(int64_t group_id);
    int64_t     get_group_fileid(int group_id);

  private:
    void _get_input_files(const char*, int, int);
    void _partition_files();
    int  _get_send_counts(int *);
    void _get_send_data(char *);
    void _get_recv_data(char *, int);

    struct FileSegment{
        std::string     filename;
        int64_t         filesize;
        int64_t         startoff;
        int64_t         endoff;
        int64_t         max_blocks; // maximum blocks partition to a process
        int             start_rank;
        int             end_rank;
        int             group_id;   // schedule order of IO
    };

    struct FileInfo{
        FileInfo(std::string filename, 
                 int64_t filesize){
            this->filename = filename;
            this->filesize = filesize;
            start_rank = -1;
            end_rank = -1;
            group_id = -1;
            max_blocks = 0;
        }

        std::string          filename;
        int64_t              filesize;
        int64_t              max_blocks;
        int                  start_rank;
        int                  end_rank;
        int                  group_id;
        std::vector<int64_t> offstarts;
    };

    int      me, nprocs;
    int64_t  blocksize;
    int64_t  totalblocks;
    int64_t  totalsize;

    std::vector<FileSegment> filesegs;
    std::vector<FileInfo>     infiles;
    std::string              filepath;
    int               shared, recurse;

    MPI_Comm comm;

    int group_to_idx[GROUP_SIZE];
};

}

#endif

