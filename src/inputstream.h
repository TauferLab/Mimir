#ifndef MIMIR_FILE_READER_H
#define MIMIR_FILE_READER_H

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>

#include "filesplitter.h"

namespace MIMIR_NS {

class InputStream;

typedef void (*UserSplit)(InputStream&);

class InputStream {
  public:
    InputStream(int64_t blocksize,
                const char *filepath, 
                int sharedflag,
                int recurse,
                MPI_Comm comm) {

        splitter = new FileSplitter(blocksize, 
                                    filepath, 
                                    sharedflag, 
                                    recurse, 
                                    comm);
        splitter->split();

        win.file_count = splitter->get_file_count();
        win.total_size = splitter->get_total_size();
        win.block_size = blocksize;

        fp = NULL;
        inbuf = NULL;
        inbufsize = 0;

        open_stream();
    }
    virtual ~InputStream(){
        close_stream();

        delete splitter;
    }

    virtual int operator>>(char& c);
    virtual int   get_char(char&);

  protected:
    virtual void read_files();
    virtual bool open_stream();
    virtual void close_stream();

    void _file_open(const char*);
    void _read_at(char*, int64_t, int64_t);
    void _close();

    void _print_win(){
        printf("Buffer window: %ld->%ld, file window: [%ld %ld]->[%ld %ld]\n", 
               win.left_buf_off, win.right_buf_off, 
               win.left_file_idx, win.left_file_off,
               win.right_file_idx, win.right_file_off);
    };

    struct FileWindows{
        int64_t left_file_idx;
        int64_t left_file_off;
        int64_t right_file_idx;
        int64_t right_file_off;
        int64_t left_buf_off;
        int64_t right_buf_off;
        int64_t file_count;
        int64_t total_size;
        int64_t block_size;
    };

    FileSplitter   *splitter;
    char           *inbuf;
    int64_t         inbufsize;

    FileWindows     win;        // slide window of the files

  private:

    enum IOMethod{CLIBIO, MPIIO} iotype;
    union FilePtr{
        FILE    *c_fp;
        MPI_File mpi_fp;
    } union_fp;

    FILE *fp;
};


class CollectiveInputStream : public InputStream {
  public:
    CollectiveInputStream(
                          int64_t blocksize,
                          const char* filepath, 
                          int sharedflag,
                          int recurse, 
                          MPI_Comm comm) : 
        InputStream(blocksize, filepath, sharedflag, recurse, comm){
        mpi_fp = MPI_FILE_NULL;
        global_comm = comm;

        MPI_Comm_rank(global_comm, &me);
        MPI_Comm_size(global_comm, &nprocs);

        splitter->print();

        _create_comm();
    }
    virtual ~CollectiveInputStream(){
        _destroy_comm();
    }

  protected:
    virtual void read_files();
    virtual bool open_stream(){
        return InputStream::open_stream();
    }
    virtual void close_stream(){
        InputStream::close_stream();
    }

    void _create_comm();
    void _destroy_comm();
    bool _read_group_files();

#define GROUP_SIZE  2
    int      me, nprocs;
    MPI_Comm global_comm;
    MPI_Comm local_comms[GROUP_SIZE];
    int      max_comm_count[GROUP_SIZE];
    int      cur_comm_count[GROUP_SIZE];

  private:
    MPI_File    mpi_fp;
};

}

#endif
