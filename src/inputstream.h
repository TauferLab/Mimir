#ifndef MIMIR_FILE_READER_H
#define MIMIR_FILE_READER_H

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>

#include "filesplitter.h"

namespace MIMIR_NS {

class InputStream;

typedef bool (*UserSplit)(InputStream*, void *ptr);

// the stream contains bytes from one or multiple files
// there is a current byte pointer ( the byte is etheir 
// a byte from file or EOF in the case with multiple files)
// when the pointer comes to end, the stream becomes empty
class InputStream {
  public:
    InputStream(int64_t blocksize,
                const char *filepath, 
                int sharedflag,
                int recurse,
                MPI_Comm comm,
                UserSplit splitcb,
                void *splitptr) {

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

        this->splitcb = splitcb;
        this->splitptr = splitptr;

        global_comm = comm;

        MPI_Comm_rank(global_comm, &me);
        MPI_Comm_size(global_comm, &nprocs);

        iscb = false;

    }
    virtual ~InputStream(){

        delete splitter;
    }


    // get current byte
    virtual unsigned char get_byte();
    virtual unsigned char operator*();
    // move to next byte
    virtual void next();
    virtual void operator++();
    // if EOF
    virtual bool is_eof();
    // if empty
    virtual bool is_empty();

    virtual bool open_stream();
    virtual void close_stream();

    //virtual int get_char(char& c);

  protected:
    virtual void read_files();
    virtual void send_tail();
    virtual bool recv_tail();

    void _file_open(const char*);
    void _read_at(char*, int64_t, int64_t);
    void _close();

    void _print_win(std::string prefix="somewhere"){

        printf("%d[%d] %s buffer window: %ld->%ld, file window: [%ld %ld]->[%ld %ld], filecount=%ld, totalsize=%ld, tail=[%d %d]\n",
               me, nprocs, prefix.c_str(),
               win.left_buf_off, win.right_buf_off, 
               win.left_file_idx, win.left_file_off,
               win.right_file_idx, win.right_file_off,
               win.file_count, win.total_size, 
               win.tail_left_off, win.tail_right_off);
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
        int     tail_left_off;
        int     tail_right_off;
        bool    tail_done;
    };

    FileSplitter   *splitter;
    char           *inbuf;
    int64_t         inbufsize;
    char           *tailbuf;
    int             tailbufsize;
    UserSplit       splitcb;
    void           *splitptr;
    bool            iscb;
    MPI_Request     tailreq;

    int      me, nprocs;
    MPI_Comm global_comm;

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
                          MPI_Comm comm,
                          UserSplit splitcb,
                          void *splitptr) :
        InputStream(blocksize, filepath, 
                    sharedflag, recurse, 
                    comm, splitcb, splitptr){

            mpi_fp = MPI_FILE_NULL;

            splitter->print();

            _create_comm();
        }
    virtual ~CollectiveInputStream(){
        _destroy_comm();
    }

   virtual bool open_stream(){
        return InputStream::open_stream();
    }
    virtual void close_stream(){
        InputStream::close_stream();
    }

  protected:
    virtual void read_files();

    void _create_comm();
    void _destroy_comm();
    bool _read_group_files();

#define GROUP_SIZE  2
    MPI_Comm local_comms[GROUP_SIZE];
    int      max_comm_count[GROUP_SIZE];
    int      cur_comm_count[GROUP_SIZE];

  private:
    MPI_File    mpi_fp;
};

}

#endif
