#ifndef MIMIR_FILE_READER_H
#define MIMIR_FILE_READER_H

#include <mpi.h>

#include "globals.h"
#include "const.h"
#include "memory.h"
#include "config.h"
#include "log.h"
#include "inputsplit.h"
#include "basefilereader.h"
#include "baserecordformat.h"

namespace MIMIR_NS {

#define  DATA_TAG     0xaa
enum IOTYPE{MIMIR_STDC_IO, MIMIR_MPI_IO, MIMIR_COLLEC_IO};

class InputSplit;
class BaseFileReader;

template<typename RecordFormat, IOTYPE iotype = MIMIR_STDC_IO>
class FileReader : public BaseFileReader {
  public:
    FileReader(InputSplit *input) {
        this->input = input;
        buffer = NULL;
    }

    virtual ~FileReader() {
    }

    bool open() {

        if (input->get_max_fsize() <= (uint64_t)INPUT_BUF_SIZE)
            bufsize = input->get_max_fsize();
        else
            bufsize = INPUT_BUF_SIZE;

        buffer =  (char*)mem_aligned_malloc(MEMPAGE_SIZE,
                                            bufsize + MAX_RECORD_SIZE + 1);

        state.seg_file = NULL;
        state.read_size = 0;
        state.start_pos = 0;
        state.win_size = 0;
        state.has_tail = false;

        req = MPI_REQUEST_NULL;

        file_init();
        read_next_file();

        return true;
    }

    void close() {
        mem_aligned_free(buffer);
    }

    RecordFormat* next() {

      if (state.seg_file == NULL)
            return NULL;

        bool is_empty = false;
        while(!is_empty) {

            // skip whitespace
            while (state.win_size > 0
                   && BaseRecordFormat::is_whitespace(*(buffer + state.start_pos))) {
                state.start_pos++;
                state.win_size--;
            }

            char *ptr = buffer + state.start_pos;
            record.set_buffer(ptr);

            bool islast = is_last_block();
            if(state.win_size > 0
               && record.has_full_record(ptr, state.win_size, islast)) {
                int record_size = record.get_record_size();
                if ((uint64_t)record_size >= state.win_size) {
                    state.win_size = 0;
                    state.start_pos = 0;
                }
                else {
                    state.start_pos += record_size;
                    state.win_size -= record_size;
                }
                return &record;
            }
            // ignore the last record
            else if (islast) {
                if (!read_next_file())
                    is_empty = true;
            }
            else {
                handle_border();
            }
        };

        return NULL;
    }

  protected:

    bool is_last_block() {
        if (state.read_size == state.seg_file->segsize 
            && !state.has_tail)
            return true;
        return false;
    }

    bool read_next_file() {
        // close possible previous file
        file_close();

        // open the next file
        state.seg_file = input->get_next_file();
        if (state.seg_file == NULL)
            return false;

        if (!file_open(state.seg_file->filename.c_str()))
            return false;

        state.start_pos = 0;
        state.win_size = 0;
        state.read_size = 0;
        FileSeg *segfile = state.seg_file;
        if (segfile->startpos + segfile->segsize < segfile->filesize)
            state.has_tail = true;
        else
            state.has_tail = false;

        // read data
        uint64_t rsize;
        if (state.seg_file->segsize <= bufsize)
            rsize = state.seg_file->segsize;
        else
            rsize = bufsize;
        file_read_at(buffer, state.seg_file->startpos, rsize);
        state.win_size += rsize;
        state.read_size += rsize;

        // skip tail of previous process
        if (state.seg_file->startpos > 0) {
            int count = send_tail(buffer, rsize);
            state.start_pos += count;
            state.win_size -= count;
        }

        // close file
        if (state.read_size == state.seg_file->segsize) {
            file_close();
        }

        return true;
    }

    void handle_border() {
        MPI_Status st;
        if (req != MPI_REQUEST_NULL) {
            MPI_Wait(&req, &st);
            req = MPI_REQUEST_NULL;
        }

        if (state.win_size > (uint64_t)MAX_RECORD_SIZE)
            LOG_ERROR("Record size (%ld) is larger than max size (%d)\n", 
                      state.win_size, MAX_RECORD_SIZE);

        for (uint64_t i = 0; i < state.win_size; i++)
            buffer[i] = buffer[state.start_pos + i];
        state.start_pos = 0;

        // recv tail from next process
        if (state.read_size == state.seg_file->segsize 
            && state.has_tail) {
            int count = recv_tail(buffer + state.win_size, bufsize);
            state.win_size += count;
            state.has_tail = false;
        }
        else {
            uint64_t rsize;
            if (state.seg_file->segsize - state.read_size <= bufsize) 
                rsize = state.seg_file->segsize - state.read_size;
            else
                rsize = bufsize;

            file_read_at(buffer + state.win_size,
                         state.seg_file->startpos + state.read_size, rsize);

            state.win_size += rsize;
            state.read_size += rsize;

            if (state.read_size == state.seg_file->segsize)
                file_close();
        }
    }

    int send_tail(char *buffer, uint64_t bufsize) {
        MPI_Status st;
        int count = 0;

        if (req != MPI_REQUEST_NULL) {
            MPI_Wait(&req, &st);
            req = MPI_REQUEST_NULL;
        }

        if (state.seg_file->startpos > 0) {
            while (!BaseRecordFormat::is_seperator(*(buffer+count))
                  && (uint64_t)count < bufsize) {
                count++;
            }
            if (count < 0)
                LOG_ERROR("Error: header size is larger than max value of int!\n");
            if ((uint64_t)count >= bufsize)
                LOG_ERROR("Error: cannot find header at the first buffer!\n");
        }

        MPI_Isend(buffer, count, MPI_BYTE, mimir_world_rank - 1, 
                  DATA_TAG, mimir_world_comm, &req);

        LOG_PRINT(DBG_IO, "Send tail file=%s:%ld+%d\n", 
                  state.seg_file->filename.c_str(),
                  state.seg_file->startpos, count);

        return count;
    }

    int recv_tail(char *buffer, uint64_t bufsize){
        MPI_Status st;
        int count;

        MPI_Irecv(buffer, (int)bufsize, MPI_BYTE, mimir_world_rank + 1,
                  DATA_TAG, mimir_world_comm, &req);
        MPI_Wait(&req, &st);

        MPI_Get_count(&st, MPI_BYTE, &count);

        LOG_PRINT(DBG_IO, "Recv tail file=%s:%ld+%d\n", 
                  state.seg_file->filename.c_str(),
                  state.seg_file->startpos + state.seg_file->segsize, 
                  count);

        return count;
    }

    virtual void file_init(){
        union_fp.c_fp = NULL;
    }

    virtual bool file_open(const char *filename){
        union_fp.c_fp = fopen(filename, "r");
        if (union_fp.c_fp == NULL)
            return false;

        LOG_PRINT(DBG_IO, "Open input file=%s\n", 
                  state.seg_file->filename.c_str());

        return true;
    }

    virtual void file_read_at(char *buf, uint64_t offset, uint64_t size){
        fseek(union_fp.c_fp, offset, SEEK_SET);
        size = fread(buf, 1, size, union_fp.c_fp);

        LOG_PRINT(DBG_IO, "Read input file=%s:%ld+%ld\n", 
                  state.seg_file->filename.c_str(), offset, size);
    }

    virtual void file_close(){
        if (union_fp.c_fp != NULL) {
            fclose(union_fp.c_fp);
            union_fp.c_fp = NULL;

            LOG_PRINT(DBG_IO, "Close input file=%s\n", 
                      state.seg_file->filename.c_str());
        }
    }

    union FilePtr{
        FILE    *c_fp;
        MPI_File mpi_fp;
    } union_fp;

    struct FileState{
        FileSeg  *seg_file;
        uint64_t  read_size;
        uint64_t  start_pos;
        uint64_t  win_size;
        bool      has_tail;
    }state;

    void print_state(){
        printf("%d[%d] file_name=%s:%ld+%ld, read_size=%ld, start_pos=%ld, win_size=%ld, has_tail=%d\n",
               mimir_world_rank, mimir_world_size,
               state.seg_file->filename.c_str(),
               state.seg_file->startpos,
               state.seg_file->segsize,
               state.read_size,
               state.start_pos,
               state.win_size,
               state.has_tail);
    }

    char             *buffer;
    uint64_t          bufsize;
    InputSplit       *input;
    RecordFormat      record;

    MPI_Request        req;
};

template <typename RecordFormat>
class MPIFileReader : public FileReader<RecordFormat, MIMIR_MPI_IO> {
public:
    MPIFileReader(InputSplit *input)
        : FileReader<RecordFormat, MIMIR_MPI_IO>(input) {
    }

    ~MPIFileReader() {
    }

protected:
    virtual void file_init() {
        this->union_fp.mpi_fp = MPI_FILE_NULL;
    }

    virtual bool file_open(const char *filename) {
        MPI_File_open(MPI_COMM_SELF, (char*)filename, MPI_MODE_RDONLY,
                      MPI_INFO_NULL, &(this->union_fp.mpi_fp));
        if (this->union_fp.mpi_fp == MPI_FILE_NULL) return false;

        LOG_PRINT(DBG_IO, "MPI open input file=%s\n",
                  this->state.seg_file->filename.c_str());

        return true;
    }

    virtual void file_read_at(char *buf, uint64_t offset, uint64_t size) {
        MPI_Status st;
        MPI_File_read_at(this->union_fp.mpi_fp, offset, buf,
                         (int)size, MPI_BYTE, &st);

        LOG_PRINT(DBG_IO, "MPI read input file=%s:%ld+%ld\n",
                  this->state.seg_file->filename.c_str(), offset, size);
    }

    virtual void file_close(){
        if (this->union_fp.mpi_fp != MPI_FILE_NULL) {
            MPI_File_close(&(this->union_fp.mpi_fp));
            this->union_fp.mpi_fp = MPI_FILE_NULL;

            LOG_PRINT(DBG_IO, "MPI close input file=%s\n", 
                      this->state.seg_file->filename.c_str());
        }
    }
};

template <typename RecordFormat>
class CollecFileReader : public FileReader< RecordFormat, MIMIR_COLLEC_IO >{
public:
    CollecFileReader(InputSplit *input) 
        : FileReader<RecordFormat, MIMIR_COLLEC_IO>(input) {
    }

    ~CollecFileReader(){
    }

protected:

    virtual void file_init() {
        this->union_fp.mpi_fp = MPI_FILE_NULL;
        sfile_idx = 0;
    }

    virtual bool file_open(const char *filename) {
        MPI_Comm file_comm = MPI_COMM_SELF;
        if (sfile_idx < sfile_count) {
            file_comm = sfile_comms[sfile_idx];
        }

        MPI_File_open(file_comm, (char*)filename, MPI_MODE_RDONLY,
                      MPI_INFO_NULL, &(this->union_fp.mpi_fp));
        if (this->union_fp.mpi_fp == MPI_FILE_NULL) return false;

        LOG_PRINT(DBG_IO, "Collective MPI open input file=%s\n",
                  this->state.seg_file->filename.c_str());

        return true;
    }

    virtual void file_read_at(char *buf, uint64_t offset, uint64_t size) {
        MPI_Status st;
        MPI_File_read_at_all(this->union_fp.mpi_fp, offset, buf,
                             (int)size, MPI_BYTE, &st);

        LOG_PRINT(DBG_IO, "Collective MPI read input file=%s:%ld+%ld\n", 
                  this->state.seg_file->filename.c_str(), offset, size);
    }

    virtual void file_close() {

        if (this->union_fp.mpi_fp != MPI_FILE_NULL) {
            if (sfile_idx < sfile_count) {
                int remain_count = (int)ROUNDUP(this->state.seg_file->maxsegsize, this->bufsize) \
                                   - (int)ROUNDUP(this->state.seg_file->segsize, this->bufsize);
                for (int i = 0; i < remain_count; i++) {
                    MPI_Status st;
                    MPI_File_read_at_all(this->union_fp.mpi_fp, 0, NULL,
                             0, MPI_BYTE, &st);
                }
                sfile_idx++;
            }

            MPI_File_close(&(this->union_fp.mpi_fp));
            this->union_fp.mpi_fp = MPI_FILE_NULL;

            LOG_PRINT(DBG_IO, "Collective MPI close input file=%s\n", 
                      this->state.seg_file->filename.c_str());
        }
    }

    void create_comm(){

        sfile_count = 0;
        MPI_Group world_group, sfile_groups[MAX_GROUPS];
        MPI_Comm_group(mimir_world_comm, &world_group);

        FileSeg *fileseg = this->input->get_next_file();
        for (int i = 0; i < MAX_GROUPS; i++) {
            int ranks[mimir_world_size], n = 1;
            int low_rank, high_rank;
            if (fileseg && fileseg->readorder == i) {
                low_rank = fileseg->startrank;
                high_rank = fileseg->endrank + 1;
                fileseg = this->input->get_next_file();
                sfile_count++;
            }
            else {
                low_rank = mimir_world_rank;
                high_rank = low_rank + 1;
            }

            for (int j = 0; j < n; j++) {
                ranks[j] = low_rank + j;
            }

            MPI_Group_incl(world_group, n, ranks, &sfile_groups[i]);
            MPI_Comm_create(mimir_world_comm, 
                            sfile_groups[i], 
                            &sfile_comms[sfile_count]);
            MPI_Group_free(&sfile_groups[i]);
        }

        MPI_Group_free(&world_group);
    }

    void destroy_comm(){
        for (int i = 0; i < MAX_GROUPS; i++)
            MPI_Comm_free(&sfile_comms[i]);
    }

    MPI_Comm sfile_comms[MAX_GROUPS];
    int      sfile_count, sfile_idx;
};

}

#endif
