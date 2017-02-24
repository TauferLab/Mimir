/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_FILE_READER_H
#define MIMIR_FILE_READER_H

#include <mpi.h>

#include "log.h"
#include "stat.h"
#include "config.h"
#include "globals.h"
#include "memory.h"
#include "inputsplit.h"
#include "interface.h"
#include "baseshuffler.h"
#include "baserecordformat.h"

namespace MIMIR_NS {

enum IOTYPE{MIMIR_STDC_IO, MIMIR_MPI_IO};

class InputSplit;
class BaseFileReader;

template<typename RecordFormat, IOTYPE iotype = MIMIR_STDC_IO>
class FileReader : public Readable {
  public:
    FileReader(InputSplit *input) {
        this->input = input;
        buffer = NULL;
        sbuffer = NULL;
        rbuffer = NULL;
    }

    virtual ~FileReader() {
    }

    std::string get_object_name() { return "FileReader"; }

    void set_shuffler(BaseShuffler *shuffler) {
        this->shuffler = shuffler;
    }

    virtual bool open() {

        LOG_PRINT(DBG_IO, "Filereader open.\n");

        if (input->get_max_fsize() <= (uint64_t)INPUT_BUF_SIZE)
            bufsize = input->get_max_fsize();
        else
            bufsize = INPUT_BUF_SIZE;

        PROFILER_RECORD_COUNT(COUNTER_MAX_FILE, (uint64_t) input->get_max_fsize(), OPMAX);

        buffer =  (char*)mem_aligned_malloc(MEMPAGE_SIZE,
                                            bufsize + MAX_RECORD_SIZE + 1);

        state.seg_file = NULL;
        state.read_size = 0;
        state.start_pos = 0;
        state.win_size = 0;
        state.has_tail = false;

        sbuffer = NULL;
        rbuffer = NULL;

        stailsize = 0;
        rtailsize = 0;

        sreq = MPI_REQUEST_NULL;
        rreq = MPI_REQUEST_NULL;
        creq = MPI_REQUEST_NULL;

        record = new RecordFormat();

        file_init();
        read_next_file();

        record_count = 0;

        return true;
    }

    virtual void close() {
        file_uninit();

        delete record;

        mem_aligned_free(buffer);
        MPI_Status st;
        if (creq != MPI_REQUEST_NULL) {
            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
            MPI_Wait(&creq, &st);
            TRACKER_RECORD_EVENT(EVENT_COMM_WAIT);
            creq = MPI_REQUEST_NULL;
        }
        if (sreq != MPI_REQUEST_NULL) {
            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
            MPI_Wait(&sreq, &st);
            TRACKER_RECORD_EVENT(EVENT_COMM_WAIT);
            sreq = MPI_REQUEST_NULL;
        }
        if (sbuffer != NULL)
            mem_aligned_free(sbuffer);
        LOG_PRINT(DBG_IO, "Filereader close.\n");
    }

    virtual uint64_t get_record_count() { return record_count++; }

    virtual RecordFormat* read() {

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
            record->set_buffer(ptr);

            bool islast = is_last_block();
            if(state.win_size > 0
               && record->has_full_record(ptr, state.win_size, islast)) {
                int record_size = record->get_record_size();
                if ((uint64_t)record_size >= state.win_size) {
                    state.win_size = 0;
                    state.start_pos = 0;
                }
                else {
                    state.start_pos += record_size;
                    state.win_size -= record_size;
                }
                record_count ++;
                return record;
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
        PROFILER_RECORD_COUNT(COUNTER_FILE_COUNT, 1, OPSUM);

        state.start_pos = 0;
        state.win_size = 0;
        state.read_size = 0;
        FileSeg *segfile = state.seg_file;
        if (segfile->startpos + segfile->segsize < segfile->filesize) {
            state.has_tail = true;
            //recv_start();
        }
        else
            state.has_tail = false;

        // read data
        uint64_t rsize;
        if (state.seg_file->segsize <= bufsize)
            rsize = state.seg_file->segsize;
        else
            rsize = bufsize;

        file_read_at(buffer, state.seg_file->startpos, rsize);
        PROFILER_RECORD_COUNT(COUNTER_FILE_SIZE, rsize, OPSUM);

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

        if (state.win_size > (uint64_t)MAX_RECORD_SIZE)
            LOG_ERROR("Record size (%ld) is larger than max size (%d)\n", 
                      state.win_size, MAX_RECORD_SIZE);

        for (uint64_t i = 0; i < state.win_size; i++)
            buffer[i] = buffer[state.start_pos + i];
        state.start_pos = 0;

        // recv tail from next process
        if (state.read_size == state.seg_file->segsize 
            && state.has_tail) {
            recv_start();
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
            PROFILER_RECORD_COUNT(COUNTER_FILE_SIZE, rsize, OPSUM);

            state.win_size += rsize;
            state.read_size += rsize;

            if (state.read_size == state.seg_file->segsize) {
                file_close();
            }
        }
    }

    int send_tail(char *buffer, uint64_t bufsize) {
        //MPI_Status st;

        FileSeg* seg_file = state.seg_file;

        if (seg_file->startpos > 0) {
            while ((uint64_t)stailsize < bufsize 
                   && !BaseRecordFormat::is_seperator(*(buffer + stailsize))) {
                stailsize++;
            }
            if (stailsize < 0)
                LOG_ERROR("Error: header size is larger than max value of int!\n");
            if ((uint64_t)stailsize >= bufsize
                && seg_file->startpos + bufsize < seg_file->filesize)
                LOG_ERROR("Error: cannot find header at the first buffer (bufsize=%ld)!\n", bufsize);
        }

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
        MPI_Isend(&stailsize, 1, MPI_INT, mimir_world_rank - 1, 
                  READER_COUNT_TAG, mimir_world_comm, &creq);
        TRACKER_RECORD_EVENT(EVENT_COMM_ISEND);

        if (stailsize != 0) {
            sbuffer = (char*)mem_aligned_malloc(MEMPAGE_SIZE, stailsize);
            memcpy(sbuffer, buffer, stailsize);
            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
            MPI_Isend(sbuffer, stailsize, MPI_BYTE, mimir_world_rank - 1, 
                      READER_DATA_TAG, mimir_world_comm, &sreq);
            TRACKER_RECORD_EVENT(EVENT_COMM_ISEND);
        }

        PROFILER_RECORD_COUNT(COUNTER_SEND_TAIL, (uint64_t) stailsize, OPSUM);

        LOG_PRINT(DBG_IO, "Send tail file=%s:%ld+%d\n", 
                  state.seg_file->filename.c_str(),
                  state.seg_file->startpos, stailsize);

        return stailsize;
    }

    void recv_start() {
        MPI_Status st;
        MPI_Request req;
        int flag = 0;

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
        MPI_Irecv(&rtailsize, 1, MPI_INT, mimir_world_rank + 1, 
                  READER_COUNT_TAG, mimir_world_comm, &req);
        TRACKER_RECORD_EVENT(EVENT_COMM_IRECV);

        while (1) {
            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
            MPI_Test(&req, &flag, &st);
            TRACKER_RECORD_EVENT(EVENT_COMM_TEST);
            if (flag) break;
            if (shuffler) shuffler->make_progress();
        };

        if (rtailsize != 0) {
            rbuffer = (char*)mem_aligned_malloc(MEMPAGE_SIZE,
                                                rtailsize);
            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
            MPI_Irecv(rbuffer, rtailsize, MPI_BYTE, mimir_world_rank + 1,
                      READER_DATA_TAG, mimir_world_comm, &rreq);
            TRACKER_RECORD_EVENT(EVENT_COMM_IRECV);
        }

        PROFILER_RECORD_COUNT(COUNTER_RECV_TAIL, (uint64_t) rtailsize, OPSUM);
    }

    int recv_tail(char *buffer, uint64_t bufsize){
        MPI_Status st;

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
        MPI_Wait(&rreq, &st);
        TRACKER_RECORD_EVENT(EVENT_COMM_WAIT);
        rreq = MPI_REQUEST_NULL;

        //MPI_Get_count(&st, MPI_BYTE, &count);

        if(rtailsize != 0) {
            if ((uint64_t)rtailsize > bufsize)
                LOG_ERROR("tail size %d is larger than buffer size %ld\n", rtailsize, bufsize);
            memcpy(buffer, rbuffer, rtailsize);
            mem_aligned_free(rbuffer);
        }

        LOG_PRINT(DBG_IO, "Recv tail file=%s:%ld+%d\n", 
                  state.seg_file->filename.c_str(),
                  state.seg_file->startpos + state.seg_file->segsize, 
                  rtailsize);

        return rtailsize;
    }

    virtual void file_init(){
        union_fp.c_fp = NULL;
    }

    virtual void file_uninit(){
    }

    virtual bool file_open(const char *filename){

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
        PROFILER_RECORD_TIME_START;

        union_fp.c_fp = fopen(filename, "r");
        if (union_fp.c_fp == NULL)
            return false;

        PROFILER_RECORD_TIME_END(TIMER_PFS_IO);
        TRACKER_RECORD_EVENT(EVENT_DISK_FOPEN);

        LOG_PRINT(DBG_IO, "Open input file=%s\n", 
                  state.seg_file->filename.c_str());

        return true;
    }

    virtual void file_read_at(char *buf, uint64_t offset, uint64_t size){
        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
        PROFILER_RECORD_TIME_START;

        fseek(union_fp.c_fp, offset, SEEK_SET);
        size = fread(buf, 1, size, union_fp.c_fp);

        PROFILER_RECORD_TIME_END(TIMER_PFS_IO);
        TRACKER_RECORD_EVENT(EVENT_DISK_FREADAT);

        LOG_PRINT(DBG_IO, "Read input file=%s:%ld+%ld\n", 
                  state.seg_file->filename.c_str(), offset, size);
    }

    virtual void file_close(){
        if (union_fp.c_fp != NULL) {

            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
            PROFILER_RECORD_TIME_START;

            fclose(union_fp.c_fp);

            PROFILER_RECORD_TIME_END(TIMER_PFS_IO);
            TRACKER_RECORD_EVENT(EVENT_DISK_FCLOSE);

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
    char             *sbuffer;
    char             *rbuffer;
    int               stailsize;
    int               rtailsize;
    InputSplit       *input;
    RecordFormat     *record;

    BaseShuffler     *shuffler;
    uint64_t          record_count;

    MPI_Request       sreq, rreq, creq;
};

template <typename RecordFormat>
class MPIFileReader : public FileReader< RecordFormat, MIMIR_MPI_IO >{
public:
    MPIFileReader(InputSplit *input) 
        : FileReader<RecordFormat, MIMIR_MPI_IO>(input) {
    }

    ~MPIFileReader(){
    }

protected:

    virtual void file_init() {
        this->union_fp.mpi_fp = MPI_FILE_NULL;
        sfile_idx = 0;
        create_comm();
    }

    virtual void file_uninit(){
        destroy_comm();
    }

    virtual bool file_open(const char *filename) {
        MPI_Comm file_comm = MPI_COMM_SELF;
        MPI_Request req;
        MPI_Status st;
        int flag = 0;

        while (sfile_idx < MAX_GROUPS
               && sfile_comms[sfile_idx] == MPI_COMM_NULL)
            sfile_idx++;

        if (sfile_idx < MAX_GROUPS) {
            file_comm = sfile_comms[sfile_idx];

            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
            MPI_Ibarrier(file_comm, &req);
            TRACKER_RECORD_EVENT(EVENT_COMM_IBARRIER);

            while (!flag) {
                TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
                MPI_Test(&req, &flag, &st);
                TRACKER_RECORD_EVENT(EVENT_COMM_TEST);
                 if (this->shuffler)
                    this->shuffler->make_progress();
            }
        }

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
        PROFILER_RECORD_TIME_START;

        MPI_File_open(file_comm, (char*)filename, MPI_MODE_RDONLY,
                      MPI_INFO_NULL, &(this->union_fp.mpi_fp));
        if (this->union_fp.mpi_fp == MPI_FILE_NULL) return false;

        PROFILER_RECORD_TIME_END(TIMER_PFS_IO);
        TRACKER_RECORD_EVENT(EVENT_DISK_MPIOPEN);

        LOG_PRINT(DBG_IO, "Collective MPI open input file=%s\n",
                  this->state.seg_file->filename.c_str());

        return true;
    }

    virtual void file_read_at(char *buf, uint64_t offset, uint64_t size) {
        MPI_Status st;
        MPI_Request req;
        int flag = 0;

        if (sfile_idx < MAX_GROUPS) {

            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
            MPI_Ibarrier(sfile_comms[sfile_idx], &req);
            TRACKER_RECORD_EVENT(EVENT_COMM_IBARRIER);

            while (!flag) {
                TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
                MPI_Test(&req, &flag, &st);
                TRACKER_RECORD_EVENT(EVENT_COMM_TEST);
                if (this->shuffler)
                    this->shuffler->make_progress();
            }
        }

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
        PROFILER_RECORD_TIME_START;

        MPI_File_read_at_all(this->union_fp.mpi_fp, offset, buf,
                             (int)size, MPI_BYTE, &st);

        PROFILER_RECORD_TIME_END(TIMER_PFS_IO);
        TRACKER_RECORD_EVENT(EVENT_DISK_MPIREADATALL);


        LOG_PRINT(DBG_IO, "Collective MPI read input file=%s:%ld+%ld\n", 
                  this->state.seg_file->filename.c_str(), offset, size);
    }

    virtual void file_close() {
        MPI_Status st;
        MPI_Request req;
        int flag = 0;

        if (this->union_fp.mpi_fp != MPI_FILE_NULL) {
            if (sfile_idx < MAX_GROUPS) {
                int remain_count = (int)ROUNDUP(this->state.seg_file->maxsegsize, this->bufsize) \
                                   - (int)ROUNDUP(this->state.seg_file->segsize, this->bufsize);
                for (int i = 0; i < remain_count; i++) {
                    TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
                    MPI_Ibarrier(sfile_comms[sfile_idx], &req);
                    TRACKER_RECORD_EVENT(EVENT_COMM_IBARRIER);

                    while (!flag) {
                        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
                        MPI_Test(&req, &flag, &st);
                        TRACKER_RECORD_EVENT(EVENT_COMM_TEST);
                        if (this->shuffler)
                            this->shuffler->make_progress();
                    }

                    TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
                    PROFILER_RECORD_TIME_START;

                    MPI_File_read_at_all(this->union_fp.mpi_fp, 0, NULL,
                             0, MPI_BYTE, &st);

                    PROFILER_RECORD_TIME_END(TIMER_PFS_IO);
                    TRACKER_RECORD_EVENT(EVENT_DISK_MPIREADATALL);
                }

                TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
                MPI_Ibarrier(sfile_comms[sfile_idx], &req);
                TRACKER_RECORD_EVENT(EVENT_COMM_IBARRIER);

                while (!flag) {
                    TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
                    MPI_Test(&req, &flag, &st);
                    TRACKER_RECORD_EVENT(EVENT_COMM_TEST);
                    if (this->shuffler)
                        this->shuffler->make_progress();
                }

                TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
                PROFILER_RECORD_TIME_START;

                MPI_File_close(&(this->union_fp.mpi_fp));

                PROFILER_RECORD_TIME_END(TIMER_PFS_IO);
                TRACKER_RECORD_EVENT(EVENT_DISK_MPICLOSE);

                sfile_idx++;
            } else {

                TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
                PROFILER_RECORD_TIME_START;

                MPI_File_close(&(this->union_fp.mpi_fp));

                PROFILER_RECORD_TIME_END(TIMER_PFS_IO);
                TRACKER_RECORD_EVENT(EVENT_DISK_MPICLOSE);
            }

            this->union_fp.mpi_fp = MPI_FILE_NULL;

            LOG_PRINT(DBG_IO, "Collective MPI close input file=%s\n", 
                      this->state.seg_file->filename.c_str());
        }
    }

    void create_comm(){
        LOG_PRINT(DBG_IO, "create comm start.\n");

        MPI_Group world_group, sfile_groups[MAX_GROUPS];

        MPI_Comm_group(mimir_world_comm, &world_group);

        for (int i = 0; i < MAX_GROUPS; i++) {
            int ranks[mimir_world_size], n = 1;
            int low_rank, high_rank;

            FileSeg *fileseg = this->input->get_share_file(i);
            if (fileseg) {
                low_rank = fileseg->startrank;
                high_rank = fileseg->endrank + 1;
            }
            else {
                low_rank = mimir_world_rank;
                high_rank = low_rank + 1;
            }

            n = high_rank - low_rank;
            for (int j = 0; j < n; j++) {
                ranks[j] = low_rank + j;
            }

            MPI_Group_incl(world_group, n, ranks, &sfile_groups[i]);
            MPI_Comm_create(mimir_world_comm, 
                            sfile_groups[i], 
                            &sfile_comms[i]);
            MPI_Group_free(&sfile_groups[i]);

            if (n == 1) sfile_comms[i] = MPI_COMM_NULL;
        }

        MPI_Group_free(&world_group);

        LOG_PRINT(DBG_IO, "create comm end.\n");
    }

    void destroy_comm(){
        for (int i = 0; i < MAX_GROUPS; i++)
            if (sfile_comms[i] != MPI_COMM_NULL)
                MPI_Comm_free(&sfile_comms[i]);
    }

    MPI_Comm sfile_comms[MAX_GROUPS];
    int      sfile_idx;
};

}

#endif
