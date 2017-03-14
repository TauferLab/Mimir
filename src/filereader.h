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

//enum IOTYPE {MIMIR_STDC_IO, MIMIR_MPI_IO};

#define BORDER_LEFT       0
#define BORDER_RIGHT      1
#define BORDER_SIZE       2

class InputSplit;
class BaseFileReader;

template <typename RecordFormat>
class MPIFileReader;

template<typename RecordFormat>
class FileReader : public Readable {
  public:
    static FileReader<RecordFormat> *getReader(InputSplit *input);
    static FileReader<RecordFormat> *reader;

  public:
    FileReader(InputSplit *input) {
        this->input = input;
        buffer = NULL;

        for (int i = 0; i < BORDER_SIZE; i++) {
            border_buffers[i] = NULL;
            border_sizes[i] = 0;
            border_cmds[i] = 0;
            border_reqs[i] = MPI_REQUEST_NULL;
            border_creqs[i] = MPI_REQUEST_NULL;
        }
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
        state.has_head = false;
        state.has_tail = false;

        for (int i = 0; i < BORDER_SIZE; i++) {
            border_buffers[i] = NULL;
            border_sizes[i] = 0;
            border_cmds[i] = 0;
            border_reqs[i] = MPI_REQUEST_NULL;
            border_creqs[i] = MPI_REQUEST_NULL;
        }

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
        for (int i = 0; i < BORDER_SIZE; i++) {
            if (border_buffers[i] != NULL)
                mem_aligned_free(border_buffers[i]);
            if (border_reqs[i] != MPI_REQUEST_NULL) {
                TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
                MPI_Wait(&border_reqs[i], &st);
                border_reqs[i] = MPI_REQUEST_NULL;
                TRACKER_RECORD_EVENT(EVENT_SYN_COMM);
            }
            if (border_creqs[i] != MPI_REQUEST_NULL) {
                TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
                MPI_Wait(&border_creqs[i], &st);
                border_creqs[i] = MPI_REQUEST_NULL;
                TRACKER_RECORD_EVENT(EVENT_SYN_COMM);
            }
        }
        LOG_PRINT(DBG_IO, "Filereader close.\n");
    }

    virtual uint64_t get_record_count() { return record_count++; }

    virtual RecordFormat* read() {

        if (state.seg_file == NULL)
            return NULL;

        bool is_empty = false;
        while(!is_empty) {

            char *ptr = buffer + state.start_pos; 
            int skip_count = record->skip_count(ptr, state.win_size);
            state.start_pos += skip_count;
            state.win_size -= skip_count;

            ptr = buffer + state.start_pos;
            record->set_buffer(ptr);
            bool islast = is_last_block();
            if(state.win_size > 0
               && record->has_full_record(ptr, state.win_size, islast)) {
                int move_count = record->move_count(ptr, state.win_size, islast);
                if ((uint64_t)move_count >= state.win_size) {
                    state.win_size = 0;
                    state.start_pos = 0;
                }
                else {
                    state.start_pos += move_count;
                    state.win_size -= move_count;
                }
                record_count ++;
                return record;
            }
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
            && !state.has_tail && !state.has_head)
            return true;
        return false;
    }

    bool read_next_file() {
        // close possible previous file
        file_close();

        // open the next file
        state.seg_file = input->get_next_file();
        if (state.seg_file == NULL) {
            return false;
        }

        if (!file_open(state.seg_file->filename.c_str())) {
            LOG_ERROR("Open file %s error!\n", state.seg_file->filename.c_str());
            return false;
        }
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
            int count = handle_left_border_start(buffer, rsize);
            if (count > 0) state.has_head = true;
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
        if (state.read_size == state.seg_file->segsize ) {
            if (state.has_tail) {
                int count = handle_right_border(buffer + state.win_size, bufsize);
                state.win_size += count;
                state.has_tail = false;
                //print_state();
            } else if  (state.has_head) {
                int count = handle_left_border_end(buffer, bufsize);
                state.win_size += count;
                state.has_head = false;
            }
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

    int handle_left_border_start (char *buffer, uint64_t bufsize) {

        border_sizes[BORDER_LEFT] = record->get_left_border(buffer, bufsize,
                                                            !state.has_tail);

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
        border_cmds[BORDER_LEFT] = border_sizes[BORDER_LEFT];
        PROFILER_RECORD_TIME_START;
        MPI_Isend(&border_cmds[BORDER_LEFT], 1, MPI_INT, mimir_world_rank - 1, 
                  READER_CMD_TAG, mimir_world_comm, &border_creqs[BORDER_LEFT]);
        PROFILER_RECORD_TIME_END(TIMER_COMM_ISEND);
        TRACKER_RECORD_EVENT(EVENT_COMM_ISEND);

        if (border_sizes[BORDER_LEFT] != 0) {
            border_buffers[BORDER_LEFT] 
                = (char*)mem_aligned_malloc(MEMPAGE_SIZE, border_sizes[BORDER_LEFT]);
            memcpy(border_buffers[BORDER_LEFT], buffer, border_sizes[BORDER_LEFT]);
            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
            PROFILER_RECORD_TIME_START;
            MPI_Isend(border_buffers[BORDER_LEFT], border_sizes[BORDER_LEFT], 
                      MPI_BYTE, mimir_world_rank - 1, 
                      READER_DATA_TAG, mimir_world_comm, &border_reqs[BORDER_LEFT]);
            PROFILER_RECORD_TIME_END(TIMER_COMM_ISEND);
            TRACKER_RECORD_EVENT(EVENT_COMM_ISEND);
        }

        PROFILER_RECORD_COUNT(COUNTER_SEND_TAIL, 
                              (uint64_t) border_sizes[BORDER_LEFT], OPSUM);

        LOG_PRINT(DBG_IO, "Send tail file=%s:%ld+%d\n", 
                  state.seg_file->filename.c_str(),
                  state.seg_file->startpos, border_sizes[BORDER_LEFT]);

        return border_sizes[BORDER_LEFT];
    }

    int handle_left_border_end (char *buffer, uint64_t bufsize) {
        int count = 0;

#if 0
        MPI_Status st;
        int flag = 0;
        if (record->has_right_cmd() ) {
            if (border_creqs[BORDER_LEFT] != MPI_REQUEST_NULL) {
                MPI_Wait(&border_creqs[BORDER_LEFT], &st);
            }

            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
            MPI_Irecv(&border_cmds[BORDER_LEFT], 1, MPI_INT,
                      mimir_world_rank - 1, READER_CMD_TAG,
                      mimir_world_comm, &border_creqs[BORDER_LEFT]);
            TRACKER_RECORD_EVENT(EVENT_COMM_IRECV);

            while (1) {
                MPI_Test(&border_creqs[BORDER_LEFT], &flag, &st);
                if (flag) break;
                if (shuffler) shuffler->make_progress();
            };

            count = record->process_left_border(border_creqs[BORDER_LEFT],
                                                buffer, bufsize,
                                                border_buffers[BORDER_LEFT],
                                                border_sizes[BORDER_LEFT]);
        }
#endif

        return count;
    }

    int handle_right_border (char *buffer, uint64_t bufsize) {
        MPI_Status st;
        int flag = 0;

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
        PROFILER_RECORD_TIME_START;
        MPI_Irecv(&border_sizes[BORDER_RIGHT], 1, MPI_INT,
                  mimir_world_rank + 1, READER_CMD_TAG,
                  mimir_world_comm, &border_creqs[BORDER_RIGHT]);
        PROFILER_RECORD_TIME_END(TIMER_COMM_IRECV);
        TRACKER_RECORD_EVENT(EVENT_COMM_IRECV);

        flag = 0;
        while (!flag) {
            PROFILER_RECORD_TIME_START;
            MPI_Test(&border_creqs[BORDER_RIGHT], &flag, &st);
            PROFILER_RECORD_TIME_END(TIMER_COMM_TEST);
            if (shuffler) shuffler->make_progress();
        };
        TRACKER_RECORD_EVENT(EVENT_SYN_COMM);

        border_creqs[BORDER_RIGHT] = MPI_REQUEST_NULL;

        if (border_sizes[BORDER_RIGHT] != 0) {
            border_buffers[BORDER_RIGHT] 
                = (char*)mem_aligned_malloc(MEMPAGE_SIZE, border_sizes[BORDER_RIGHT]);
            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
            PROFILER_RECORD_TIME_START;
            MPI_Irecv(border_buffers[BORDER_RIGHT], border_sizes[BORDER_RIGHT], 
                      MPI_BYTE, mimir_world_rank + 1,
                      READER_DATA_TAG, mimir_world_comm, &border_reqs[BORDER_RIGHT]);
            PROFILER_RECORD_TIME_END(TIMER_COMM_IRECV);
            TRACKER_RECORD_EVENT(EVENT_COMM_IRECV);
        }

        PROFILER_RECORD_COUNT(COUNTER_RECV_TAIL, 
                              (uint64_t) border_sizes[BORDER_RIGHT], OPSUM);

#if 0
        if (record->has_right_cmd()) {
            border_cmds[BORDER_RIGHT] = record->get_right_cmd();
            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
            MPI_Isend(&border_cmds[BORDER_RIGHT], 1, MPI_INT, mimir_world_rank + 1, 
                      READER_CMD_TAG, mimir_world_comm, &border_creqs[BORDER_RIGHT]);
            TRACKER_RECORD_EVENT(EVENT_COMM_ISEND);
        }
#endif

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
        flag = 0;
        while (!flag) {
            PROFILER_RECORD_TIME_START;
            MPI_Test(&border_reqs[BORDER_RIGHT], &flag, &st);
            PROFILER_RECORD_TIME_END(TIMER_COMM_TEST);
            if (shuffler) shuffler->make_progress();
        };
        //MPI_Wait(&border_reqs[BORDER_RIGHT], &st);
        TRACKER_RECORD_EVENT(EVENT_SYN_COMM);
        border_reqs[BORDER_RIGHT] = MPI_REQUEST_NULL;

        if (border_sizes[BORDER_RIGHT] != 0) {
            if ((uint64_t)border_sizes[BORDER_RIGHT] > bufsize)
                LOG_ERROR("tail size %d is larger than buffer size %ld\n",
                          border_sizes[BORDER_RIGHT], bufsize);
            memcpy(buffer, border_buffers[BORDER_RIGHT], border_sizes[BORDER_RIGHT]);
            mem_aligned_free(border_buffers[BORDER_RIGHT]);
            border_buffers[BORDER_RIGHT] = NULL;
        }

        LOG_PRINT(DBG_IO, "Recv tail file=%s:%ld+%d\n", 
                  state.seg_file->filename.c_str(),
                  state.seg_file->startpos + state.seg_file->segsize, 
                  border_sizes[BORDER_RIGHT]);

        return border_sizes[BORDER_RIGHT];
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

        PROFILER_RECORD_TIME_END(TIMER_PFS_INPUT);
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

        PROFILER_RECORD_TIME_END(TIMER_PFS_INPUT);
        TRACKER_RECORD_EVENT(EVENT_DISK_FREADAT);

        LOG_PRINT(DBG_IO, "Read input file=%s:%ld+%ld\n", 
                  state.seg_file->filename.c_str(), offset, size);
    }

    virtual void file_close(){
        if (union_fp.c_fp != NULL) {

            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
            PROFILER_RECORD_TIME_START;

            fclose(union_fp.c_fp);

            PROFILER_RECORD_TIME_END(TIMER_PFS_INPUT);
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
        bool      has_head;
        bool      has_tail;
    }state;

    void print_state(){
        printf("%d[%d] file_name=%s:%ld+%ld, read_size=%ld, start_pos=%ld, win_size=%ld, has_tail=%d, has_head=%d\n",
               mimir_world_rank, mimir_world_size,
               state.seg_file->filename.c_str(),
               state.seg_file->startpos,
               state.seg_file->segsize,
               state.read_size,
               state.start_pos,
               state.win_size,
               state.has_tail,
               state.has_head);
    }

    char             *buffer;
    uint64_t          bufsize;

    InputSplit       *input;
    RecordFormat     *record;

    BaseShuffler     *shuffler;
    uint64_t          record_count;

    char*        border_buffers[BORDER_SIZE];
    int          border_sizes[BORDER_SIZE];
    int          border_cmds[BORDER_SIZE];
    MPI_Request  border_reqs[BORDER_SIZE];
    MPI_Request  border_creqs[BORDER_SIZE];
};

template <typename RecordFormat>
class MPIFileReader : public FileReader< RecordFormat >{
public:
    MPIFileReader(InputSplit *input) 
        : FileReader<RecordFormat>(input) {
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

        LOG_PRINT(DBG_IO, "Collective MPI open input file=%s start\n",
                  this->state.seg_file->filename.c_str());

        while (sfile_idx < MAX_GROUPS
               && sfile_comms[sfile_idx] == MPI_COMM_NULL)
            sfile_idx++;

        if (sfile_idx < MAX_GROUPS) {
            file_comm = sfile_comms[sfile_idx];

            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
            MPI_Ibarrier(file_comm, &req);
            flag = 0;
            while (!flag) {
                PROFILER_RECORD_TIME_START;
                MPI_Test(&req, &flag, &st);
                PROFILER_RECORD_TIME_END(TIMER_COMM_TEST);
                 if (this->shuffler)
                    this->shuffler->make_progress();
            }
            TRACKER_RECORD_EVENT(EVENT_SYN_COMM);

        }

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
        PROFILER_RECORD_TIME_START;

        MPI_File_open(file_comm, (char*)filename, MPI_MODE_RDONLY,
                      MPI_INFO_NULL, &(this->union_fp.mpi_fp));
        if (this->union_fp.mpi_fp == MPI_FILE_NULL) return false;

        PROFILER_RECORD_TIME_END(TIMER_PFS_INPUT);
        TRACKER_RECORD_EVENT(EVENT_DISK_MPIOPEN);

        LOG_PRINT(DBG_IO, "Collective MPI open input file=%s\n",
                  this->state.seg_file->filename.c_str());

        return true;
    }

    virtual void file_read_at(char *buf, uint64_t offset, uint64_t size) {
        MPI_Status st;
        MPI_Request req;
        int flag = 0;

        LOG_PRINT(DBG_IO, "Collective MPI read input file=%s:%ld+%ld start\n", 
                  this->state.seg_file->filename.c_str(), offset, size);

        if (sfile_idx < MAX_GROUPS) {

            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
            MPI_Ibarrier(sfile_comms[sfile_idx], &req);
            flag = 0;
            while (!flag) {
                PROFILER_RECORD_TIME_START;
                MPI_Test(&req, &flag, &st);
                PROFILER_RECORD_TIME_END(TIMER_COMM_TEST);
                if (this->shuffler)
                    this->shuffler->make_progress();
            }
            TRACKER_RECORD_EVENT(EVENT_SYN_COMM);
        }

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
        PROFILER_RECORD_TIME_START;

        MPI_File_read_at_all(this->union_fp.mpi_fp, offset, buf,
                             (int)size, MPI_BYTE, &st);

        PROFILER_RECORD_TIME_END(TIMER_PFS_INPUT);
        TRACKER_RECORD_EVENT(EVENT_DISK_MPIREADATALL);


        LOG_PRINT(DBG_IO, "Collective MPI read input file=%s:%ld+%ld\n", 
                  this->state.seg_file->filename.c_str(), offset, size);
    }

    virtual void file_close() {
        MPI_Status st;
        MPI_Request req;
        int flag = 0;

        if (this->union_fp.mpi_fp != MPI_FILE_NULL) {

            LOG_PRINT(DBG_IO, "Collective MPI close input file=%s start\n", 
                      this->state.seg_file->filename.c_str());

            if (sfile_idx < MAX_GROUPS) {
                int remain_count = (int)ROUNDUP(this->state.seg_file->maxsegsize, this->bufsize) \
                                   - (int)ROUNDUP(this->state.seg_file->segsize, this->bufsize);
                for (int i = 0; i < remain_count; i++) {
                    TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
                    MPI_Ibarrier(sfile_comms[sfile_idx], &req);
                    flag = 0;
                    while (!flag) {
                        PROFILER_RECORD_TIME_START;
                        MPI_Test(&req, &flag, &st);
                        PROFILER_RECORD_TIME_END(TIMER_COMM_TEST);
                        if (this->shuffler)
                            this->shuffler->make_progress();
                    }
                    TRACKER_RECORD_EVENT(EVENT_SYN_COMM);
                    PROFILER_RECORD_TIME_START;

                    MPI_File_read_at_all(this->union_fp.mpi_fp, 0, NULL,
                             0, MPI_BYTE, &st);

                    PROFILER_RECORD_TIME_END(TIMER_PFS_INPUT);
                    TRACKER_RECORD_EVENT(EVENT_DISK_MPIREADATALL);
                }

                TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
                MPI_Ibarrier(sfile_comms[sfile_idx], &req);
                flag = 0;
                while (!flag) {
                    PROFILER_RECORD_TIME_START;
                    MPI_Test(&req, &flag, &st);
                    PROFILER_RECORD_TIME_END(TIMER_COMM_TEST);
                    if (this->shuffler)
                        this->shuffler->make_progress();
                }
                TRACKER_RECORD_EVENT(EVENT_SYN_COMM);

                PROFILER_RECORD_TIME_START;

                MPI_File_close(&(this->union_fp.mpi_fp));

                PROFILER_RECORD_TIME_END(TIMER_PFS_INPUT);
                TRACKER_RECORD_EVENT(EVENT_DISK_MPICLOSE);

                sfile_idx++;
            } else {

                TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
                PROFILER_RECORD_TIME_START;

                MPI_File_close(&(this->union_fp.mpi_fp));

                PROFILER_RECORD_TIME_END(TIMER_PFS_INPUT);
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

template<typename RecordFormat>
FileReader<RecordFormat>* FileReader<RecordFormat>::reader = NULL;

template<typename RecordFormat>
FileReader<RecordFormat>* FileReader<RecordFormat>
    ::getReader(InputSplit *input) {
    if (reader != NULL) delete reader;
    if (READER_TYPE == 0) {
        reader = new FileReader<RecordFormat>(input);
    } else if (READER_TYPE == 1) {
        reader = new MPIFileReader<RecordFormat>(input);
    } else {
        LOG_ERROR("Error reader type %d\n", READER_TYPE);
    }
    return reader;
}

}

#endif
