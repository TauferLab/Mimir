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
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include "log.h"
#include "stat.h"
#include "config.h"
#include "globals.h"
#include "memory.h"
#include "chunkmanager.h"
#include "interface.h"
#include "baseshuffler.h"
#include "baserecordformat.h"

namespace MIMIR_NS {

class InputSplit;

template <typename RecordFormat>
class MPIFileReader;

template<typename RecordFormat>
class FileReader : public Readable {
  public:
    static FileReader<RecordFormat> *getReader(ChunkManager *chunk_mgr,
                                               RepartitionCallback repartition_fn);
    static FileReader<RecordFormat> *reader;

  public:
    FileReader(ChunkManager *chunk_mgr, RepartitionCallback repartition_fn) {
        this->chunk_mgr = chunk_mgr;
        this->repartition_fn = repartition_fn;

        buffer = NULL;
        bufsize = 0;

        record = NULL;
        shuffler = NULL;

        record_count = 0;
    }

    virtual ~FileReader() {
    }

    std::string get_object_name() { return "FileReader"; }

    void set_shuffler(BaseShuffler *shuffler) {
        this->shuffler = shuffler;
        chunk_mgr->set_shuffler(shuffler);
    }

    virtual bool open() {

        LOG_PRINT(DBG_IO, "Filereader open.\n");

        //if (input->get_max_fsize() <= (uint64_t)INPUT_BUF_SIZE)
        //    bufsize = ROUNDUP(input->get_max_fsize(), DISKPAGE_SIZE) * DISKPAGE_SIZE;
        //else
        //    bufsize = ROUNDUP(INPUT_BUF_SIZE, DISKPAGE_SIZE) * DISKPAGE_SIZE;
        //
        bufsize = INPUT_BUF_SIZE;
        if (bufsize % DISKPAGE_SIZE != 0)
            LOG_ERROR("The chunck size should be multiple times of disk sector size!\n");

        PROFILER_RECORD_COUNT(COUNTER_MAX_FILE, (uint64_t) bufsize, OPMAX);

        buffer =  (char*)mem_aligned_malloc(MEMPAGE_SIZE, bufsize + MAX_RECORD_SIZE + 1);

        state.cur_chunk.fileseg = NULL;
        state.start_pos = 0;
        state.win_size = 0;
        state.has_tail = false;

        record = new RecordFormat();

        file_init();
        read_next_chunk();

        record_count = 0;
        return true;
    }

    virtual void close() {
        file_close();
        file_uninit();

        delete record;

        mem_aligned_free(buffer);

        LOG_PRINT(DBG_IO, "Filereader close.\n");
    }

    virtual uint64_t get_record_count() { return record_count; }

    virtual RecordFormat* read() {

        if (state.cur_chunk.fileseg == NULL)
            return NULL;

        bool is_empty = false;
        while(!is_empty) {

            char *ptr = buffer + state.start_pos; 
            int skip_count = record->get_skip_size(ptr, state.win_size);
            state.start_pos += skip_count;
            state.win_size -= skip_count;

            ptr = buffer + state.start_pos;
            record->set_buffer(ptr);
            bool islast = is_last_block();
            if(state.win_size > 0
               && record->get_next_record_size(ptr, state.win_size, islast) != -1) {
                int move_count = record->get_record_size();
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
            else {
                // read next chunk
                if(!read_next_chunk()) {
                    is_empty = true;
                }
            }
        };

        chunk_mgr->wait();
        return NULL;
    }

  protected:

    bool is_last_block() {
        if (state.cur_chunk.fileoff + INPUT_BUF_SIZE >= state.cur_chunk.fileseg->filesize
            || !state.has_tail)
            return true;
        return false;
    }

    bool read_next_chunk() {

        chunk_mgr->make_progress();

        //print_state();

        bool cont_chunk = false;
        Chunk new_chunk;
        if (state.cur_chunk.fileseg && chunk_mgr->has_tail(state.cur_chunk) && !is_last_block()) {
            uint64_t aligned_size = ROUNDUP(state.win_size, MEMPAGE_SIZE) * MEMPAGE_SIZE;
            uint64_t new_start_pos = aligned_size - state.win_size;
            for (uint64_t i = 0; i < state.win_size; i++)
                buffer[i + new_start_pos] = buffer[state.start_pos + i];
            state.start_pos = new_start_pos;
            if (chunk_mgr->acquire_local_chunk(new_chunk, state.cur_chunk.localid + 1) == false) {
                //printf("%d[%d] acquire local chunk=%ld fail!\n",
                //       mimir_world_rank, mimir_world_size, state.cur_chunk.localid + 1);
                int count = chunk_mgr->recv_tail(state.cur_chunk,
                                                buffer + state.start_pos + state.win_size,
                                                MAX_RECORD_SIZE);
                state.win_size += count;
                state.has_tail = false;
                return true;
            } else {
                cont_chunk = true;
            }

        } else {
            state.start_pos = 0;
            state.win_size = 0;
            if (chunk_mgr->acquire_chunk(new_chunk) == false) {
                return false;
            }
        }

        if (!state.cur_chunk.fileseg
            || new_chunk.fileseg->filename != state.cur_chunk.fileseg->filename) {
            file_close();
            if (!file_open(new_chunk.fileseg->filename.c_str())) {
                LOG_ERROR("Open file %s error!\n", new_chunk.fileseg->filename.c_str());
                return false;
            }
            state.start_pos = 0;
            state.win_size = 0;
            cont_chunk = false;
            PROFILER_RECORD_COUNT(COUNTER_FILE_COUNT, 1, OPSUM);
        }

        state.cur_chunk = new_chunk;
        file_read_at(buffer + state.start_pos + state.win_size,
                     state.cur_chunk.fileoff, state.cur_chunk.chunksize);
        state.win_size += state.cur_chunk.chunksize;
        PROFILER_RECORD_COUNT(COUNTER_FILE_SIZE, new_chunk.chunksize, OPSUM);

        if (chunk_mgr->has_head(state.cur_chunk) && cont_chunk == false) {
            int count = repartition_fn(buffer + state.start_pos,
                                       state.win_size,
                                       chunk_mgr->is_file_end(state.cur_chunk));
            chunk_mgr->send_head(state.cur_chunk, buffer, count);
            state.start_pos += count;
            state.win_size -= count;
        }

        if (!chunk_mgr->is_file_end(state.cur_chunk)) {
            state.has_tail = true;
        } else {
            state.has_tail = false;
        }

        //print_state();

        return true;
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

        LOG_PRINT(DBG_IO, "Open input file=%s\n", filename);

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
                  state.cur_chunk.fileseg->filename.c_str(), offset, size);
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
                      state.cur_chunk.fileseg->filename.c_str());
        }
    }

    union FilePtr{
        FILE    *c_fp;
        MPI_File mpi_fp;
        int      posix_fd;
    } union_fp;

    struct FileState{
        Chunk     cur_chunk;
        uint64_t  start_pos;
        uint64_t  win_size;
        bool      has_tail;
    }state;

    void print_state(){
        if (state.cur_chunk.fileseg != NULL) {
            printf("%d[%d] file_name=%s:%ld+%ld (%ld<%d,%ld>), start_pos=%ld, win_size=%ld, has_tail=%d\n",
               mimir_world_rank, mimir_world_size,
               state.cur_chunk.fileseg->filename.c_str(),
               state.cur_chunk.fileoff,
               state.cur_chunk.chunksize,
               state.cur_chunk.globalid,
               state.cur_chunk.procrank,
               state.cur_chunk.localid,
               state.start_pos,
               state.win_size,
               state.has_tail);
        }
    }

    char*           buffer;
    int             bufsize;
    ChunkManager*   chunk_mgr;
    RecordFormat*   record;
    BaseShuffler*   shuffler;
    uint64_t        record_count;
    RepartitionCallback repartition_fn;
};

template <typename RecordFormat>
class DirectFileReader : public FileReader< RecordFormat >{

  public:
    DirectFileReader(ChunkManager *chunk_mgr, RepartitionCallback repartition_cb) 
        : FileReader<RecordFormat>(chunk_mgr, repartition_cb) {
    }

    ~DirectFileReader(){
    }

  protected:

    virtual void file_init(){
        this->union_fp.posix_fd = -1;
    }

    virtual void file_uninit(){
    }

    virtual bool file_open(const char *filename){

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
        PROFILER_RECORD_TIME_START;

        this->union_fp.posix_fd = ::open(filename, O_RDONLY | O_DIRECT | O_LARGEFILE);
        if (this->union_fp.posix_fd == -1)
            return false;

        PROFILER_RECORD_TIME_END(TIMER_PFS_INPUT);
        TRACKER_RECORD_EVENT(EVENT_DISK_FOPEN);

        LOG_PRINT(DBG_IO, "Open (POSIX) input file=%s\n", filename);

        return true;
    }

    virtual void file_read_at(char *buf, uint64_t offset, uint64_t size){
        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

        if ((uint64_t)buf % MEMPAGE_SIZE !=0)
            LOG_ERROR("Buffer (%p) should be page alignment!\n", buf);

        if (offset % DISKPAGE_SIZE != 0)
            LOG_ERROR("Read offset (%ld) should be sector alignment!\n", offset);

        size_t remain_bytes = size;
        while (remain_bytes > 0) {
            ssize_t read_bytes = 0;
            if (read_bytes % DISKPAGE_SIZE != 0)
                LOG_ERROR("Read bytes (%ld) should be sector alignment!\n", read_bytes);
            if ((uint64_t)buf % MEMPAGE_SIZE !=0)
                LOG_ERROR("Buffer (%p) should be page alignment!\n", buf);
            ::lseek64(this->union_fp.posix_fd, (off64_t)offset, SEEK_SET);
            size_t param_bytes = ROUNDUP(remain_bytes, DISKPAGE_SIZE) * DISKPAGE_SIZE;
            PROFILER_RECORD_TIME_START;
            read_bytes = ::read(this->union_fp.posix_fd, buf, param_bytes);
            PROFILER_RECORD_TIME_END(TIMER_PFS_INPUT);
            TRACKER_RECORD_EVENT(EVENT_DISK_FREADAT);
            if (read_bytes < (ssize_t)remain_bytes)
                read_bytes = read_bytes / DISKPAGE_SIZE * DISKPAGE_SIZE;
            this->chunk_mgr->make_progress();
            if (this->shuffler) this->shuffler->make_progress();
            LOG_PRINT(DBG_IO, "Read (POSIX) input file=%s:%ld+%ld\n", 
                      this->state.cur_chunk.fileseg->filename.c_str(), offset, read_bytes);
            remain_bytes -= read_bytes;
            buf += read_bytes;
            offset += read_bytes;
        }

   }

    virtual void file_close(){
        if (this->union_fp.posix_fd != -1) {

            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
            PROFILER_RECORD_TIME_START;

            ::close(this->union_fp.posix_fd);

            PROFILER_RECORD_TIME_END(TIMER_PFS_INPUT);
            TRACKER_RECORD_EVENT(EVENT_DISK_FCLOSE);

            this->union_fp.posix_fd = -1;

            LOG_PRINT(DBG_IO, "Close (POSIX) input file=%s\n", 
                      this->state.cur_chunk.fileseg->filename.c_str());
        }
    }

};

template <typename RecordFormat>
class MPIFileReader : public FileReader< RecordFormat >{

  public:
    MPIFileReader(ChunkManager *chunk_mgr, RepartitionCallback repartition_cb) 
        : FileReader<RecordFormat>(chunk_mgr, repartition_cb) {
    }

    ~MPIFileReader(){
    }

  protected:

    virtual void file_init() {
        this->union_fp.mpi_fp = MPI_FILE_NULL;
    }

    virtual void file_uninit() {
    }

    virtual bool file_open(const char *filename){

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
        PROFILER_RECORD_TIME_START;

        MPI_Info file_info;
        MPI_Info_create(&file_info);
        MPI_Info_set(file_info, "direct_read", "true");
        MPI_File_open(MPI_COMM_SELF, filename, MPI_MODE_RDONLY,
                      file_info, &(this->union_fp.mpi_fp));
        MPI_Info_free(&file_info);
        if (this->union_fp.mpi_fp == MPI_FILE_NULL)
            return false;

        PROFILER_RECORD_TIME_END(TIMER_PFS_INPUT);
        TRACKER_RECORD_EVENT(EVENT_DISK_FOPEN);

        LOG_PRINT(DBG_IO, "Open (MPI) input file=%s\n", filename);

        return true;
    }

    virtual void file_read_at(char *buf, uint64_t offset, uint64_t size){
        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

        MPI_Request req;
        PROFILER_RECORD_TIME_START;
        MPI_File_iread_at(this->union_fp.mpi_fp, offset, 
                          buf, (int)size, MPI_BYTE, &req);
        PROFILER_RECORD_TIME_END(TIMER_PFS_INPUT);
        TRACKER_RECORD_EVENT(EVENT_DISK_FREADAT);

        int flag = 0;
        MPI_Status st;
        int read_count = 0;
        while (!flag) {
            MPI_Test(&req, &flag, &st);
            this->chunk_mgr->make_progress();
            if (this->shuffler) this->shuffler->make_progress();
            if (flag) {
                MPI_Get_count(&st, MPI_BYTE, &read_count);
                if (read_count != (int)size)
                    LOG_ERROR("Read file error!\n");
            }
        }

        LOG_PRINT(DBG_IO, "Read (MPI) input file=%s:%ld+%ld\n", 
                  this->state.cur_chunk.fileseg->filename.c_str(), offset, size);

   }

    virtual void file_close(){
        if (this->union_fp.mpi_fp != MPI_FILE_NULL) {

            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
            PROFILER_RECORD_TIME_START;

            MPI_File_close(&(this->union_fp.mpi_fp));

            PROFILER_RECORD_TIME_END(TIMER_PFS_INPUT);
            TRACKER_RECORD_EVENT(EVENT_DISK_FCLOSE);

            this->union_fp.mpi_fp = MPI_FILE_NULL;

            LOG_PRINT(DBG_IO, "Close (MPI) input file=%s\n", 
                      this->state.cur_chunk.fileseg->filename.c_str());
        }
    }

};

#if 0
template <typename RecordFormat>
class MPIFileReader : public FileReader< RecordFormat >{
public:
    MPIFileReader(InputSplit *input, RepartitionCallback repartition_cb) 
        : FileReader<RecordFormat>(input, repartition_cb) {
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
#endif

template<typename RecordFormat>
FileReader<RecordFormat>* FileReader<RecordFormat>::reader = NULL;

template<typename RecordFormat>
FileReader<RecordFormat>* FileReader<RecordFormat>
    ::getReader(ChunkManager *mgr, RepartitionCallback repartition_fn) {
    if (reader != NULL) delete reader;
    if (READER_TYPE == 0) {
        reader = new FileReader<RecordFormat>(mgr, repartition_fn);
    } else if (READER_TYPE == 1) {
        reader = new DirectFileReader<RecordFormat>(mgr, repartition_fn);
    } else if (READER_TYPE == 2) {
        reader = new MPIFileReader<RecordFormat>(mgr, repartition_fn);
    } else {
        LOG_ERROR("Error reader type %d\n", READER_TYPE);
    }
    return reader;
}

}

#endif
