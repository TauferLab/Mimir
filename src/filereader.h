//
// (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego
//     Supercomputer Center, National University of Defense Technology,
//     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
//
//     See COPYRIGHT in top-level directory.
//

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
//#include "dataformat.h"
#include "fileparser.h"

#ifndef HAVE_LSEEK64
#define lseek64 lseek
#define off64_t off_t
#endif

namespace MIMIR_NS {

class InputSplit;

enum InputFileFormat { TextFileFormat };

template <InputFileFormat FileFormat, typename KeyType, typename ValType,
          typename InKeyType = char *, typename InValType = void>
class FileReader : public Readable<InKeyType, InValType>
{
  public:
    static FileReader<FileFormat, KeyType, ValType, InKeyType, InValType>
        *getReader(MPI_Comm comm, ChunkManager<KeyType, ValType> *chunk_mgr,
                   int (*padding_fn)(const char *buf, int buflen, bool islast),
                   int keycount = 1, int valcount = 1, int inkeycount = 1,
                   int invalcount = 1);
    static FileReader<FileFormat, KeyType, ValType, InKeyType, InValType>
        *reader;

  public:
    FileReader(MPI_Comm comm, ChunkManager<KeyType, ValType> *chunk_mgr,
               int (*padding_fn)(const char *buf, int buflen, bool islast),
               int keycount = 1, int valcount = 1, int inkeycount = 1,
               int invalcount = 1)
    {
        this->reader_comm = comm;
        this->chunk_mgr = chunk_mgr;
        this->padding_fn = padding_fn;
        this->keycount = keycount;
        this->valcount = valcount;

        MPI_Comm_rank(reader_comm, &reader_rank);
        MPI_Comm_size(reader_comm, &reader_size);

        buffer = NULL;
        bufsize = 0;

        shuffler = NULL;

        record_count = 0;

        ser = new Serializer<InKeyType, InValType>(inkeycount, invalcount);
    }

    virtual ~FileReader() { delete ser; }

    std::string get_object_name() { return "FileReader"; }

    void set_shuffler(BaseShuffler<KeyType, ValType> *shuffler)
    {
        this->shuffler = shuffler;
        chunk_mgr->set_shuffler(shuffler);
    }

    virtual int open()
    {
        LOG_PRINT(DBG_IO, "Filereader open.\n");

        //if (input->get_max_fsize() <= (uint64_t)INPUT_BUF_SIZE)
        //    bufsize = ROUNDUP(input->get_max_fsize(), DISKPAGE_SIZE) * DISKPAGE_SIZE;
        //else
        //    bufsize = ROUNDUP(INPUT_BUF_SIZE, DISKPAGE_SIZE) * DISKPAGE_SIZE;
        //
        bufsize = (int) INPUT_BUF_SIZE;
        if (bufsize % DISKPAGE_SIZE != 0)
            LOG_ERROR(
                "The chunck size should be multiple times of disk sector "
                "size!\n");

        PROFILER_RECORD_COUNT(COUNTER_MAX_FILE, (uint64_t) bufsize, OPMAX);

        buffer = (char *) mem_aligned_malloc(
            MEMPAGE_SIZE, bufsize + MAX_RECORD_SIZE + 1, MCDRAM_ALLOCATE);

        state.cur_chunk.fileseg = NULL;
        state.start_pos = 0;
        state.win_size = 0;
        state.has_tail = false;

        file_init();
        read_next_chunk();

        record_count = 0;
        return true;
    }

    virtual void close()
    {
        file_close();
        file_uninit();

        mem_aligned_free(buffer);

        LOG_PRINT(DBG_IO, "Filereader close.\n");
    }

    virtual int seek(DB_POS pos)
    {
        LOG_WARNING("FileReader doesnot support seek methods!\n");
        return false;
    }

    virtual uint64_t get_record_count() { return record_count; }

    virtual int read(InKeyType *key, InValType *val)
    {
        if (state.cur_chunk.fileseg == NULL) return false;

        bool is_empty = false;
        while (!is_empty) {
            char *ptr = buffer + state.start_pos;
            //int skip_count = record->get_skip_size(ptr, state.win_size);
            //state.start_pos += skip_count;
            //state.win_size -= skip_count;

            //ptr = buffer + state.start_pos;
            //record->set_buffer(ptr);
            bool islast = is_last_block();
            if (state.win_size > 0
                && parser.to_line(ptr, (int) state.win_size, islast) != -1) {
                //&& record->get_next_record_size(ptr, state.win_size, islast) != -1) {
                //int move_count = record->get_record_size();
                //*key = (InKeyType)ptr;
                int move_count
                    = ser->key_from_bytes(key, ptr, (int) state.win_size);
                //int move_count = strlen((const char*)(*key)) + 1;
                //int move_count = ser->get_key_bytes(key);
                if ((uint64_t) move_count >= state.win_size) {
                    state.win_size = 0;
                    state.start_pos = 0;
                }
                else {
                    state.start_pos += move_count;
                    state.win_size -= move_count;
                }
                record_count++;
                //return record;
                return true;
            }
            else {
                // read next chunk
                if (!read_next_chunk()) {
                    is_empty = true;
                }
            }
        };

        chunk_mgr->wait();
        return false;
    }

  protected:
    bool is_last_block()
    {
        if (state.cur_chunk.fileoff + INPUT_BUF_SIZE
                >= state.cur_chunk.fileseg->filesize
            || !state.has_tail)
            return true;
        return false;
    }

    bool read_next_chunk()
    {
        chunk_mgr->make_progress();
        if (MAKE_PROGRESS && this->shuffler && state.cur_chunk.fileseg)
            this->shuffler->make_progress(true);

        //print_state();

        bool cont_chunk = false;
        Chunk new_chunk;
        if (state.cur_chunk.fileseg && chunk_mgr->has_tail(state.cur_chunk)
            && !is_last_block()) {
            uint64_t aligned_size
                = ROUNDUP(state.win_size, MEMPAGE_SIZE) * MEMPAGE_SIZE;
            uint64_t new_start_pos = aligned_size - state.win_size;
            for (uint64_t i = 0; i < state.win_size; i++)
                buffer[i + new_start_pos] = buffer[state.start_pos + i];
            state.start_pos = new_start_pos;
            if (chunk_mgr->acquire_local_chunk(new_chunk,
                                               state.cur_chunk.localid + 1)
                == false) {
                //printf("%d[%d] acquire local chunk=%ld fail!\n",
                //       mimir_world_rank, mimir_world_size, state.cur_chunk.localid + 1);
                int count = chunk_mgr->recv_tail(
                    state.cur_chunk, buffer + state.start_pos + state.win_size,
                    MAX_RECORD_SIZE);
                state.win_size += count;
                state.has_tail = false;
                return true;
            }
            else {
                cont_chunk = true;
            }
        }
        else {
            state.start_pos = 0;
            state.win_size = 0;
            if (chunk_mgr->acquire_chunk(new_chunk) == false) {
                return false;
            }
        }

        if (!state.cur_chunk.fileseg
            || new_chunk.fileseg->filename
                   != state.cur_chunk.fileseg->filename) {
            file_close();
            if (!file_open(new_chunk.fileseg->filename.c_str())) {
                LOG_ERROR("Open file %s error!\n",
                          new_chunk.fileseg->filename.c_str());
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
            int count
                = padding_fn(buffer + state.start_pos, (int) state.win_size,
                             chunk_mgr->is_file_end(state.cur_chunk));
            chunk_mgr->send_head(state.cur_chunk, buffer, count);
            state.start_pos += count;
            state.win_size -= count;
        }

        if (!chunk_mgr->is_file_end(state.cur_chunk)) {
            state.has_tail = true;
        }
        else {
            state.has_tail = false;
        }

        //print_state();

        return true;
    }

    virtual void file_init() { union_fp.c_fp = NULL; }

    virtual void file_uninit() {}

    virtual bool file_open(const char *filename)
    {
        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
        PROFILER_RECORD_TIME_START;

        union_fp.c_fp = fopen(filename, "r");
        if (union_fp.c_fp == NULL) return false;

        PROFILER_RECORD_TIME_END(TIMER_PFS_INPUT);
        TRACKER_RECORD_EVENT(EVENT_DISK_FOPEN);

        LOG_PRINT(DBG_IO, "Open input file=%s\n", filename);

        return true;
    }

    virtual void file_read_at(char *buf, uint64_t offset, uint64_t size)
    {
        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
        PROFILER_RECORD_TIME_START;

        fseek(union_fp.c_fp, offset, SEEK_SET);
        size = fread(buf, 1, size, union_fp.c_fp);

        PROFILER_RECORD_TIME_END(TIMER_PFS_INPUT);
        TRACKER_RECORD_EVENT(EVENT_DISK_FREADAT);

        LOG_PRINT(DBG_IO, "Read input file=%s:%ld+%ld\n",
                  state.cur_chunk.fileseg->filename.c_str(), offset, size);
    }

    virtual void file_close()
    {
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

    union FilePtr {
        FILE *c_fp;
        MPI_File mpi_fp;
        int posix_fd;
    } union_fp;

    struct FileState
    {
        Chunk cur_chunk;
        uint64_t start_pos;
        uint64_t win_size;
        bool has_tail;
    } state;

    void print_state()
    {
        if (state.cur_chunk.fileseg != NULL) {
            printf(
                "%d[%d] file_name=%s:%ld+%ld (%ld<%d,%ld>), start_pos=%ld, "
                "win_size=%ld, has_tail=%d\n",
                reader_rank, reader_size,
                state.cur_chunk.fileseg->filename.c_str(),
                state.cur_chunk.fileoff, state.cur_chunk.chunksize,
                state.cur_chunk.globalid, state.cur_chunk.procrank,
                state.cur_chunk.localid, state.start_pos, state.win_size,
                state.has_tail);
        }
    }

    char *buffer;
    int bufsize;
    ChunkManager<KeyType, ValType> *chunk_mgr;
    BaseShuffler<KeyType, ValType> *shuffler;
    FileParser parser;
    uint64_t record_count;
    int (*padding_fn)(const char *buf, int buflen, bool islast);

    Serializer<InKeyType, InValType> *ser;
    int keycount, valcount;

    MPI_Comm reader_comm;
    int reader_rank;
    int reader_size;
};

template <InputFileFormat FileFormat, typename KeyType, typename ValType,
          typename InKeyType = char *, typename InValType = void>
class DirectFileReader
    : public FileReader<FileFormat, KeyType, ValType, InKeyType, InValType>
{
  public:
    DirectFileReader(MPI_Comm comm, ChunkManager<KeyType, ValType> *chunk_mgr,
                     int (*padding_fn)(const char *buf, int buflen,
                                       bool islast),
                     int keycount = 1, int valcount = 1, int inkeycount = 1,
                     int invalcount = 1)
        : FileReader<FileFormat, KeyType, ValType, InKeyType, InValType>(
              comm, chunk_mgr, padding_fn, keycount, valcount, inkeycount,
              invalcount)
    {
    }

    ~DirectFileReader() {}

  protected:
    virtual void file_init() { this->union_fp.posix_fd = -1; }

    virtual void file_uninit() {}

    virtual bool file_open(const char *filename)
    {
        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
        PROFILER_RECORD_TIME_START;

#ifdef O_DIRECT
        this->union_fp.posix_fd
            = ::open(filename, O_RDONLY | O_DIRECT | O_LARGEFILE);
        if (this->union_fp.posix_fd == -1) return false;
#else
#ifdef F_NOCACHE
        this->union_fp.posix_fd
            = ::open(filename, O_RDONLY);
        ::fcntl(this->union_fp.posix_fd, F_NOCACHE, 1);
        if (this->union_fp.posix_fd == -1) return false;
#else
#warning "No direct IO support"
        this->union_fp.posix_fd
            = ::open(filename, O_RDONLY | O_LARGEFILE);
        if (this->union_fp.posix_fd == -1) return false;
#endif
#endif

        PROFILER_RECORD_TIME_END(TIMER_PFS_INPUT);
        TRACKER_RECORD_EVENT(EVENT_DISK_FOPEN);

        LOG_PRINT(DBG_IO, "Open (POSIX) input file=%s\n", filename);

        return true;
    }

    virtual void file_read_at(char *buf, uint64_t offset, uint64_t size)
    {
        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

        if ((uint64_t) buf % MEMPAGE_SIZE != 0)
            LOG_ERROR("Buffer (%p) should be page alignment!\n", buf);

        if (offset % DISKPAGE_SIZE != 0)
            LOG_ERROR("Read offset (%ld) should be sector alignment!\n",
                      offset);

        size_t remain_bytes = size;
        while (remain_bytes > 0) {
            ssize_t read_bytes = 0;
            if (read_bytes % DISKPAGE_SIZE != 0)
                LOG_ERROR("Read bytes (%ld) should be sector alignment!\n",
                          read_bytes);
            if ((uint64_t) buf % MEMPAGE_SIZE != 0)
                LOG_ERROR("Buffer (%p) should be page alignment!\n", buf);
            ::lseek64(this->union_fp.posix_fd, (off64_t) offset, SEEK_SET);
            size_t param_bytes
                = ROUNDUP(remain_bytes, DISKPAGE_SIZE) * DISKPAGE_SIZE;
            PROFILER_RECORD_TIME_START;
            read_bytes = ::read(this->union_fp.posix_fd, buf, param_bytes);
            PROFILER_RECORD_TIME_END(TIMER_PFS_INPUT);
            TRACKER_RECORD_EVENT(EVENT_DISK_FREADAT);
            if (read_bytes < (ssize_t) remain_bytes)
                read_bytes = read_bytes / DISKPAGE_SIZE * DISKPAGE_SIZE;
            //this->chunk_mgr->make_progress();
            //if (this->shuffler) this->shuffler->make_progress();
            LOG_PRINT(DBG_IO, "Read (POSIX) input file=%s:%ld+%ld\n",
                      this->state.cur_chunk.fileseg->filename.c_str(), offset,
                      read_bytes);
            remain_bytes -= read_bytes;
            buf += read_bytes;
            offset += read_bytes;
        }
    }

    virtual void file_close()
    {
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

template <InputFileFormat FileFormat, typename KeyType, typename ValType,
          typename InKeyType = char *, typename InValType = void>
class MPIFileReader
    : public FileReader<FileFormat, KeyType, ValType, InKeyType, InValType>
{
  public:
    MPIFileReader(MPI_Comm comm, ChunkManager<KeyType, ValType> *chunk_mgr,
                  int (*padding_fn)(const char *buf, int buflen, bool islast),
                  int keycount = 1, int valcount = 1, int inkeycount = 1,
                  int invalcount = 1)
        : FileReader<FileFormat, KeyType, ValType, InKeyType, InValType>(
              comm, chunk_mgr, padding_fn, keycount, valcount, inkeycount,
              invalcount)
    {
    }

    ~MPIFileReader() {}

  protected:
    virtual void file_init() { this->union_fp.mpi_fp = MPI_FILE_NULL; }

    virtual void file_uninit() {}

    virtual bool file_open(const char *filename)
    {
        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
        PROFILER_RECORD_TIME_START;

        MPI_Info file_info;
        MPI_Info_create(&file_info);
        if (DIRECT_READ) MPI_Info_set(file_info, "direct_read", "true");
        MPI_CHECK(MPI_File_open(MPI_COMM_SELF, filename, MPI_MODE_RDONLY,
                                file_info, &(this->union_fp.mpi_fp)));
        MPI_Info_free(&file_info);
        if (this->union_fp.mpi_fp == MPI_FILE_NULL) return false;

        PROFILER_RECORD_TIME_END(TIMER_PFS_INPUT);
        TRACKER_RECORD_EVENT(EVENT_DISK_FOPEN);

        LOG_PRINT(DBG_IO, "Open (MPI) input file=%s\n", filename);

        return true;
    }

    virtual void file_read_at(char *buf, uint64_t offset, uint64_t size)
    {
        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

        int read_count = 0;

        PROFILER_RECORD_TIME_START;
        while (read_count < (int) size) {
            MPI_Request req;
            MPI_CHECK(MPI_File_iread_at(
                this->union_fp.mpi_fp, offset + read_count, buf + read_count,
                (int) size - read_count, MPI_BYTE, &req));
            int flag = 0;
            MPI_Status st;
            while (!flag) {
                for (int i = 0; i < 100; i++) {
                    MPI_Test(&req, &flag, &st);
                    if (flag) break;
                }
                if (flag) break;
                this->chunk_mgr->make_progress();
                if (MAKE_PROGRESS && this->shuffler)
                    this->shuffler->make_progress(true);
            }
            int count = 0;
            MPI_Get_count(&st, MPI_BYTE, &count);
            read_count += count;
            LOG_PRINT(DBG_IO, "Read (MPI) input file=%s:%ld+%d\n",
                      this->state.cur_chunk.fileseg->filename.c_str(), offset,
                      count);
        }
        PROFILER_RECORD_TIME_END(TIMER_PFS_INPUT);
        TRACKER_RECORD_EVENT(EVENT_DISK_FREADAT);
    }

    virtual void file_close()
    {
        if (this->union_fp.mpi_fp != MPI_FILE_NULL) {
            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
            PROFILER_RECORD_TIME_START;

            MPI_CHECK(MPI_File_close(&(this->union_fp.mpi_fp)));

            PROFILER_RECORD_TIME_END(TIMER_PFS_INPUT);
            TRACKER_RECORD_EVENT(EVENT_DISK_FCLOSE);

            this->union_fp.mpi_fp = MPI_FILE_NULL;

            LOG_PRINT(DBG_IO, "Close (MPI) input file=%s\n",
                      this->state.cur_chunk.fileseg->filename.c_str());
        }
    }
};

template <InputFileFormat FileFormat, typename KeyType, typename ValType,
          typename InKeyType, typename InValType>
FileReader<FileFormat, KeyType, ValType, InKeyType, InValType>
    *FileReader<FileFormat, KeyType, ValType, InKeyType, InValType>::reader
    = NULL;

template <InputFileFormat FileFormat, typename KeyType, typename ValType,
          typename InKeyType, typename InValType>
FileReader<FileFormat, KeyType, ValType, InKeyType, InValType>
    *FileReader<FileFormat, KeyType, ValType, InKeyType, InValType>::getReader(
        MPI_Comm comm, ChunkManager<KeyType, ValType> *mgr,
        int (*padding_fn)(const char *buf, int buflen, bool islast),
        int keycount, int valcount, int inkeycount, int invalcount)
{
    if (READ_TYPE == 0) {
        if (DIRECT_READ) {
            reader = new DirectFileReader<FileFormat, KeyType, ValType,
                                          InKeyType, InValType>(
                comm, mgr, padding_fn, keycount, valcount, inkeycount,
                invalcount);
        }
        else {
            reader
                = new FileReader<FileFormat, KeyType, ValType, InKeyType,
                                 InValType>(comm, mgr, padding_fn, keycount,
                                            valcount, inkeycount, invalcount);
        }
    }
    else if (READ_TYPE == 1) {
        reader = new MPIFileReader<FileFormat, KeyType, ValType, InKeyType,
                                   InValType>(comm, mgr, padding_fn, keycount,
                                              valcount, inkeycount, invalcount);
    }
    else {
        LOG_ERROR("Error reader type %d\n", READ_TYPE);
    }
    return reader;
}

} // namespace MIMIR_NS

#endif
