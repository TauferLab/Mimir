/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_FILE_WRITER_H
#define MIMIR_FILE_WRITER_H

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string>
#include <sstream>

#include "log.h"
#include "stat.h"
#include "config.h"
#include "interface.h"
#include "memory.h"
#include "globals.h"
#include "baseshuffler.h"
#include "serializer.h"

namespace MIMIR_NS {

enum OUTPUT_FORMAT {BINARY_FORMAT, TEXT_FORMAT};

template <typename KeyType, typename ValType>
class FileWriter : public Writable<KeyType, ValType> {
  public:
    static FileWriter *getWriter(MPI_Comm comm, const char *filename);
    static FileWriter *writer;

  public:
    FileWriter(MPI_Comm comm, const char *filename, bool singlefile = false, int keycount = 1, int valcount = 1) {
        this->writer_comm = comm;
        this->filename = filename;
        this->singlefile = singlefile;
        this->keycount = keycount;
        this->valcount = valcount;
        MPI_Comm_rank(writer_comm, &(this->writer_rank));
        MPI_Comm_size(writer_comm, &(this->writer_size));
        if (!singlefile) {
            std::ostringstream oss;
            oss << this->writer_size << "." << this->writer_rank;
            this->filename += oss.str();
        }
        shuffler = NULL;
        ser = new Serializer<KeyType, ValType>(keycount, valcount);
        output_format = BINARY_FORMAT;
    }

    virtual ~FileWriter() {
        delete ser;
    }

    std::string get_object_name() { return "FileWriter"; }
    std::string& get_file_name() { return filename; }

    void set_file_format(const char *format = "binary") {
        if (strcmp(format, "binary") == 0) {
            output_format = BINARY_FORMAT;
        } else if (strcmp(format, "text") == 0) {
            output_format = TEXT_FORMAT;
        } else {
            LOG_ERROR("Wrong output format (%s)!\n", format);
        }
    }

    void set_shuffler(BaseShuffler<KeyType,ValType> *shuffler) {
        this->shuffler = shuffler;
    }

    virtual bool is_single_file() {
        return singlefile;
    }

    virtual int open() {
        bufsize = INPUT_BUF_SIZE;
        this->datasize = 0;
        buffer =  (char*)mem_aligned_malloc(MEMPAGE_SIZE,  bufsize);
        record_count = 0;
        this->done_flag = 0;
        return file_open();
    }

    virtual void close() {
        this->done_flag = 1;
        if (this->datasize > 0) file_write();
        file_close();
        mem_aligned_free(buffer);
    }

    virtual int write(KeyType *key, ValType *val) {
        int kvsize = 0;
        if (output_format == BINARY_FORMAT) {
            //kvsize = this->ser->get_kv_bytes(key, val);
            //if (kvsize > bufsize) {
            //    LOG_ERROR("The write record length is larger than the buffer size!\n");
            //}
            kvsize = this->ser->kv_to_bytes(key, val, buffer + datasize, (int)(bufsize - datasize));
            if (kvsize == -1) {
                file_write();
                kvsize = this->ser->kv_to_bytes(key, val, buffer + datasize, (int)(bufsize - datasize));
                if (kvsize == -1)
                    LOG_ERROR("The write record length is larger than the buffer size!\n");
            }
        } else if (output_format == TEXT_FORMAT) {
            //kvsize = this->ser->get_kv_txt_len(key, val);
            //kvsize += 1; // add \n
            //if (kvsize > bufsize) {
            //    LOG_ERROR("The write record length is larger than the buffer size!\n");
            //}
            kvsize = this->ser->kv_to_txt(key, val, buffer + datasize, (int)(bufsize - datasize));
            if (kvsize == -1) {
                file_write();
                kvsize = this->ser->kv_to_txt(key, val, buffer + datasize, (int)(bufsize - datasize));
                if (kvsize == -1)
                    LOG_ERROR("The write record length is larger than the buffer size!\n");
            }
            //this->ser->kv_to_txt(key, val, buffer + datasize, bufsize - datasize);
            //*(buffer + datasize + kvsize - 1) = '\n';
        }
        datasize += kvsize;
        record_count++;
        return 1;
    }

    virtual uint64_t get_record_count() { return record_count; }

    virtual int file_open() {
        TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

        //std::ostringstream oss;
        //oss << mimir_world_size << "." << mimir_world_rank;
        //filename += oss.str();

        PROFILER_RECORD_TIME_START;

        this->union_fp.c_fp = fopen(this->filename.c_str(), "w+");
        if (!this->union_fp.c_fp) {
            LOG_ERROR("Open file %s error!\n", this->filename.c_str());
        }

        PROFILER_RECORD_TIME_END(TIMER_PFS_OUTPUT);

        TRACKER_RECORD_EVENT(EVENT_DISK_FOPEN);

        LOG_PRINT(DBG_IO, "Open output file %s.\n", this->filename.c_str());	

        return true;
    }

    virtual void file_write() {
        TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

        LOG_PRINT(DBG_IO, "Write output file %s:%d\n", 
                  this->filename.c_str(), (int)(this->datasize));

        PROFILER_RECORD_TIME_START;
        fwrite(this->buffer, this->datasize, 1, this->union_fp.c_fp);
        PROFILER_RECORD_TIME_END(TIMER_PFS_OUTPUT);

        this->datasize = 0;
        TRACKER_RECORD_EVENT(EVENT_DISK_FWRITE);
    }

    virtual void file_close() {
        if (this->union_fp.c_fp) {
            TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

            PROFILER_RECORD_TIME_START;
            fclose(this->union_fp.c_fp);
            PROFILER_RECORD_TIME_END(TIMER_PFS_OUTPUT);

            this->union_fp.c_fp = NULL;
            TRACKER_RECORD_EVENT(EVENT_DISK_FCLOSE);

            LOG_PRINT(DBG_IO, "Close output file %s.\n", this->filename.c_str());	
        }
    }

  protected:
    std::string filename;
    uint64_t record_count;

    BaseShuffler<KeyType,ValType> *shuffler;

    union FilePtr {
        FILE    *c_fp;
        MPI_File mpi_fp;
        int      posix_fd;
    } union_fp;

    char        *buffer;
    uint64_t    datasize;
    uint64_t    bufsize;
    bool        singlefile;
    int         done_flag;

    MPI_Comm    writer_comm;
    int         writer_rank;
    int         writer_size;

    int     keycount, valcount;

    Serializer<KeyType, ValType> *ser;

    OUTPUT_FORMAT  output_format;
};

template <typename KeyType, typename ValType>
class DirectFileWriter : public FileWriter<KeyType, ValType>
{
  public:
    DirectFileWriter(MPI_Comm comm, const char *filename) 
        : FileWriter<KeyType, ValType>(comm, filename, false) {
    }

    virtual int file_open() {
        TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

        PROFILER_RECORD_TIME_START;

        this->union_fp.posix_fd = ::open(this->filename.c_str(), O_CREAT | O_WRONLY | 
                                   O_DIRECT | O_LARGEFILE,
                                   S_IRUSR | S_IWUSR);
        if (this->union_fp.posix_fd == -1) {
            LOG_ERROR("Open file %s error %d!\n", this->filename.c_str(), errno);
        }

        filesize = 0;

        PROFILER_RECORD_TIME_END(TIMER_PFS_OUTPUT);

        TRACKER_RECORD_EVENT(EVENT_DISK_FOPEN);

        LOG_PRINT(DBG_IO, "Open (POSIX) output file %s.\n", this->filename.c_str());	

        return true;
    }

    virtual void file_write() {
        TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

        LOG_PRINT(DBG_IO, "Write (POSIX) output file %s:%d\n", 
                  this->filename.c_str(), (int)(this->datasize));

        PROFILER_RECORD_TIME_START;
        //::lseek64(union_fp.posix_fd, 0, SEEK_END);
        uint64_t total_bytes = 0;
        if (this->done_flag) {
            total_bytes = ROUNDUP((this->datasize), DISKPAGE_SIZE) * DISKPAGE_SIZE;
        } else {
            total_bytes = ROUNDDOWN((this->datasize), DISKPAGE_SIZE) * DISKPAGE_SIZE;
        }
        uint64_t remain_bytes = total_bytes;
        char *remain_buffer = this->buffer;
        do {
            ssize_t write_bytes = ::write(this->union_fp.posix_fd, remain_buffer, remain_bytes);
            if (write_bytes == -1) {
                LOG_ERROR("Write error, %d\n", errno);
            }
            remain_bytes -= write_bytes;
            remain_buffer += write_bytes;
        } while (remain_bytes > 0);
        PROFILER_RECORD_TIME_END(TIMER_PFS_OUTPUT);

        if (total_bytes < this->datasize) {
            filesize += total_bytes;
            this->datasize = this->datasize - total_bytes;
            for (size_t i = 0; i < this->datasize; i++) {
                this->buffer[i] = this->buffer[total_bytes + i];
            }
        } else if (total_bytes > this->datasize ) {
            filesize += this->datasize;
            this->datasize = 0;
            LOG_PRINT(DBG_IO, "Set (POSIX) output file %s:%ld\n", 
                      this->filename.c_str(), filesize);
            ::ftruncate64(this->union_fp.posix_fd, filesize);
        } else {
            filesize += this->datasize;
            this->datasize = 0;
        }
        TRACKER_RECORD_EVENT(EVENT_DISK_FWRITE);
    }

    virtual void file_close() {
        if (this->union_fp.posix_fd != -1) {
            TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

            PROFILER_RECORD_TIME_START;
            ::close(this->union_fp.posix_fd);
            PROFILER_RECORD_TIME_END(TIMER_PFS_OUTPUT);

            this->union_fp.posix_fd = -1;
            TRACKER_RECORD_EVENT(EVENT_DISK_FCLOSE);

            LOG_PRINT(DBG_IO, "Close (POSIX) output file %s.\n", this->filename.c_str());	
        }
    }

  private:
    off64_t     filesize;
};

template <typename KeyType, typename ValType>
class MPIFileWriter : public FileWriter<KeyType, ValType> {
  public:
    MPIFileWriter(MPI_Comm comm, const char *filename) : 
        FileWriter<KeyType, ValType>(comm, filename, true) {
    }

    virtual int file_open() {
        TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

        MPI_Request req;
        MPI_Status st;
        if (this->shuffler) {
            //PROFILER_RECORD_TIME_START;
            MPI_Ibarrier(this->writer_comm, &req);
            //PROFILER_RECORD_TIME_END(TIMER_COMM_IBARRIER);
            int flag = 0;
            while (!flag) {
                //PROFILER_RECORD_TIME_START;
                MPI_Test(&req, &flag, &st);
                //PROFILER_RECORD_TIME_END(TIMER_COMM_TEST);
                this->shuffler->make_progress();
            }
            TRACKER_RECORD_EVENT(EVENT_SYN_COMM);
        }

        LOG_PRINT(DBG_IO, "Collective open output file %s.\n", this->filename.c_str());	
        PROFILER_RECORD_TIME_START;
        MPI_CHECK(MPI_File_open(this->writer_comm, this->filename.c_str(), 
                                MPI_MODE_WRONLY | MPI_MODE_CREATE,
                                MPI_INFO_NULL, &(this->union_fp.mpi_fp)));
        if (this->union_fp.mpi_fp == MPI_FILE_NULL) {
            LOG_ERROR("Open file %s error!\n", this->filename.c_str());
        }
        PROFILER_RECORD_TIME_END(TIMER_PFS_OUTPUT);

        TRACKER_RECORD_EVENT(EVENT_DISK_MPIOPEN);

        done_count = 0;
        filesize = 0;
        return true;
    }

    virtual void file_write() {
        //MPI_Request done_req, req;
        MPI_Request req;
        MPI_Status st;
        MPI_Offset fileoff = 0;
        int sendcounts[this->writer_size];

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

        if (this->shuffler) {
            //PROFILER_RECORD_TIME_START;
            MPI_Ibarrier(this->writer_comm, &req);
            //PROFILER_RECORD_TIME_END(TIMER_COMM_IBARRIER);
            int flag = 0;
            while (!flag) {
                //PROFILER_RECORD_TIME_START;
                MPI_Test(&req, &flag, &st);
                //PROFILER_RECORD_TIME_END(TIMER_COMM_TEST);
                this->shuffler->make_progress();
            }
            TRACKER_RECORD_EVENT(EVENT_SYN_COMM);
        }

        //MPI_File_get_size(union_fp.mpi_fp, &filesize);
        PROFILER_RECORD_TIME_START;
        MPI_Allgather(&(this->datasize), 1, MPI_INT,
                      sendcounts, 1, MPI_INT, this->writer_comm);
        PROFILER_RECORD_TIME_END(TIMER_COMM_ALLGATHER);

        TRACKER_RECORD_EVENT(EVENT_COMM_ALLGATHER);

        fileoff = filesize;
        for (int i = 0; i < this->writer_rank; i++) fileoff += sendcounts[i];
        for (int i = 0; i < this->writer_size; i++) filesize += sendcounts[i];

        //if (mimir_world_rank == 0)
        //LOG_PRINT(DBG_IO, "Collective set output file %s:%lld\n", 
        //          filename.c_str(), filesize);
        //MPI_File_set_size(union_fp.mpi_fp, filesize);

        //if (this->shuffler) {
        //    MPI_Ibarrier(mimir_world_comm, &req);
        //    int flag = 0;
        //    while (!flag) {
        //        MPI_Test(&req, &flag, &st);
        //        this->shuffler->make_progress();
        //    }
        //    TRACKER_RECORD_EVENT(EVENT_SYN_COMM);
        //}

        LOG_PRINT(DBG_IO, "Collective write output file %s:%lld+%d\n", 
                  this->filename.c_str(), fileoff, (int)(this->datasize));

        PROFILER_RECORD_TIME_START;
        MPI_CHECK(MPI_File_write_at_all(this->union_fp.mpi_fp, fileoff, 
                                        this->buffer, (int)(this->datasize), MPI_BYTE, &st));
        this->datasize = 0;
        PROFILER_RECORD_TIME_END(TIMER_PFS_OUTPUT);
        TRACKER_RECORD_EVENT(EVENT_DISK_MPIWRITEATALL);

        //int flag = 0;
        //while (!flag) {
        //    PROFILER_RECORD_TIME_START;
        //    MPI_Test(&done_req, &flag, &st);
        //    PROFILER_RECORD_TIME_END(TIMER_COMM_TEST);
        //    if (this->shuffler) this->shuffler->make_progress();
        //}
        PROFILER_RECORD_TIME_START;
        MPI_Allreduce(&this->done_flag, &done_count, 1, MPI_INT, MPI_SUM, 
                       this->writer_comm);
        PROFILER_RECORD_TIME_END(TIMER_COMM_RDC);

        TRACKER_RECORD_EVENT(EVENT_SYN_COMM);
    }

    virtual void file_close() {
        if (this->union_fp.mpi_fp) {
            while (done_count < this->writer_size) {
                file_write();
            }

            TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

            MPI_Request req;
            MPI_Status st;
            if (this->shuffler) {
                //PROFILER_RECORD_TIME_START;
                MPI_Ibarrier(this->writer_comm, &req);
                //PROFILER_RECORD_TIME_END(TIMER_COMM_IBARRIER);
                int flag = 0;
                while (!flag) {
                    //PROFILER_RECORD_TIME_START;
                    MPI_Test(&req, &flag, &st);
                    //PROFILER_RECORD_TIME_END(TIMER_COMM_TEST);
                    this->shuffler->make_progress();
                }
                TRACKER_RECORD_EVENT(EVENT_SYN_COMM);
            }

            PROFILER_RECORD_TIME_START;
            MPI_CHECK(MPI_File_close(&(this->union_fp.mpi_fp)));
            this->union_fp.mpi_fp = MPI_FILE_NULL;
            PROFILER_RECORD_TIME_END(TIMER_PFS_OUTPUT);

            TRACKER_RECORD_EVENT(EVENT_DISK_MPICLOSE);

            LOG_PRINT(DBG_IO, "Collective close output file %s.\n", this->filename.c_str());	
        }
    }
  private:
    int done_count;
    MPI_Offset filesize;
};

template <typename KeyType, typename ValType>
FileWriter<KeyType, ValType>* FileWriter<KeyType, ValType>::writer = NULL;

template <typename KeyType, typename ValType>
FileWriter<KeyType, ValType>* FileWriter<KeyType, ValType>::getWriter(MPI_Comm comm, const char *filename) {
    //if (writer != NULL) delete writer;
    if (WRITER_TYPE == 0) {
        writer = new FileWriter<KeyType, ValType>(comm, filename);
    } else if (WRITER_TYPE == 1) {
        writer = new DirectFileWriter<KeyType, ValType>(comm, filename);
    } else if (WRITER_TYPE == 2) {
        writer = new MPIFileWriter<KeyType, ValType>(comm, filename); 
    } else {
        LOG_ERROR("Error writer type %d\n", WRITER_TYPE);
    }
    return writer;
}

}

#endif
