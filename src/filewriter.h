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

namespace MIMIR_NS {

//class MPIFileWriter;

class FileWriter : public Writable {
  public:
    static FileWriter *getWriter(const char *filename);
    static FileWriter *writer;

  public:
    FileWriter(const char *filename, bool singlefile = false) {
        this->filename = filename;
        this->singlefile = singlefile;
        if (!singlefile) {
            std::ostringstream oss;
            oss << mimir_world_size << "." << mimir_world_rank;
            this->filename += oss.str();
        }
        shuffler = NULL;
    }

    std::string get_object_name() { return "FileWriter"; }
    std::string& get_file_name() { return filename; }

    void set_shuffler(BaseShuffler *shuffler) {
        this->shuffler = shuffler;
    }

    virtual bool is_single_file() {
        return singlefile;
    }

    virtual bool open() {
        bufsize = INPUT_BUF_SIZE;
        datasize = 0;
        buffer =  (char*)mem_aligned_malloc(MEMPAGE_SIZE,  bufsize);
        record_count = 0;
        done_flag = 0;
        return file_open();
    }

    virtual void close() {
        done_flag = 1;
        if (datasize > 0) file_write();
        file_close();
        mem_aligned_free(buffer);
    }

    virtual void write(BaseRecordFormat *record) {
        if ((uint64_t)record->get_record_size() > bufsize) {
            LOG_ERROR("The write record length is larger than the buffer size!\n");
        }
        if (record->get_record_size() + datasize > bufsize) {
            file_write();
        }
        //printf("record size=%d\n", record->get_record_size());
        KVRecord kv;
        kv.set_buffer(buffer + datasize);
        kv.convert((KVRecord*)record);
        //memcpy(buffer + datasize, record->get_record(),
        //       record->get_record_size());
        datasize += record->get_record_size();
        record_count++;
    }

    virtual uint64_t get_record_count() { return record_count; }

    virtual bool file_open() {
        TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

        //std::ostringstream oss;
        //oss << mimir_world_size << "." << mimir_world_rank;
        //filename += oss.str();

        PROFILER_RECORD_TIME_START;

        union_fp.c_fp = fopen(filename.c_str(), "w+");
        if (!union_fp.c_fp) {
            LOG_ERROR("Open file %s error!\n", filename.c_str());
        }

        PROFILER_RECORD_TIME_END(TIMER_PFS_OUTPUT);

        TRACKER_RECORD_EVENT(EVENT_DISK_FOPEN);

        LOG_PRINT(DBG_IO, "Open output file %s.\n", filename.c_str());	

        return true;
    }

    virtual void file_write() {
        TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

        LOG_PRINT(DBG_IO, "Write output file %s:%d\n", 
                  filename.c_str(), (int)datasize);

        PROFILER_RECORD_TIME_START;
        fwrite(buffer, datasize, 1, union_fp.c_fp);
        PROFILER_RECORD_TIME_END(TIMER_PFS_OUTPUT);

        datasize = 0;
        TRACKER_RECORD_EVENT(EVENT_DISK_FWRITE);
    }

    virtual void file_close() {
        if (union_fp.c_fp) {
            TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

            PROFILER_RECORD_TIME_START;
            fclose(union_fp.c_fp);
            PROFILER_RECORD_TIME_END(TIMER_PFS_OUTPUT);

            union_fp.c_fp = NULL;
            TRACKER_RECORD_EVENT(EVENT_DISK_FCLOSE);

            LOG_PRINT(DBG_IO, "Close output file %s.\n", filename.c_str());	
        }
    }

  protected:
    std::string filename;
    uint64_t record_count;

    BaseShuffler     *shuffler;

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
};

class DirectFileWriter : public FileWriter
{
  public:
    DirectFileWriter(const char *filename) : FileWriter(filename, false) {
    }

    virtual bool file_open() {
        TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

        PROFILER_RECORD_TIME_START;

        union_fp.posix_fd = ::open(filename.c_str(), O_CREAT | O_WRONLY | 
                                   O_DIRECT | O_LARGEFILE,
                                   S_IRUSR | S_IWUSR);
        if (union_fp.posix_fd == -1) {
            LOG_ERROR("Open file %s error %d!\n", filename.c_str(), errno);
        }

        filesize = 0;

        PROFILER_RECORD_TIME_END(TIMER_PFS_OUTPUT);

        TRACKER_RECORD_EVENT(EVENT_DISK_FOPEN);

        LOG_PRINT(DBG_IO, "Open (POSIX) output file %s.\n", filename.c_str());	

        return true;
    }

    virtual void file_write() {
        TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

        LOG_PRINT(DBG_IO, "Write (POSIX) output file %s:%d\n", 
                  filename.c_str(), (int)datasize);

        PROFILER_RECORD_TIME_START;
        //::lseek64(union_fp.posix_fd, 0, SEEK_END);
        uint64_t total_bytes = 0;
        if (done_flag) {
            total_bytes = ROUNDUP(datasize, DISKPAGE_SIZE) * DISKPAGE_SIZE;
        } else {
            total_bytes = ROUNDDOWN(datasize, DISKPAGE_SIZE) * DISKPAGE_SIZE;
        }
        uint64_t remain_bytes = total_bytes;
        char *remain_buffer = buffer;
        do {
            ssize_t write_bytes = ::write(union_fp.posix_fd, remain_buffer, remain_bytes);
            if (write_bytes == -1) {
                LOG_ERROR("Write error, %d\n", errno);
            }
            remain_bytes -= write_bytes;
            remain_buffer += write_bytes;
        } while (remain_bytes > 0);
        PROFILER_RECORD_TIME_END(TIMER_PFS_OUTPUT);

        if (total_bytes < datasize) {
            filesize += total_bytes;
            datasize = datasize - total_bytes;
            for (size_t i = 0; i < datasize; i++) {
                buffer[i] = buffer[total_bytes + i];
            }
        } else if (total_bytes > datasize ) {
            filesize += datasize;
            datasize = 0;
            LOG_PRINT(DBG_IO, "Set (POSIX) output file %s:%ld\n", 
                      filename.c_str(), filesize);
            ::ftruncate64(union_fp.posix_fd, filesize);
        } else {
            filesize += datasize;
            datasize = 0;
        }
        TRACKER_RECORD_EVENT(EVENT_DISK_FWRITE);
    }

    virtual void file_close() {
        if (union_fp.posix_fd != -1) {
            TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

            PROFILER_RECORD_TIME_START;
            ::close(union_fp.posix_fd);
            PROFILER_RECORD_TIME_END(TIMER_PFS_OUTPUT);

            union_fp.posix_fd = -1;
            TRACKER_RECORD_EVENT(EVENT_DISK_FCLOSE);

            LOG_PRINT(DBG_IO, "Close (POSIX) output file %s.\n", filename.c_str());	
        }
    }

  private:
    off64_t     filesize;
};

class MPIFileWriter : public FileWriter {
  public:
    MPIFileWriter(const char *filename) : FileWriter(filename, true) {
    }

    virtual bool file_open() {
        TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

        MPI_Request req;
        MPI_Status st;
        if (this->shuffler) {
            //PROFILER_RECORD_TIME_START;
            MPI_Ibarrier(mimir_world_comm, &req);
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

        LOG_PRINT(DBG_IO, "Collective open output file %s.\n", filename.c_str());	
        PROFILER_RECORD_TIME_START;
        MPI_CHECK(MPI_File_open(mimir_world_comm, filename.c_str(), 
                                MPI_MODE_WRONLY | MPI_MODE_CREATE,
                                MPI_INFO_NULL, &(union_fp.mpi_fp)));
        if (union_fp.mpi_fp == MPI_FILE_NULL) {
            LOG_ERROR("Open file %s error!\n", filename.c_str());
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
        int sendcounts[mimir_world_size];

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

        if (this->shuffler) {
            //PROFILER_RECORD_TIME_START;
            MPI_Ibarrier(mimir_world_comm, &req);
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
        MPI_Allgather(&datasize, 1, MPI_INT,
                      sendcounts, 1, MPI_INT, mimir_world_comm);
        PROFILER_RECORD_TIME_END(TIMER_COMM_ALLGATHER);

        TRACKER_RECORD_EVENT(EVENT_COMM_ALLGATHER);

        fileoff = filesize;
        for (int i = 0; i < mimir_world_rank; i++) fileoff += sendcounts[i];
        for (int i = 0; i < mimir_world_size; i++) filesize += sendcounts[i];

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
                  filename.c_str(), fileoff, (int)datasize);

        PROFILER_RECORD_TIME_START;
        MPI_CHECK(MPI_File_write_at_all(union_fp.mpi_fp, fileoff, buffer,
                                        (int)datasize, MPI_BYTE, &st));
        datasize = 0;
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
        MPI_Allreduce(&done_flag, &done_count, 1, MPI_INT, MPI_SUM, 
                       mimir_world_comm);
        PROFILER_RECORD_TIME_END(TIMER_COMM_RDC);

        TRACKER_RECORD_EVENT(EVENT_SYN_COMM);
    }

    virtual void file_close() {
        if (union_fp.mpi_fp) {
            while (done_count < mimir_world_size) {
                file_write();
            }

            TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

            MPI_Request req;
            MPI_Status st;
            if (this->shuffler) {
                //PROFILER_RECORD_TIME_START;
                MPI_Ibarrier(mimir_world_comm, &req);
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
            MPI_CHECK(MPI_File_close(&(union_fp.mpi_fp)));
            union_fp.mpi_fp = MPI_FILE_NULL;
            PROFILER_RECORD_TIME_END(TIMER_PFS_OUTPUT);

            TRACKER_RECORD_EVENT(EVENT_DISK_MPICLOSE);

            LOG_PRINT(DBG_IO, "Collective close output file %s.\n", filename.c_str());	
        }
    }
  private:
    int done_count;
    MPI_Offset filesize;
};

}

#endif
