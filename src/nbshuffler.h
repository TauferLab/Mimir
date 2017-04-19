/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_NB_SHUFFLER_H
#define MIMIR_NB_SHUFFLER_H

#include <mpi.h>
#include <vector>

#include "container.h"
#include "baseshuffler.h"

namespace MIMIR_NS {

class NBShuffler : public BaseShuffler {
public:
    NBShuffler(Writable *out, HashCallback user_hash);
    virtual ~NBShuffler();

    virtual bool open();
    virtual void close();
    virtual void write(BaseRecordFormat *record);
    virtual void make_progress(bool issue_new = false);

protected:
    void wait();
    void start_send(int);
    void save_data(int,int);

    int64_t buf_size;

    int  *cur_idx;
    int  buf_count;
    std::vector<char*> send_buffers;
    std::vector<int*>  send_offsets;
    char *recv_buffer;
    int  *recv_count;

    KVRecord kv;

    MPI_Request *send_reqs;
    MPI_Request *recv_reqs;
};

}
#endif
