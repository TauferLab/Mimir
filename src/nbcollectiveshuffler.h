/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_NB_COLLECTIVE_SHUFFLER_H
#define MIMIR_NB_COLLECTIVE_SHUFFLER_H

#include <mpi.h>
#include <vector>

#include "container.h"
#include "baseshuffler.h"

namespace MIMIR_NS {

class NBCollectiveShuffler : public BaseShuffler {
public:
    NBCollectiveShuffler(Writable *out, HashCallback user_hash);
    virtual ~NBCollectiveShuffler();

    virtual bool open();
    virtual void close();
    virtual void write(BaseRecordFormat *record);
    virtual void make_progress() {
        if(done_kv_exchange())
            start_kv_exchange();
        push_kv_exchange();
    }

protected:
    void wait();
    void start_kv_exchange();
    void push_kv_exchange();
    bool done_kv_exchange();
    void pull_done_flag();
    void save_data();

    int64_t buf_size;

    MPI_Datatype comm_type;
    int type_log_bytes;
    int *a2a_s_count;
    int *a2a_s_displs;
    int *a2a_r_count;
    int *a2a_r_displs;

    int  cur_idx, pre_idx, buf_count;
    std::vector<char*> send_buffers;
    std::vector<int*>  send_offsets;
    char *recv_buffer;
    int  *recv_count;

    KVRecord kv;

    MPI_Request done_req;
    MPI_Request a2a_req;
    MPI_Request a2av_req;
};

}
#endif
