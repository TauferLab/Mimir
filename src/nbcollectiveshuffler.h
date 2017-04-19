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

enum ShuffleMsgState {
    ShuffleMsgStart,
    ShuffleMsgPending,
    ShuffleMsgComplete};

struct ShuffleMsgBuf {
    char*           send_buffer;
    char*           recv_buffer;
    int*            send_offset;
    int*            recv_count;
    uint64_t        msg_token;
    MPI_Request     a2a_req;
    MPI_Request     a2av_req;
    MPI_Request     done_req;
    int             done_flag;
    int             done_count;
    ShuffleMsgState msg_state;
    uint64_t        send_bytes;
    uint64_t        recv_bytes;
};

class NBCollectiveShuffler : public BaseShuffler {
public:
    NBCollectiveShuffler(Writable *out, HashCallback user_hash);
    virtual ~NBCollectiveShuffler();

    virtual bool open();
    virtual void close();
    virtual void write(BaseRecordFormat *record);
    virtual void make_progress(bool issue_new = false) {
        if (issue_new && pending_msg == 0)
            start_kv_exchange();
        push_kv_exchange();
    }

protected:
    void wait();
    void start_kv_exchange();
    void push_kv_exchange();
    bool done_kv_exchange(int idx) {
        if (msg_buffers[idx].msg_state == ShuffleMsgComplete) {
            return true;
        }
        return false;
    }
    //void wait_kv_exchange(int idx) {
    //    while (!done_kv_exchange(idx)) {
    //        push_kv_exchange();
    //    }
    //}
    void wait_all() {
        //for (int i = 0; i < buf_count; i++)
        //    wait_kv_exchange(i);
        while (pending_msg > 0)
            push_kv_exchange();
    }
    void save_data(int idx);
    void insert_comm_buffer();

    int64_t buf_size;

    MPI_Datatype comm_type;
    int type_log_bytes;
    int *a2a_s_count;
    int *a2a_s_displs;
    int *a2a_r_count;
    int *a2a_r_displs;

    int                         cur_idx, buf_count;
    std::vector<ShuffleMsgBuf>  msg_buffers;

    uint64_t                    a2a_token;
    uint64_t                    a2av_token;
    MPI_Comm                    a2a_comm;
    MPI_Comm                    a2av_comm;
    MPI_Comm                    done_comm;
    int                         pending_msg;

    KVRecord kv;
};

}
#endif
