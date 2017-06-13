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
    ShuffleMsgState msg_state;
    uint64_t        send_bytes;
    uint64_t        recv_bytes;
};

template <typename KeyType, typename ValType>
class NBCollectiveShuffler : public BaseShuffler<KeyType, ValType> {
public:
  NBCollectiveShuffler(MPI_Comm comm,
                       Writable<KeyType, ValType> *out,
                       int (*user_hash)(KeyType* key, ValType* val, int npartition),
                       int keycount, int valcount)
      :  BaseShuffler<KeyType, ValType>(comm, out, user_hash, keycount, valcount)
      {
          if (COMM_BUF_SIZE < (int64_t) COMM_UNIT_SIZE * (int64_t) this->shuffle_size) {
              LOG_ERROR("Error: send buffer(%ld) should be larger than COMM_UNIT_SIZE(%d)*size(%d).\n", COMM_BUF_SIZE, COMM_UNIT_SIZE, this->shuffle_size);
          }
          buf_size = (COMM_BUF_SIZE / COMM_UNIT_SIZE / this->shuffle_size) * COMM_UNIT_SIZE;
          type_log_bytes = 0;
          int type_bytes = 0x1;
          while ((int64_t) type_bytes * (int64_t) MAX_COMM_SIZE 
                 < buf_size * this->shuffle_size) {
              type_bytes <<= 1;
              type_log_bytes++;
          }
          MPI_Comm_dup(this->shuffle_comm, &a2a_comm);
          MPI_Comm_dup(this->shuffle_comm, &a2av_comm);
          buf_count = 0;
      }

    virtual ~NBCollectiveShuffler()
    {
        MPI_Comm_free(&a2a_comm);
        MPI_Comm_free(&a2av_comm);
    }

    virtual int open() {
        this->done_flag = 0;
        this->done_count = 0;
        buf_count = 0;
        cur_idx = 0;
        a2a_token = 0;
        a2av_token = 0;
        pending_msg = 0;
        for (int i = 0; i < MIN_SBUF_COUNT; i++)
            insert_comm_buffer();

        a2a_s_count = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * this->shuffle_size);
        ;
        a2a_s_displs = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * this->shuffle_size);
        ;
        a2a_r_count = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * this->shuffle_size);
        ;
        a2a_r_displs = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * this->shuffle_size);
        ;
        for (int i = 0; i < this->shuffle_size; i++) {
            a2a_s_count[i] = 0;
            a2a_s_displs[i] = 0;
            a2a_r_count[i] = 0;
            a2a_r_displs[i] = 0;
        }

        MPI_Type_contiguous((0x1 << type_log_bytes), MPI_BYTE, &comm_type);
        MPI_Type_commit(&comm_type);

        LOG_PRINT(DBG_GEN, "NBCollectiveShuffler open: buf_size=%ld\n", buf_size);
        return true;
    }

    virtual void close() {
        wait();

        PROFILER_RECORD_COUNT(COUNTER_COMM_BUFS, buf_count, OPMAX);

        MPI_Type_free(&comm_type);

        mem_aligned_free(a2a_r_displs);
        mem_aligned_free(a2a_r_count);
        mem_aligned_free(a2a_s_count);
        mem_aligned_free(a2a_s_displs);

        for (int i = 0; i < buf_count; i++) {
            mem_aligned_free(msg_buffers[i].send_buffer);
            mem_aligned_free(msg_buffers[i].recv_buffer);
            mem_aligned_free(msg_buffers[i].send_offset);
            mem_aligned_free(msg_buffers[i].recv_count);
        }
        LOG_PRINT(DBG_GEN, "NBCollectiveShuffler close.\n");

    }

    virtual int write(KeyType *key, ValType *val)
    {
        //int target = get_target_rank(((KVRecord*)record)->get_key(), 
        //                             ((KVRecord*)record)->get_key_size());

        int target = this->get_target_rank(key, val);

        if (target == this->shuffle_rank) {
            this->out->write(key, val);
            return 0;
        }

        //int kvsize = record->get_record_size();
        int kvsize = this->ser->get_kv_bytes(key, val);
        if (kvsize > buf_size)
            LOG_ERROR("Error: KV size (%d) is larger than buf_size (%ld)\n", 
                      kvsize, buf_size);

        if ((int64_t)msg_buffers[cur_idx].send_offset[target] + (int64_t)kvsize > buf_size) {
            start_kv_exchange();
        }

        char *buffer = msg_buffers[cur_idx].send_buffer + target * (int64_t)buf_size 
            + msg_buffers[cur_idx].send_offset[target];
        //kv.set_buffer(buffer);
        //kv.convert((KVRecord*)record);
        kvsize = this->ser->kv_to_bytes(key, val, buffer, kvsize);
        msg_buffers[cur_idx].send_offset[target] += kvsize;

        push_kv_exchange();
        this->kvcount++;

        return  0;
    }

    virtual void make_progress(bool issue_new = false) {
        if (issue_new && pending_msg == 0)
            start_kv_exchange();
        push_kv_exchange();
    }

protected:
    void wait()
    {
        MPI_Status st;
        int flag = 0;

        for (int i = 0; i < this->shuffle_size; i++)
            msg_buffers[cur_idx].send_bytes += (uint64_t)msg_buffers[cur_idx].send_offset[i];

        if (msg_buffers[cur_idx].send_bytes > 0)
            start_kv_exchange();

        LOG_PRINT(DBG_COMM, "Comm: start wait.\n");

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

        wait_all();

        this->done_flag = 1;
        do {
            PROFILER_RECORD_TIME_START;
            start_kv_exchange();
            wait_all();
            PROFILER_RECORD_TIME_END(TIMER_COMM_BLOCK);
        } while (this->done_count < this->shuffle_size);

        TRACKER_RECORD_EVENT(EVENT_SYN_COMM);

        LOG_PRINT(DBG_COMM, "Comm: finish wait.\n");
    }

    void start_kv_exchange()
    {
        if (this->done_flag) {
            for (int i = 0; i < this->shuffle_size; i++)
                msg_buffers[cur_idx].send_offset[i] = -1;
        } else {
            for (int i = 0; i < this->shuffle_size; i++)
                msg_buffers[cur_idx].send_bytes += (uint64_t)msg_buffers[cur_idx].send_offset[i];
            PROFILER_RECORD_COUNT(COUNTER_SEND_BYTES, msg_buffers[cur_idx].send_bytes, OPSUM);
        }
        PROFILER_RECORD_COUNT(COUNTER_SHUFFLE_TIMES, 1, OPSUM);

        MPI_Ialltoall(msg_buffers[cur_idx].send_offset, 1, MPI_INT,
                      msg_buffers[cur_idx].recv_count, 1, MPI_INT,
                      a2a_comm, &(msg_buffers[cur_idx].a2a_req));

        msg_buffers[cur_idx].msg_state = ShuffleMsgStart;
        msg_buffers[cur_idx].msg_token = a2a_token;
        a2a_token += 1;
        pending_msg += 1;

        LOG_PRINT(DBG_COMM, "Comm: MPI_Ialltoall start. (token=%ld, send=%ld)\n",
                  msg_buffers[cur_idx].msg_token, msg_buffers[cur_idx].send_bytes);

        push_kv_exchange();
        cur_idx = -1;
        for (int i = 0; i < buf_count; i++) {
            if (done_kv_exchange(i)) {
                cur_idx = i;
                break;
            }
        }

        if (cur_idx == -1) {
            if (buf_count < MAX_SBUF_COUNT) {
                insert_comm_buffer();
                cur_idx = buf_count - 1;
            }
            else {
                cur_idx = 0;
                TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
                PROFILER_RECORD_TIME_START;
                while (1) {
                    push_kv_exchange();
                    if (done_kv_exchange(cur_idx))
                        break;
                    cur_idx = (cur_idx + 1) % buf_count;
                }
                PROFILER_RECORD_TIME_END(TIMER_COMM_BLOCK);
                TRACKER_RECORD_EVENT(EVENT_SYN_COMM);
            }
        }
    }

    void push_kv_exchange() {
        uint64_t recvcount = 0;
        int flag = 0;
        MPI_Status st;

        for (int k = 0; k < buf_count; k++) {

            if (msg_buffers[k].msg_state == ShuffleMsgComplete) continue;

            if (msg_buffers[k].msg_state == ShuffleMsgStart) {

                if (msg_buffers[k].a2a_req != MPI_REQUEST_NULL) {

                    MPI_Test(&(msg_buffers[k].a2a_req), &flag, &st);

                    if (flag) {
                        this->done_count = 0;
                        for (int i = 0; i < this->shuffle_size; i++) {
                            if (msg_buffers[k].recv_count[i] == -1) this->done_count ++;
                            else
                                msg_buffers[k].recv_bytes += (int64_t) msg_buffers[k].recv_count[i];
                        }
                        LOG_PRINT(DBG_COMM, "Comm: MPI_Ialltoall finish (token=%ld, done=%d).\n",
                                  msg_buffers[k].msg_token, this->done_count);
                        msg_buffers[k].a2a_req = MPI_REQUEST_NULL;
                    }
                }

                if (this->done_count > this->shuffle_size)
                    LOG_ERROR("Done count %d is lager than process size %d!\n",
                              this->done_count, this->shuffle_size);

                if (this->done_count == this->shuffle_size) {
                    msg_buffers[k].msg_state = ShuffleMsgComplete;
                    pending_msg -= 1;
                }

                if (msg_buffers[k].msg_token == a2av_token 
                    && msg_buffers[k].a2a_req == MPI_REQUEST_NULL 
                    && this->done_count < this->shuffle_size) {

                    for (int i = 0; i < this->shuffle_size; i++) {
                        if (msg_buffers[k].send_offset[i] == -1)
                            msg_buffers[k].send_offset[i] = 0;
                        if (msg_buffers[k].recv_count[i] == -1)
                            msg_buffers[k].recv_count[i] = 0;
                        a2a_s_count[i] = (msg_buffers[k].send_offset[i] 
                                          + (0x1 << type_log_bytes) - 1) >> type_log_bytes;
                        a2a_r_count[i] = (msg_buffers[k].recv_count[i] 
                                          + (0x1 << type_log_bytes) - 1) >> type_log_bytes;
                        a2a_s_displs[i] = (i * (int) buf_size) >> type_log_bytes;
                    }
                    a2a_r_displs[0] = 0;
                    for (int i = 1; i < this->shuffle_size; i++)
                        a2a_r_displs[i] = a2a_r_displs[i - 1] + a2a_r_count[i - 1];

                    LOG_PRINT(DBG_COMM, "Comm: MPI_Ialltoallv start (token=%ld,send=%ld,recv=%ld).\n",
                              msg_buffers[k].msg_token, msg_buffers[k].send_bytes, msg_buffers[k].recv_bytes);

                    MPI_Ialltoallv(msg_buffers[k].send_buffer, a2a_s_count, a2a_s_displs, comm_type,
                                   msg_buffers[k].recv_buffer, a2a_r_count, a2a_r_displs, comm_type, 
                                   a2av_comm, &(msg_buffers[k].a2av_req));

                    PROFILER_RECORD_COUNT(COUNTER_RECV_BYTES, (uint64_t) msg_buffers[k].recv_bytes, OPSUM);

                    msg_buffers[k].msg_state = ShuffleMsgPending;
                    a2av_token += 1;
                }
            }

            if (msg_buffers[k].msg_state == ShuffleMsgPending) {
                if (msg_buffers[k].a2av_req != MPI_REQUEST_NULL) {
                    MPI_Test(&(msg_buffers[k].a2av_req), &flag, &st);
                    if (flag) {
                        LOG_PRINT(DBG_COMM, "Comm: MPI_Ialltoallv finish (token=%ld, done=%d).\n",
                                  msg_buffers[k].msg_token, this->done_count);
                        msg_buffers[k].a2av_req = MPI_REQUEST_NULL;
                        save_data(k);
                        msg_buffers[k].msg_token = 0;
                        msg_buffers[k].send_bytes = 0;
                        msg_buffers[k].recv_bytes = 0;
                        for (int i = 0; i < this->shuffle_size; i++) {
                            msg_buffers[k].send_offset[i] = 0;
                            msg_buffers[k].recv_count[i] = 0;
                        }
                        msg_buffers[k].msg_state = ShuffleMsgComplete;
                        pending_msg -= 1;
                    }
                }
            }

        }

        return;
    }

    bool done_kv_exchange(int idx) {
        if (msg_buffers[idx].msg_state == ShuffleMsgComplete) {
            return true;
        }
        return false;
    }
    void wait_all() {
        while (pending_msg > 0)
            push_kv_exchange();
    }
    void save_data(int idx)
    {
        //KVRecord record;
        char *src_buf = msg_buffers[idx].recv_buffer;
        int k = 0;
        for (k = 0; k < this->shuffle_size; k++) {
            int count = 0;
            while (count < msg_buffers[idx].recv_count[k]) {
                //int kvsize = 0;
                //record.set_buffer(src_buf);
                //kvsize = record.get_record_size();
                typename SafeType<KeyType>::type key[this->keycount];
                typename SafeType<ValType>::type val[this->valcount];
                int kvsize = this->ser->kv_from_bytes(&key[0], &val[0],
                     src_buf, this->msg_buffers[idx].recv_count[k] - count);
                this->out->write(key, val);
                src_buf += kvsize;
                count += kvsize;
            }
            int padding = msg_buffers[idx].recv_count[k] & ((0x1 << type_log_bytes) - 0x1);
            src_buf += padding;
        }
    }

    void insert_comm_buffer() {
        ShuffleMsgBuf buf;
        buf.send_buffer = (char*)mem_aligned_malloc(MEMPAGE_SIZE, buf_size * this->shuffle_size);
        buf.recv_buffer = (char*)mem_aligned_malloc(MEMPAGE_SIZE, buf_size * this->shuffle_size);
        buf.send_offset = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * this->shuffle_size);
        buf.recv_count = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * this->shuffle_size);
        buf.a2a_req = MPI_REQUEST_NULL;
        buf.a2av_req = MPI_REQUEST_NULL;
        buf.send_bytes = 0;
        buf.recv_bytes = 0;
        buf.msg_state = ShuffleMsgComplete;
        msg_buffers.push_back(buf);
        for (int i = 0; i < this->shuffle_size; i++) {
            msg_buffers[buf_count].send_offset[i] = 0;
            msg_buffers[buf_count].recv_count[i] = 0;
        }
        buf_count ++;
        LOG_PRINT(DBG_COMM, "Comm: add a comm buffer. (count=%ld)\n",
                  buf_count);
    }

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
    int                         pending_msg;
};

}
#endif
