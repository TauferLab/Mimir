/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_COLLECTIVE_SHUFFLER_H
#define MIMIR_COLLECTIVE_SHUFFLER_H

#include <mpi.h>
#include <vector>

#include "serializer.h"
#include "container.h"
#include "baseshuffler.h"

namespace MIMIR_NS {

template <typename KeyType, typename ValType>
class CollectiveShuffler : public BaseShuffler<KeyType,ValType> {
public:
  CollectiveShuffler(MPI_Comm comm,
                     Writable<KeyType, ValType> *out,
                     int (*user_hash)(KeyType* key),
                     int keycount, int valcount) 
      : BaseShuffler<KeyType, ValType>(comm, out, user_hash,
                                       keycount, valcount)
      {
          if (COMM_BUF_SIZE < (int64_t) COMM_UNIT_SIZE * (int64_t) this->shuffle_size) {
              LOG_ERROR("Error: send buffer(%ld) should be larger than COMM_UNIT_SIZE(%d)*size(%d).\n",
                        COMM_BUF_SIZE, COMM_UNIT_SIZE, this->shuffle_size);
          }
          buf_size = (COMM_BUF_SIZE / COMM_UNIT_SIZE / this->shuffle_size) * COMM_UNIT_SIZE;
          type_log_bytes = 0;
          int type_bytes = 0x1;
          while ((int64_t) type_bytes * (int64_t) MAX_COMM_SIZE 
                 < buf_size * this->shuffle_size) {
              type_bytes <<= 1;
              type_log_bytes++;
          }
      }

    virtual ~CollectiveShuffler()
    {
    }

    virtual int open()
    {
        this->done_flag = 0;
        this->done_count = 0;

        send_buffer = (char*)mem_aligned_malloc(MEMPAGE_SIZE, buf_size * this->shuffle_size);
        recv_buffer = (char*)mem_aligned_malloc(MEMPAGE_SIZE, buf_size * this->shuffle_size);
        send_offset = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * this->shuffle_size);
        recv_count = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * this->shuffle_size);
        a2a_s_count = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * this->shuffle_size);
        ;
        a2a_s_displs = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * this->shuffle_size);
        ;
        a2a_r_count = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * this->shuffle_size);
        ;
        a2a_r_displs = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * this->shuffle_size);
        ;
        for (int i = 0; i < this->shuffle_size; i++) {
            send_offset[i] = 0;
            recv_count[i] = 0;
            a2a_s_count[i] = 0;
            a2a_s_displs[i] = 0;
            a2a_r_count[i] = 0;
            a2a_r_displs[i] = 0;
        }

        MPI_Type_contiguous((0x1 << type_log_bytes), MPI_BYTE, &comm_type);
        MPI_Type_commit(&comm_type);

        PROFILER_RECORD_COUNT(COUNTER_COMM_BUFS, 1, OPMAX);

        LOG_PRINT(DBG_GEN, "CollectiveShuffler open: buf_size=%ld\n", buf_size);

        return true;
    }

    virtual void close() {
        wait();

        MPI_Type_free(&comm_type);

        mem_aligned_free(a2a_r_displs);
        mem_aligned_free(a2a_r_count);
        mem_aligned_free(a2a_s_count);
        mem_aligned_free(a2a_s_displs);
        mem_aligned_free(send_buffer);
        mem_aligned_free(recv_buffer);
        mem_aligned_free(send_offset);
        mem_aligned_free(recv_count);

        LOG_PRINT(DBG_GEN, "CollectiveShuffler close.\n");
    }

    virtual int write(KeyType *key, ValType *val)
    {
        int target = this->get_target_rank(key);

        if (target == this->shuffle_rank) {
            int ret = this->out->write(key, val);
            if (BALANCE_LOAD && !(this->user_hash) && ret == 1) {
                char tmpkey[MAX_RECORD_SIZE];
                int keysize = this->ser->get_key_bytes(key);
                if (keysize > MAX_RECORD_SIZE) LOG_ERROR("The key is too long!\n");
                this->ser->key_to_bytes(key, tmpkey, MAX_RECORD_SIZE);
                uint32_t hid = hashlittle(tmpkey, keysize, 0);
                int bidx = (int) (hid % (uint32_t) (this->shuffle_size * SAMPLE_COUNT));
                auto iter = this->bin_table.find(bidx);
                if (iter != this->bin_table.end()) {
                    iter->second += 1;
                    this->local_kv_count += 1;
                } else {
                    LOG_ERROR("Wrong bin index=%d\n", bidx);
                }
            }
            return 0;
        }

        int kvsize = this->ser->get_kv_bytes(key, val);
        if (kvsize > buf_size)
            LOG_ERROR("Error: KV size (%d) is larger than buf_size (%ld)\n", 
                      kvsize, buf_size);

        if ((int64_t)send_offset[target] + (int64_t)kvsize > buf_size) {
            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
            exchange_kv();
        }

        char *buffer = send_buffer + target * (int64_t)buf_size + send_offset[target];
        kvsize = this->ser->kv_to_bytes(key, val, buffer, (int)buf_size - send_offset[target]);
        //kv.set_buffer(buffer);
        //kv.convert((KVRecord*)record);
        send_offset[target] += kvsize;
        this->kvcount++;
        return 0;
    }
    virtual void make_progress(bool issue_new = false) { exchange_kv(); }

protected:

    void wait()
    {
        LOG_PRINT(DBG_COMM, "Comm: start wait.\n");

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

        uint64_t sendcount = 0;

        for (int i = 0; i < this->shuffle_size; i++)
            sendcount += (uint64_t) send_offset[i];

        while (sendcount > 0 || this->shuffle_times == 0) {
            exchange_kv();
            sendcount = 0;
            for (int i = 0; i < this->shuffle_size; i++)
                sendcount += (uint64_t) send_offset[i];
        }

        this->done_flag = 1;

        do {
            exchange_kv();
        } while (this->done_count < this->shuffle_size);

        TRACKER_RECORD_EVENT(EVENT_SYN_COMM);
        LOG_PRINT(DBG_COMM, "Comm: finish wait.\n");
    }

    void save_data()
    {
        typename SafeType<KeyType>::type key[this->keycount];
        typename SafeType<ValType>::type val[this->valcount];

        char *src_buf = recv_buffer;
        int k = 0;
        for (k = 0; k < this->shuffle_size; k++) {
            int count = 0;
            while (count < recv_count[k]) {
                int kvsize = this->ser->kv_from_bytes(&key[0], &val[0],
                     src_buf, recv_count[k] - count);
                int ret = this->out->write(key, val);
                if (BALANCE_LOAD && !(this->user_hash) && ret == 1) {
                    int keysize = this->ser->get_key_bytes(&key[0]);
                    uint32_t hid = hashlittle(src_buf, keysize, 0);
                    int bidx = (int) (hid % (uint32_t) (this->shuffle_size * SAMPLE_COUNT));
                    auto iter = this->bin_table.find(bidx);
                    if (iter != this->bin_table.end()) {
                        iter->second += 1;
                        this->local_kv_count += 1;
                    } else {
                        LOG_ERROR("Wrong bin index=%d\n", bidx);
                    }
                }
                src_buf += kvsize;
                count += kvsize;
            }
            int padding = recv_count[k] & ((0x1 << type_log_bytes) - 0x1);
            src_buf += padding;
        }
    }

    void exchange_kv()
    {
        uint64_t sendcount = 0, recvcount = 0;

        for (int i = 0; i < this->shuffle_size; i++)
            sendcount += (uint64_t) send_offset[i];
        PROFILER_RECORD_COUNT(COUNTER_SEND_BYTES, sendcount, OPSUM);

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

        LOG_PRINT(DBG_COMM, "Comm: start alltoall\n");

        if (this->done_flag) {
            for (int i = 0; i < this->shuffle_size; i++) {
                send_offset[i] = -1;
            }
        }

        PROFILER_RECORD_TIME_START;
        MPI_Alltoall(send_offset, 1, MPI_INT,
                     recv_count, 1, MPI_INT, this->shuffle_comm);
        PROFILER_RECORD_TIME_END(TIMER_COMM_A2A);

        TRACKER_RECORD_EVENT(EVENT_COMM_ALLTOALL);

        this->done_count  = 0;
        for (int i = 0; i < this->shuffle_size; i++) {
            if (recv_count[i] == -1) {
                this->done_count++;
                recv_count[i] = 0;
            }
            if (send_offset[i] == -1) {
                send_offset[i] = 0;
            }
        }

        if (this->done_count >= this->shuffle_size) return;

        recvcount = (uint64_t) recv_count[0];
        for (int i = 1; i < this->shuffle_size; i++) {
            recvcount += (int64_t) recv_count[i];
        }

        for (int i = 0; i < this->shuffle_size; i++) {
            a2a_s_count[i] = (send_offset[i] + (0x1 << type_log_bytes) - 1)
                >> type_log_bytes;
            a2a_r_count[i] = (recv_count[i] + (0x1 << type_log_bytes) - 1)
                >> type_log_bytes;
            a2a_s_displs[i] = (i * (int) buf_size) >> type_log_bytes;
        }
        a2a_r_displs[0] = 0;
        for (int i = 1; i < this->shuffle_size; i++)
            a2a_r_displs[i] = a2a_r_displs[i - 1] + a2a_r_count[i - 1];

        LOG_PRINT(DBG_COMM, "Comm: start alltoallv\n");

        PROFILER_RECORD_TIME_START;
        MPI_Alltoallv(send_buffer, a2a_s_count, a2a_s_displs, comm_type,
                      recv_buffer, a2a_r_count, a2a_r_displs, comm_type, 
                      this->shuffle_comm);
        PROFILER_RECORD_TIME_END(TIMER_COMM_A2AV);

        TRACKER_RECORD_EVENT(EVENT_COMM_ALLTOALLV);

        LOG_PRINT(DBG_COMM, "Comm: receive data. (count=%ld)\n", recvcount);

        PROFILER_RECORD_COUNT(COUNTER_RECV_BYTES, (uint64_t) recvcount, OPSUM);
        if (recvcount > 0) {
            save_data();
        }

        for (int i = 0; i < this->shuffle_size; i++) send_offset[i] = 0;

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

        PROFILER_RECORD_COUNT(COUNTER_SHUFFLE_TIMES, 1, OPSUM);

        LOG_PRINT(DBG_COMM, "Comm: exchange KV. (send count=%ld, recv count=%ld, done count=%d)\n", sendcount, recvcount, this->done_count);

        if (BALANCE_LOAD && !(this->user_hash) && this->check_load_balance() == false) {
            LOG_PRINT(DBG_REPAR, "Load balance start\n");
            this->balance_load();
            LOG_PRINT(DBG_REPAR, "Load balance end\n");
        }

        this->shuffle_times += 1;
    }

    int64_t buf_size;

    MPI_Datatype comm_type;
    int type_log_bytes;
    int *a2a_s_count;
    int *a2a_s_displs;
    int *a2a_r_count;
    int *a2a_r_displs;

    char *send_buffer;
    int  *send_offset;
    char *recv_buffer;
    int  *recv_count;
};

}
#endif
