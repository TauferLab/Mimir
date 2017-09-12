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
                     int (*user_hash)(KeyType* key, ValType* val, int npartition),
                     int keycount, int valcount,
                     bool split_hint, HashBucket<> *h) 
      : BaseShuffler<KeyType, ValType>(comm, out, user_hash,
                                       keycount, valcount,
                                       split_hint, h)
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

        LOG_PRINT(DBG_GEN, "CollectiveShuffler open: allocation start, peakmem=%ld\n",
                  peakmem);

        send_buffer = (char*)mem_aligned_malloc(MEMPAGE_SIZE, buf_size * this->shuffle_size, MCDRAM_ALLOCATE);
        recv_buffer = (char*)mem_aligned_malloc(MEMPAGE_SIZE, buf_size * this->shuffle_size, MCDRAM_ALLOCATE);
        send_offset = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * this->shuffle_size, MCDRAM_ALLOCATE);
        recv_count = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * this->shuffle_size, MCDRAM_ALLOCATE);
        a2a_s_count = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * this->shuffle_size, MCDRAM_ALLOCATE);
        ;
        a2a_s_displs = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * this->shuffle_size, MCDRAM_ALLOCATE);
        ;
        a2a_r_count = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * this->shuffle_size, MCDRAM_ALLOCATE);
        ;
        a2a_r_displs = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * this->shuffle_size, MCDRAM_ALLOCATE);
        ;

        LOG_PRINT(DBG_GEN, "CollectiveShuffler open: allocation end\n");

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

        LOG_PRINT(DBG_GEN, "CollectiveShuffler open: buf_size=%ld, peakmem=%ld\n",
                  buf_size, peakmem);

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
        int target = this->get_target_rank(key, val);

        if (target == this->shuffle_rank) {
            int ret = this->out->write(key, val);
            if (BALANCE_LOAD && !(this->user_hash)) {
                this->record_bin_info(key, ret);
                //uint32_t hid = this->ser->get_hash_code(key);
                //uint32_t bidx = hid % (uint32_t) (this->shuffle_size * BIN_COUNT);
                //auto iter = this->bin_table.find(bidx);
                //if (iter != this->bin_table.end()) {
                //    iter->second += 1;
                //    this->local_kv_count += 1;
                //} else {
                //    LOG_ERROR("Wrong bin index=%d\n", bidx);
                //}
            }
            return true;
        }

        char *buffer = send_buffer + target * (int64_t)buf_size + send_offset[target];
        int kvsize = this->ser->kv_to_bytes(key, val, buffer, (int)buf_size - send_offset[target]);

        if (kvsize == -1) {
            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
            exchange_kv();
            target = this->get_target_rank(key, val);
            buffer = send_buffer + target * (int64_t)buf_size + send_offset[target];
            kvsize = this->ser->kv_to_bytes(key, val, buffer, (int)buf_size - send_offset[target]);
            if (kvsize == -1) {
                LOG_ERROR("Error: KV size (%d) is larger than buf_size (%ld)\n", 
                          kvsize, buf_size);
            }
        }

        send_offset[target] += kvsize;
        this->kvcount++;

        return true;
    }

    virtual void make_progress(bool issue_new = false) { exchange_kv(); }

    virtual void migrate_kvs(std::map<uint32_t,int>& redirect_bins,
                             std::set<int>& send_procs,
                             std::set<int>& recv_procs,
                             std::unordered_set<uint32_t>& suspect_table,
                             std::unordered_set<uint32_t>& local_split_table) {

        std::unordered_map<uint32_t, uint64_t> suspect_stat;

        MPI_Request req;
        MPI_Status st;
        int flag, count;

        LOG_PRINT(DBG_COMM, "Comm: migrate send procs=%ld, recv procs=%ld\n",
                  send_procs.size(), recv_procs.size());

        // Create send procs map
        std::map<int,int> send_procs_map;
        int idx = 0;
        for (auto send_proc : send_procs) {
            send_procs_map[send_proc] = idx;
            idx ++;
        }

        // Send KVs
        this->out->seek(DB_START);

        if (send_procs.size() > 0) {
            int factor = this->shuffle_size / (int)send_procs.size();

            typename SafeType<KeyType>::type key[this->keycount];
            typename SafeType<ValType>::type val[this->valcount];

            while(this->out_reader->read(key,val) == true) {
                uint32_t hid = this->ser->get_hash_code(key);
                uint32_t bid = hid % (uint32_t) (this->shuffle_size * BIN_COUNT);

                // Gather stat of suspect
                if (suspect_table.find(bid) != suspect_table.end()) {
                    if (suspect_stat.find(hid) != suspect_stat.end()) {
                        suspect_stat[hid] += 1;
                    } else {
                        suspect_stat[hid] = 0;
                    }
                }

                // This KV needs migrate out
                auto iter = redirect_bins.find(bid);
                if (iter != redirect_bins.end()) {
                    int target = send_procs_map[iter->second];
                    char *buffer = send_buffer + factor * target * (int64_t)buf_size;
                    int kvsize = this->ser->kv_to_bytes(key, val,
                                    buffer + send_offset[target],
                                    (int)buf_size * factor - send_offset[target]);
                    if (kvsize == -1) {
                        MPI_Isend(buffer, send_offset[target], MPI_BYTE,
                                  iter->second, LB_MIGRATE_TAG,
                                  this->shuffle_comm, &req);
                        MPI_Wait(&req, &st);
                        LOG_PRINT(DBG_COMM, "Comm: migrate to %d size=%d\n",
                                  iter->second, send_offset[target]);
                        send_offset[target] = 0;
                        buffer = send_buffer + factor * target * (int64_t)buf_size;
                        kvsize = this->ser->kv_to_bytes(key, val, buffer + send_offset[target], 
                                            (int)buf_size * factor - send_offset[target]);
                        if (kvsize == -1) LOG_ERROR("Error to send out the KV!\n");
                    }
                    send_offset[target] += kvsize;
                    this->out_mover->remove();
                }
            }
            for (auto iter : send_procs) {
                int target = send_procs_map[iter];
                if (send_offset[target] != 0) {
                    char *buffer = send_buffer + factor * target * (int64_t)buf_size;
                    MPI_Isend(buffer, send_offset[target], MPI_BYTE,
                            iter, LB_MIGRATE_TAG, this->shuffle_comm, &req);
                    MPI_Wait(&req, &st);
                    LOG_PRINT(DBG_COMM, "Comm: migrate to %d size=%d\n",
                              iter, send_offset[target]);
                }
                send_offset[target] = 0;
                MPI_Isend(NULL, 0, MPI_BYTE, iter, LB_MIGRATE_TAG,
                          this->shuffle_comm, &req);
            }
        } else {
            if (suspect_table.size() != 0) {
                typename SafeType<KeyType>::type key[this->keycount];
                typename SafeType<ValType>::type val[this->valcount];

                while(this->out_reader->read(key,val) == true) {
                    uint32_t hid = this->ser->get_hash_code(key);
                    uint32_t bid = hid % (uint32_t) (this->shuffle_size * BIN_COUNT);

                    // Gather stat of suspect
                    if (suspect_table.find(bid) != suspect_table.end()) {
                        if (suspect_stat.find(hid) != suspect_stat.end()) {
                            suspect_stat[hid] += 1;
                        } else {
                            suspect_stat[hid] = 0;
                        }
                    }
                }
            }
        }

        // Update split table
        if (suspect_table.size() != 0) {
            for (auto iter : suspect_stat) {
                if ((double)iter.second * this->shuffle_size > (double)this->global_kv_count * 0.8) {
                    uint32_t hid = iter.first;
                    uint32_t bid = hid % (uint32_t)(this->shuffle_size * BIN_COUNT);
                    if (this->split_table.find(hid) == this->split_table.end())
                    {
                        LOG_PRINT(DBG_REPAR, "Find split key hid=%u, bid=%u\n", hid, bid);
                        local_split_table.insert(hid);
                    }
                }
            }
        }

        //LOG_PRINT(DBG_REPAR, "Find suspect table=%ld, split table=%ld, ignore_table=%ld\n",
        //          suspect_table.size(), local_split_table.size(), ignore_table.size());

        // Recv KVs
        this->out->seek(DB_END);
        int recv_proc_count = (int)recv_procs.size();
        while ( recv_proc_count > 0) {
            typename SafeType<KeyType>::ptrtype key = NULL;
            typename SafeType<ValType>::ptrtype val = NULL;

            MPI_Irecv(recv_buffer, (int)buf_size * this->shuffle_size,
                      MPI_BYTE, MPI_ANY_SOURCE, LB_MIGRATE_TAG,
                      this->shuffle_comm, &req);

            flag = 0;
            while (!flag) MPI_Test(&req, &flag, &st);

            MPI_Get_count(&st, MPI_BYTE, &count);
            if (count == 0) {
                recv_proc_count --;
                continue;
            }

            LOG_PRINT(DBG_COMM, "Comm: migrate from %d size=%d\n",
                      st.MPI_SOURCE, count);

            int offset = 0;
            char *src_buf = recv_buffer;
            while (offset < count) {
                int kvsize = this->ser->kv_from_bytes(&key, &val,
                     src_buf, count - offset);
                int ret = this->out->write(key, val);
                if (BALANCE_LOAD && !(this->user_hash)) {
                    this->record_bin_info(key, ret);
                    //this->ser->get_key_bytes(&key[0]);
                    //uint32_t hid = this->ser->get_hash_code(key);
                    //uint32_t bidx = hid % (uint32_t) (this->shuffle_size * BIN_COUNT);
                    //auto iter = this->bin_table.find(bidx);
                    //if (iter != this->bin_table.end()) {
                    //    iter->second += 1;
                    //    this->local_kv_count += 1;
                    //} else {
                    //    LOG_ERROR("Wrong bin index=%d\n", bidx);
                    //}
                }
                src_buf += kvsize;
                offset += kvsize;
            }
        }
    }

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

        auto iter = this->bin_table.begin();
        while (iter != this->bin_table.end()) {
            PROFILER_RECORD_COUNT(COUNTER_MAX_BIN_SIZE, iter->second.first, OPMAX);
            iter ++;
        }

        TRACKER_RECORD_EVENT(EVENT_SYN_COMM);
        LOG_PRINT(DBG_COMM, "Comm: finish wait.\n");
    }

    void save_data()
    {

        typename SafeType<KeyType>::ptrtype key = NULL;
        typename SafeType<ValType>::ptrtype val = NULL;

        char *src_buf = recv_buffer;
        int k = 0;
        for (k = 0; k < this->shuffle_size; k++) {
            int count = 0;
            while (count < recv_count[k]) {
                int kvsize = this->ser->kv_from_bytes(&key, &val,
                     src_buf, recv_count[k] - count);
                int ret = this->out->write(key, val);
                if (BALANCE_LOAD && !(this->user_hash)) {
                    this->record_bin_info(key, ret);
                    //this->ser->get_key_bytes(&key[0]);
                    //uint32_t hid = this->ser->get_hash_code(key);
                    //uint32_t bidx = hid % (uint32_t) (this->shuffle_size * BIN_COUNT);
                    //auto iter = this->bin_table.find(bidx);
                    //if (iter != this->bin_table.end()) {
                    //    iter->second += 1;
                    //    this->local_kv_count += 1;
                    //} else {
                    //    LOG_ERROR("Wrong bin index=%d\n", bidx);
                    //}
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

        LOG_PRINT(DBG_COMM, "Comm: start alltoall (isrepartition=%d, peakmem=%ld)\n", this->isrepartition, peakmem);

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

        if (BALANCE_LOAD && !(this->user_hash) && this->shuffle_times % BALANCE_FREQ == 0) {
            PROFILER_RECORD_TIME_START;
            bool flag = this->check_load_balance();
            PROFILER_RECORD_TIME_END(TIMER_LB_CHECK);
            if (flag == false) {
                LOG_PRINT(DBG_GEN, "shuffle index=%d: load balance start\n", this->shuffle_times);
                PROFILER_RECORD_TIME_START;
                this->balance_load();
                PROFILER_RECORD_TIME_END(TIMER_LB_MIGRATE);
                PROFILER_RECORD_COUNT(COUNTER_BALANCE_TIMES, 1, OPSUM);
                LOG_PRINT(DBG_GEN, "shuffle index=%d: load balance end\n", this->shuffle_times);
            }
        }

        LOG_PRINT(DBG_COMM, "Comm: exchange KV. (send count=%ld, recv count=%ld, done count=%d, peakmem=%ld)\n",
                  sendcount, recvcount, this->done_count, peakmem);

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
