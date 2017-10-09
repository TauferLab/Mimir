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
        unit_size = MEMPAGE_SIZE;
        while ((int64_t)unit_size * (int64_t) this->shuffle_size > COMM_BUF_SIZE) {
            unit_size /= 2;
        }
        if (unit_size < 64) {
            LOG_ERROR("The communication buffer is too small (%d*%d)!",
                      unit_size, this->shuffle_size);
        }
        //if (COMM_BUF_SIZE < (int64_t) COMM_UNIT_SIZE * (int64_t) this->shuffle_size) {
        //    LOG_ERROR("Error: send buffer(%ld) should be larger than COMM_UNIT_SIZE(%d)*size(%d).\n",
        //              COMM_BUF_SIZE, COMM_UNIT_SIZE, this->shuffle_size);
        //}
        buf_size = (COMM_BUF_SIZE / unit_size / this->shuffle_size) * unit_size;
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

	migrate_kvs();

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
            if (BALANCE_LOAD && !(this->user_hash) && !(this->ismigrate)) {
                this->record_bin_info(key, ret);
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

    void perform_repartition() {
        PROFILER_RECORD_TIME_START;
        bool flag = check_load_balance(); 
        PROFILER_RECORD_TIME_END(TIMER_LB_CHECK);
        if (!flag) {
            PROFILER_RECORD_TIME_START;
            gather_counts();
            balance_load();
            PROFILER_RECORD_TIME_END(TIMER_LB_RP);
            if (this->split_hint) {
                PROFILER_RECORD_TIME_START;
                split_keys(); 
                PROFILER_RECORD_TIME_END(TIMER_LB_SPLIT);
            }
        } 
    }

    // if there is load imbanace problem, return false
    bool check_load_balance() {
        if (!(this->migratable)) return true;

        int64_t min_kv_count = 0x7fffffffffffffff, max_kv_count = 0;
        int64_t min_unique_count = 0x7fffffffffffffff, max_unique_count = 0;

        if (!(this->out_combiner)) {
            MPI_Allreduce(&(this->local_kv_count), &min_kv_count, 1,
                          MPI_INT64_T, MPI_MIN, this->shuffle_comm);
            MPI_Allreduce(&(this->local_kv_count), &max_kv_count, 1,
                          MPI_INT64_T, MPI_MAX, this->shuffle_comm);
         } else {
            MPI_Allreduce(&(this->local_unique_count), &min_unique_count, 1,
                          MPI_INT64_T, MPI_MIN, this->shuffle_comm);
            MPI_Allreduce(&(this->local_unique_count), &max_unique_count, 1,
                          MPI_INT64_T, MPI_MAX, (this->shuffle_comm));
        }

        if (!(this->out_combiner)) {
            if (max_kv_count < 1024) return true;
            if ((double)max_kv_count > BALANCE_FACTOR * (double)min_kv_count)
                return false;
        } else {
            if (max_unique_count < 1024) return true;
            if ((double)max_unique_count > BALANCE_FACTOR * (double)min_unique_count)
                return false;
        }

        return true;
    }

    void gather_counts() {

        if (!(this->out_combiner)) {
            MPI_Gather(&(this->local_kv_count), 1, MPI_INT64_T,
                   this->kv_per_core, 1, MPI_INT64_T, 0, this->shared_comm);
            if (this->shared_rank == 0) {
                MPI_Allgatherv(this->kv_per_core, this->shared_size, MPI_INT64_T,
                            this->kv_per_proc, this->proc_map_count, this->proc_map_off,
                            MPI_INT64_T, this->node_comm);
            }
        } else {
            MPI_Gather(&(this->local_unique_count), 1, MPI_INT64_T,
                       this->kv_per_core, 1, MPI_INT64_T, 0, this->shared_comm);
            if (this->shared_rank == 0) {
                MPI_Allgatherv(this->kv_per_core, this->shared_size, MPI_INT64_T,
                               this->unique_per_proc, this->proc_map_count, this->proc_map_off,
                               MPI_INT64_T, this->node_comm);
            }
        }
        MPI_Barrier(this->shared_comm);

        this->global_kv_count = this->global_unique_count = 0;
        int i = 0;
        for (i = 0 ; i < this->shuffle_size; i++) {
            if (!(this->out_combiner)) this->global_kv_count += this->kv_per_proc[i];
            else this->global_unique_count += this->unique_per_proc[i];
        }

    }

    void balance_load() {
        LOG_PRINT(DBG_GEN, "shuffle index=%d: load balance start\n",
            this->shuffle_times);

        // Get redirect bins
        std::map<uint32_t,int> redirect_bins;
        std::map<uint32_t,std::pair<uint64_t, uint64_t>> bin_counts;
        this->compute_redirect_bins(redirect_bins, bin_counts);

        for (auto iter : redirect_bins) {
            auto iter1 = this->bin_table.find(iter.first);
            if (iter1 != this->bin_table.end()) {
                this->local_kv_count -= iter1->second.first;
                this->local_unique_count -= iter1->second.second;
                this->bin_table.erase(iter1);
                //iter1->second.first = 0;
                //iter1->second.second = 0;
            } else {
                LOG_ERROR("Error!\n");
            }
        }

        LOG_PRINT(DBG_REPAR, "compute redirect bins end.\n");

        // Update redirect table
        int sendcount, recvcount;
        int recvcounts[this->shuffle_size], displs[this->shuffle_size];

        sendcount = (int)redirect_bins.size() * 24;

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

        MPI_Allgather(&sendcount, 1, MPI_INT,
                       recvcounts, 1, MPI_INT, this->shuffle_comm);

        TRACKER_RECORD_EVENT(EVENT_COMM_ALLGATHER);

        recvcount  = recvcounts[0];
        displs[0] = 0;
        for (int i = 1; i < this->shuffle_size; i++) {
            displs[i] = displs[i - 1] + recvcounts[i - 1];
            recvcount += recvcounts[i];
        }

        if (recvcount == 0) return;

        char sendbuf[sendcount], recvbuf[recvcount];
        int k = 0;
        for (auto iter : redirect_bins) {
            *(int*)(sendbuf+k) = iter.first;
            *(int*)(sendbuf+k+4) = iter.second;
	    auto iter1 = bin_counts.find(iter.first);
            if (iter1 != bin_counts.end()) {
                *(int64_t*)(sendbuf+k+8) = iter1->second.first;
                *(int64_t*)(sendbuf+k+16) = iter1->second.second;
            }
            k += 24;
        }

        MPI_Allgatherv(sendbuf, sendcount, MPI_CHAR,
                       recvbuf, recvcounts, displs, MPI_CHAR, this->shuffle_comm);

        TRACKER_RECORD_EVENT(EVENT_COMM_ALLGATHERV);
        for (int i = 0; i < recvcount; i += 24) {
            uint32_t binid = *(uint32_t*)(recvbuf + i);
            int rankid = *(int*)(recvbuf + i + 4);
            int64_t kvcount = *(int64_t*)(recvbuf + i + 8);
            int64_t ucount = *(int64_t*)(recvbuf + i + 16);
            if (rankid == this->shuffle_rank) {
                this->local_kv_count += kvcount;
                this->local_unique_count += ucount;
                auto iter = this->bin_table.find(binid);
                if (iter != this->bin_table.end()) {
                    iter->second.first += kvcount;
                    iter->second.second += ucount;
                } else {
                    this->bin_table[binid] = {kvcount,ucount};
                }
            }
            this->redirect_table[binid] = rankid;
        }

        PROFILER_RECORD_COUNT(COUNTER_REDIRECT_BINS,
                              this->redirect_table.size(), OPMAX);
        PROFILER_RECORD_COUNT(COUNTER_BALANCE_TIMES, 1, OPSUM);
        LOG_PRINT(DBG_GEN, "shuffle index=%d: load balance end\n", this->shuffle_times);
     }

     void split_keys() {

        std::unordered_set<uint32_t> suspect_table;
        auto iter = this->bin_table_flip.rbegin();
        while (iter != this->bin_table_flip.rend()) {
            if (iter->second.first == std::numeric_limits<uint32_t>::max()) {
                iter ++;
                continue;
            }
            if (iter->first * this->shuffle_size > this->global_kv_count) {
                suspect_table.insert(iter->second.first);
                LOG_PRINT(DBG_REPAR, "Find split suspect bid=%u (%ld,%lf)\n",
                          iter->second.first, iter->first,
                          (double)iter->first / (double)(this->global_kv_count));
            } else {
                break;
            }
            iter ++;
        }

        std::unordered_set<uint32_t> local_split_table;
        std::unordered_map<uint32_t, uint64_t> suspect_stat;
        if (suspect_table.size() != 0) {
            typename SafeType<KeyType>::type key[this->keycount];
            typename SafeType<ValType>::type val[this->valcount];

            this->out->seek(DB_START);
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
            for (auto iter : suspect_stat) {
                if ((double)iter.second * this->shuffle_size > 
                    (double)(this->global_kv_count) * 0.8) {
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

        int sendcount, recvcount;
        int recvcounts[this->shuffle_size], displs[this->shuffle_size];

        sendcount = (int)local_split_table.size();
        MPI_Allgather(&sendcount, 1, MPI_INT,
                       recvcounts, 1, MPI_INT, this->shuffle_comm);
        recvcount  = recvcounts[0];
        displs[0] = 0;
        for (int i = 1; i < this->shuffle_size; i++) {
            displs[i] = displs[i - 1] + recvcounts[i - 1];
            recvcount += recvcounts[i];
        }

        if (recvcount != 0) {

            int sendbuf[sendcount], recvbuf[recvcount];
            int idx = 0;
            for (auto iter : local_split_table) {
                sendbuf[idx] = iter;
                idx ++;
            }
            MPI_Allgatherv(sendbuf, sendcount, MPI_INT,
                           recvbuf, recvcounts, displs, MPI_INT, this->shuffle_comm);
            for (idx = 0; idx < recvcount; idx ++) {
                uint32_t hid = recvbuf[idx];
                uint32_t bid = hid % (uint32_t) (this->shuffle_size * BIN_COUNT);
                this->split_table.insert(hid);
                this->ignore_table.insert(bid);
                PROFILER_RECORD_COUNT(COUNTER_SPLIT_KEYS, this->split_table.size(), OPMAX);
                if (this->bin_table.find(bid) == this->bin_table.end()) {
                    this->bin_table.insert({bid, {0,0}});
                }
            }
        }
    }

    virtual BaseDatabase<KeyType,ValType>* get_tmp_db() {
        BaseDatabase<KeyType,ValType> *kv = NULL;
        kv = new KVContainer<KeyType,ValType>(this->keycount, this->valcount);
        return kv;
    }

    virtual void migrate_kvs() {
        if (this->redirect_table.size() == 0) return;

        printf("%d[%d] migrate start peakmem=%ld\n",
               this->shuffle_rank, this->shuffle_size, peakmem);

        PROFILER_RECORD_TIME_START;

        this->done_flag = 0;
        this->ismigrate = true; 

        LOG_PRINT(DBG_GEN, "migrate kvs start\n");

        BaseDatabase<KeyType,ValType> *kv = get_tmp_db();
        // Change output DB
        Writable<KeyType,ValType>* tmp_out = this->out;
        this->out = kv;

        // Migrate data
        typename SafeType<KeyType>::type key[this->keycount];
        typename SafeType<ValType>::type val[this->valcount];
        kv->open();
        this->out_reader->seek(DB_START);
        if (!(this->split_hint)) {
            while(this->out_reader->read(key,val) == true) {
                uint32_t hid = this->ser->get_hash_code(key);
                uint32_t bid = hid % (uint32_t) (this->shuffle_size * BIN_COUNT);
                auto iter = this->redirect_table.find(bid);
                if (iter != this->redirect_table.end()
                    && iter->second != this->shuffle_rank) {
                    PROFILER_RECORD_COUNT(COUNTER_MIGRATE_KVS, 1, OPSUM);
                    this->write(key, val);
                    this->out_mover->remove();
                }
           }
        } else {
            while(this->out_reader->read(key,val) == true) {
                uint32_t hid = this->ser->get_hash_code(key);
                uint32_t bid = hid % (uint32_t) (this->shuffle_size * BIN_COUNT);
                auto iter = this->redirect_table.find(bid);
                if (iter != this->redirect_table.end()
                    && iter->second != this->shuffle_rank) {
                    if (this->split_table.find(hid) == this->split_table.end()) {
                        PROFILER_RECORD_COUNT(COUNTER_MIGRATE_KVS, 1, OPSUM);
                        this->write(key, val);
                        this->out_mover->remove();
                    }
                }
           }
        }
        this->wait();

        // Copy back
        this->out = tmp_out;
        this->out->close();
        this->out->open();
        kv->seek(DB_START);
        while(kv->read(key,val) == true) {
            this->out->write(key, val);
        }
        kv->close();
        delete kv;
        PROFILER_RECORD_TIME_END(TIMER_LB_MIGRATE);

        printf("%d[%d] migrate end peakmem=%ld\n",
               this->shuffle_rank, this->shuffle_size, peakmem);

        this->ismigrate = false;

        LOG_PRINT(DBG_GEN, "migrate kvs end\n");
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
                if (BALANCE_LOAD && !(this->user_hash) && !(this->ismigrate)) {
                    this->record_bin_info(key, ret);
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

        LOG_PRINT(DBG_COMM, "Comm: start alltoall (peakmem=%ld)\n", peakmem);

        if (this->done_flag) {
            for (int i = 0; i < this->shuffle_size; i++) {
                send_offset[i] = -1;
            }
        }

        if (!(this->ismigrate)) PROFILER_RECORD_TIME_START;
        MPI_Alltoall(send_offset, 1, MPI_INT,
                     recv_count, 1, MPI_INT, this->shuffle_comm);
        if (!(this->ismigrate)) PROFILER_RECORD_TIME_END(TIMER_COMM_A2A);

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

        if (!(this->ismigrate)) PROFILER_RECORD_TIME_START;
        MPI_Alltoallv(send_buffer, a2a_s_count, a2a_s_displs, comm_type,
                      recv_buffer, a2a_r_count, a2a_r_displs, comm_type, 
                      this->shuffle_comm);
        if (!(this->ismigrate)) PROFILER_RECORD_TIME_END(TIMER_COMM_A2AV);

        TRACKER_RECORD_EVENT(EVENT_COMM_ALLTOALLV);

        LOG_PRINT(DBG_COMM, "Comm: receive data. (count=%ld)\n", recvcount);

        PROFILER_RECORD_COUNT(COUNTER_RECV_BYTES, (uint64_t) recvcount, OPSUM);
        if (recvcount > 0) {
            save_data();
        }

        for (int i = 0; i < this->shuffle_size; i++) send_offset[i] = 0;

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

        PROFILER_RECORD_COUNT(COUNTER_SHUFFLE_TIMES, 1, OPSUM);

        if (BALANCE_LOAD && !(this->user_hash) && !(this->ismigrate) 
		&& this->shuffle_times % BALANCE_FREQ == 0) {
            perform_repartition();
        }

        LOG_PRINT(DBG_COMM, "Comm: exchange KV. (send count=%ld, recv count=%ld, done count=%d, peakmem=%ld)\n",
                  sendcount, recvcount, this->done_count, peakmem);

        this->shuffle_times += 1;
    }

    int64_t buf_size;
    int unit_size;

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
