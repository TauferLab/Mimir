/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_BASE_SHUFFLER_H
#define MIMIR_BASE_SHUFFLER_H

#include <iostream>
#include <vector>
#include <unordered_map>
#include <cassert>
#include "config.h"
#include "interface.h"
#include "hashbucket.h"
#include "serializer.h"
#include "bincontainer.h"

namespace MIMIR_NS {

template <typename KeyType, typename ValType>
class BaseShuffler : public Writable<KeyType, ValType> {
public:
    BaseShuffler(MPI_Comm comm,
                 Writable<KeyType, ValType> *out,
                 int (*user_hash)(KeyType* key, ValType* val, int npartition),
                 int keycount, int valcount) {

        if (out == NULL) LOG_ERROR("Output shuffler cannot be NULL!\n");

        this->shuffle_comm = comm;
        this->out = out;
        this->user_hash = user_hash;
        this->keycount = keycount;
        this->valcount = valcount;

        out_db = dynamic_cast<BinContainer<KeyType,ValType>*>(out);

        ser = new Serializer<KeyType, ValType>(keycount, valcount);

        MPI_Comm_rank(shuffle_comm, &shuffle_rank);
        MPI_Comm_size(shuffle_comm, &shuffle_size);
        shuffle_times = 0;

        done_flag = 0;
        done_count = 0;
	kvcount = 0;

        if (BALANCE_LOAD) {
            kv_per_proc = (int64_t*)mem_aligned_malloc(MEMPAGE_SIZE,
                                                        sizeof(int64_t) * shuffle_size);
            //bin_recv_buf = (char*)mem_aligned_malloc(MEMPAGE_SIZE, MAX_RECORD_SIZE);
            this->local_kv_count = 0;
            this->global_kv_count = 0;
            for (int i = 0; i < shuffle_size; i++) {
                kv_per_proc[i] = 0;
            }
            for (int i = 0; i < BIN_COUNT; i++) {
                bin_table.insert({shuffle_rank+i*shuffle_size, 0});
            }
        }
        isrepartition = false;
    }

    virtual ~BaseShuffler() {
        delete ser;
        if (BALANCE_LOAD) {
            mem_aligned_free(kv_per_proc);
            //mem_aligned_free(bin_recv_buf);
        }
    }

    virtual int open() = 0;
    virtual int write(KeyType *key, ValType *val) = 0;
    virtual void close() = 0;
    virtual void make_progress(bool issue_new = false) = 0;
    virtual uint64_t get_record_count() { return kvcount; }

protected:

    int get_target_rank(KeyType *key, ValType *val) {

        int target = 0;
        if (user_hash != NULL) {
            target = user_hash(key, val, shuffle_size) % shuffle_size;
        }
        else {
            uint32_t hid = ser->get_hash_code(key);
            if (!BALANCE_LOAD) {
                target = (int) (hid % (uint32_t) shuffle_size);
                //printf("%d[%d] word=%s, partitioned rank=%d\n",
                //       shuffle_rank, shuffle_size, *key, target);
            } else {
                // search item in the redirect table
                uint32_t bid = hid % (uint32_t) (shuffle_size * BIN_COUNT);
                auto iter = redirect_table.find(bid);
                // find the item in the redirect table
                if (iter != redirect_table.end()) {
                    target = iter->second;
                } else {
                    target = (int) (hid % (uint32_t) shuffle_size);
                }
            }
        }
        if (target < 0 || target >= shuffle_size) {
            LOG_ERROR("Error: target process (%d) isn't correct!\n", target);
        }

        return target;
    }

    bool check_load_balance() {

        if (out_db == NULL) return true;

        // Not allowed to perform load balance
        if (this->isrepartition || this->done_flag) {
            int64_t one = -1;
            MPI_Allgather(&one, 1, MPI_INT64_T,
                          kv_per_proc, 1, MPI_INT64_T, shuffle_comm);
        } else {
            MPI_Allgather(&(local_kv_count), 1, MPI_INT64_T,
                          kv_per_proc, 1, MPI_INT64_T, shuffle_comm);
        }

        global_kv_count = 0;
        int64_t min_val = 0xffffffffffffffff, max_val = 0;
        int i = 0;
        for (i = 0 ; i < shuffle_size; i++) {
            if (kv_per_proc[i] == -1) break;
            global_kv_count += kv_per_proc[i];
            if (kv_per_proc[i] <= min_val) min_val = kv_per_proc[i];
            if (kv_per_proc[i] >= max_val) max_val = kv_per_proc[i];
        }

        if (i < shuffle_size) return true;

        // Need to be updated
        if ((double)max_val > BALANCE_FACTOR * (double)min_val) return false;

        return true;
    }

    void compute_redirect_bins(std::map<uint32_t,int> &redirect_bins,
                               std::set<int> &send_procs,
                               std::set<int> &recv_procs,
                               uint64_t &migrate_kv_count) {

        // Get redirect bins
        double *freq_per_proc = (double*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(double) * shuffle_size);
        for (int i = 0; i < shuffle_size; i++) {
            double freq = (double)kv_per_proc[i] / (double)global_kv_count * shuffle_size;
            freq_per_proc[i] = freq;
        }
        int i = 0, j = 0;
        while (i < shuffle_size && j < shuffle_size) {
            while (freq_per_proc[i] > 1.01) {
                while (freq_per_proc[j] > 0.99 && j < shuffle_size) j++;
                if (j >= shuffle_size) break;
                double redirect_freq = 0.0;
                if (1.0 - freq_per_proc[j] < freq_per_proc[i] - 1.0) {
                    redirect_freq = 1.0 - freq_per_proc[j];
                    freq_per_proc[i] -= redirect_freq;
                    freq_per_proc[j] = 1.0;
                } else {
                    redirect_freq = freq_per_proc[i] - 1.0;
                    freq_per_proc[j] += redirect_freq;
                    freq_per_proc[i] = 1.0;
                }
                if (i == shuffle_rank) {
                    auto iter = bin_table.begin();
                    while (iter != bin_table.end()) {
                        if (iter->second == 0) {
                            iter++;
                            continue;
                        }
                        double bin_freq = (double)iter->second / (double)local_kv_count * freq_per_proc[shuffle_rank];
                        if (bin_freq < redirect_freq) {
                            LOG_PRINT(DBG_REPAR, "Redirect bin %d-> P%d (%ld, %.6lf)\n",
                                      iter->first, j, iter->second, bin_freq);
                            migrate_kv_count += iter->second;
                            iter->second = 0;
                            redirect_bins[iter->first] = j;
                            redirect_freq -= bin_freq;
                        }
                        if (redirect_freq < 0.01) break;
                        iter ++;
                    }
                    printf("%d[%d] send to %d\n", shuffle_rank, shuffle_size, j);
                    send_procs.insert(j);
                }
                if (j == shuffle_rank) {
                    printf("%d[%d] recv from %d\n", shuffle_rank, shuffle_size, i);
                    recv_procs.insert(i);
                }
            }
            i ++;
        }
        mem_aligned_free(freq_per_proc);
        this->local_kv_count -= migrate_kv_count;

        PROFILER_RECORD_COUNT(COUNTER_MIGRATE_KVS, migrate_kv_count, OPSUM);
    }

    void balance_load() {

        // Get redirect bins
        std::map<uint32_t,int> redirect_bins;
        std::set<int> send_procs, recv_procs;
        uint64_t migrate_kv_count = 0;

        compute_redirect_bins(redirect_bins, send_procs,
                              recv_procs, migrate_kv_count);

        // Update redirect table
        int sendcount, recvcount;
        int recvcounts[shuffle_size], displs[shuffle_size];

        sendcount = (int)redirect_bins.size() * 2;

        MPI_Allgather(&sendcount, 1, MPI_INT,
                       recvcounts, 1, MPI_INT, shuffle_comm);

        recvcount  = recvcounts[0];
        displs[0] = 0;
        for (int i = 1; i < shuffle_size; i++) {
            displs[i] = displs[i - 1] + recvcounts[i - 1];
            recvcount += recvcounts[i];
        }

        int sendbuf[sendcount], recvbuf[recvcount];
        int k = 0;
        for (auto iter : redirect_bins) {
            sendbuf[2*k] = iter.first;
            sendbuf[2*k+1] = iter.second;
            bin_table.erase(iter.first);
            k ++;
        }

        MPI_Allgatherv(sendbuf, sendcount, MPI_INT,
                       recvbuf, recvcounts, displs, MPI_INT, shuffle_comm);
        for (int i = 0; i < recvcount/2; i++) {
            uint32_t binid = recvbuf[2*i];
            int rankid = recvbuf[2*i+1];
            redirect_table[binid] = rankid;
            if (rankid == shuffle_rank) bin_table[binid] = 0;
        }

        PROFILER_RECORD_COUNT(COUNTER_REDIRECT_BINS, redirect_table.size(), OPMAX);

        assert(isrepartition == false);

        // Ensure no extrea repartition within repartition
        isrepartition = true;
        if (!out_db || (recv_procs.size() > 0 && send_procs.size() > 0)) {
            LOG_ERROR("Cannot convert to removable object!\n");
        }

        printf("%d[%d] send counts=%ld, recv_counts=%ld\n",\
               shuffle_rank, shuffle_size, 
               send_procs.size(), recv_procs.size());

        // receive message
        int recv_count = (int)recv_procs.size();
        while ( recv_count > 0) {
            MPI_Request req;
            MPI_Status st;
            int flag;
            int count;

            int bidx = out_db->get_empty_bin();
            char *ptr = out_db->get_bin_ptr(bidx);
            int usize = out_db->get_unit_size();

            MPI_Irecv(ptr, usize, MPI_BYTE, MPI_ANY_SOURCE, 
                      LB_MIGRATE_TAG, shuffle_comm, &req);

            flag = 0;
            while (!flag) MPI_Test(&req, &flag, &st);

            MPI_Get_count(&st, MPI_BYTE, &count);

            if (recv_procs.find(st.MPI_SOURCE) == recv_procs.end()) {
                LOG_ERROR("%d recv message from %d\n",
                          shuffle_rank, st.MPI_SOURCE);
            }

            if (count == 0) {
                recv_count --;
                continue;
            }

            typename SafeType<KeyType>::ptrtype key = NULL;
            typename SafeType<ValType>::ptrtype val = NULL;

            int off = 0;
            int kvcount = 0;
            uint32_t bintag = 0;
            while (off < count) {
                int kvsize = this->ser->kv_from_bytes(&key, &val,
                                                      ptr + off, count - off);
                bintag = ser->get_hash_code(key) % (uint32_t) (shuffle_size * BIN_COUNT);

                //int ret = this->out->write(key, val);
                //if (ret == 1) {
                auto iter = this->bin_table.find(bintag);
                if (iter != this->bin_table.end()) {
                    iter->second += 1;
                    this->local_kv_count += 1;
                } else {
                    LOG_ERROR("Wrong bin index=%d\n", bintag);
                }
                //}

                off += kvsize;
                kvcount += 1;
            }

            if (off != count) {
                LOG_ERROR("Error in processing the repartition KVs! off=%d, count=%d\n",
                          off, count);
            }

            out_db->set_bin_info(bidx, bintag, count, kvcount);
        }

        if (send_procs.size() > 0) {
            char *buffer = NULL;
            uint32_t bintag = 0;
            int bidx = 0, datasize = 0, kvcount = 0;
            MPI_Request reqs[32];
            MPI_Status sts[32];
            int idx = 0;

            for (int i = 0; i < 32; i++) reqs[i] = MPI_REQUEST_NULL;

            uint64_t total_kvcount = 0;

            while ((bidx = out_db->get_next_bin(buffer, datasize, bintag, kvcount)) != -1) {
                if (datasize == 0) continue;
                auto iter = redirect_bins.find(bintag);
                if (iter == redirect_bins.end()) continue;
                int dst = iter->second;
                out_db->set_bin_info(bidx, 0, 0, 0);

                total_kvcount += kvcount;

                if (bin_table.find(bintag) != bin_table.end()) {
                    LOG_ERROR("Still can find bin %d in the send procs!\n", bintag);
                }

                MPI_Isend(buffer, datasize, MPI_BYTE, dst,
                          LB_MIGRATE_TAG, shuffle_comm, &reqs[idx++]);

                printf("%d[%d] %d Send: %d->%d %d bintag=%d, count=%d\n",
                       shuffle_rank, shuffle_size, shuffle_times, 
                       shuffle_rank, dst, datasize, bintag, kvcount);

                if (idx % 32 == 0) {
                    MPI_Waitall(32, reqs, sts);
                    for (int i = 0; i < 32; i++) reqs[i] = MPI_REQUEST_NULL;
                    idx = 0;
                }
            }
            MPI_Waitall(32, reqs, sts);
            for (int i = 0; i < 32; i++) reqs[i] = MPI_REQUEST_NULL;

            idx = 0;
            for (auto iter : send_procs) {
                int dst = iter;
                MPI_Isend(buffer, 0, MPI_BYTE, dst,
                          LB_MIGRATE_TAG, shuffle_comm, &reqs[idx++]);

                printf("%d[%d] %d Send 0: %d->%d\n",
                       shuffle_rank, shuffle_size, shuffle_times, shuffle_rank, dst);

                if (idx % 32 == 0) {
                    MPI_Waitall(32, reqs, sts);
                    for (int i = 0; i < 32; i++) reqs[i] = MPI_REQUEST_NULL;
                    idx = 0;
                }
            }
            MPI_Waitall(32, reqs, sts);
            for (int i = 0; i < 32; i++) reqs[i] = MPI_REQUEST_NULL;

            if (total_kvcount != migrate_kv_count) {
                LOG_ERROR("total kvcount=%ld, migrate kvcount=%ld\n",
                          total_kvcount, migrate_kv_count);
            }
        }

        isrepartition = false;
    }

    int (*user_hash)(KeyType* key, ValType* val, int npartition);
    Writable<KeyType,ValType> *out;

    Serializer<KeyType, ValType> *ser;

    int done_flag, done_count;

    uint64_t kvcount;

    MPI_Comm shuffle_comm;
    int      shuffle_rank;
    int      shuffle_size;
    int      shuffle_times;

    int      keycount, valcount;

    // redirect <key,value> to other processes
    BinContainer<KeyType,ValType>          *out_db;
    std::unordered_map<uint32_t, int>       redirect_table;
    std::unordered_map<uint32_t, uint64_t>  bin_table;
    uint64_t                                global_kv_count;
    uint64_t                                local_kv_count;
    int64_t                                *kv_per_proc;
    bool                                    isrepartition;
    //char                                   *bin_recv_buf;
};

}
#endif
