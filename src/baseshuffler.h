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

        out_db = dynamic_cast<Removable<KeyType,ValType>*>(out);

        ser = new Serializer<KeyType, ValType>(keycount, valcount);

        MPI_Comm_rank(shuffle_comm, &shuffle_rank);
        MPI_Comm_size(shuffle_comm, &shuffle_size);
        shuffle_times = 0;

        done_flag = 0;
        done_count = 0;
	kvcount = 0;

        if (BALANCE_LOAD) {
            kv_per_proc = (uint64_t*)mem_aligned_malloc(MEMPAGE_SIZE,
                                                        sizeof(uint64_t) * shuffle_size);
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
            uint64_t one = -1;
            MPI_Allgather(&one, 1, MPI_UINT64_T,
                          kv_per_proc, 1, MPI_UINT64_T, shuffle_comm);
        } else {
            MPI_Allgather(&(local_kv_count), 1, MPI_UINT64_T,
                          kv_per_proc, 1, MPI_UINT64_T, shuffle_comm);
        }

        global_kv_count = 0;
        uint64_t min_val = 0xffffffffffffffff, max_val = 0;
        int i = 0;
        for (i = 0 ; i < shuffle_size; i++) {
            if (kv_per_proc[i] == -1) break;
            global_kv_count += kv_per_proc[i];
            if (kv_per_proc[i] <= min_val) min_val = kv_per_proc[i];
            if (kv_per_proc[i] >= max_val) max_val = kv_per_proc[i];
        }

        if (i < shuffle_size) return true;

        // Need to be updated
        if (max_val > BALANCE_FACTOR * min_val) return false;

        return true;
    }

    void compute_redirect_bins(std::vector<std::pair<uint32_t,int>> &redirect_bins) {
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
                            LOG_PRINT(DBG_REPAR, "Redirect bin %d-> P%d\n", iter->first, j);
                            iter->second = 0;
                            redirect_bins.push_back(std::make_pair(iter->first,j));
                            redirect_freq -= bin_freq;
                        }
                        if (redirect_freq < 0.01) break;
                        iter ++;
                    }
                }
            }
            i ++;
        }
        mem_aligned_free(freq_per_proc);
    }

    void balance_load() {

        // Get redirect bins
        std::vector<std::pair<uint32_t,int>> redirect_bins;
        compute_redirect_bins(redirect_bins);

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

        // Migrate data
        std::set<uint32_t> reminders;
        for (auto iter : redirect_bins) {
            reminders.insert(iter.first);
        }

        typename SafeType<KeyType>::type key[keycount];
        typename SafeType<ValType>::type val[valcount];
        int bid = 0;

        uint64_t migrate_kv_count = 0;

        assert(isrepartition == false);

        // Ensure no extrea repartition within repartition
        isrepartition = true;
        if (out_db == NULL) LOG_ERROR("Cannot convert to removable object!\n");
        while ((bid = out_db->remove(key, val, reminders)) != -1) {
            this->write(key, val);
            migrate_kv_count += 1;
        }
        this->local_kv_count -= migrate_kv_count;
        PROFILER_RECORD_COUNT(COUNTER_MIGRATE_KVS, migrate_kv_count, OPSUM);
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
    Removable<KeyType,ValType>             *out_db;
    std::unordered_map<uint32_t, int>       redirect_table;
    std::unordered_map<uint32_t, uint64_t>  bin_table;
    uint64_t                                global_kv_count;
    uint64_t                                local_kv_count;
    uint64_t                                *kv_per_proc;
    bool                                    isrepartition;
};

}
#endif
