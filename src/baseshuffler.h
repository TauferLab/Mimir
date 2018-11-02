//
// (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego
//     Supercomputer Center, National University of Defense Technology,
//     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
//
//     See COPYRIGHT in top-level directory.
//

#ifndef MIMIR_BASE_SHUFFLER_H
#define MIMIR_BASE_SHUFFLER_H

#include <iostream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <cassert>
#include <random>
#include "config.h"
#include "interface.h"
#include "hashbucket.h"
#include "serializer.h"
#include "bincontainer.h"
#include "kvcontainer.h"

namespace MIMIR_NS {

template <typename KeyType, typename ValType>
class BaseShuffler : public Writable<KeyType, ValType>
{
  public:
    BaseShuffler(MPI_Comm comm, Writable<KeyType, ValType> *out,
                 int (*user_hash)(KeyType *key, ValType *val, int npartition),
                 int keycount, int valcount, bool split_hint, HashBucket<> *h)
    {
        if (out == NULL) LOG_ERROR("Output shuffler cannot be NULL!\n");

        this->shuffle_comm = comm;
        this->out = out;
        this->user_hash = user_hash;
        this->keycount = keycount;
        this->valcount = valcount;

        this->ismigrate = false;

        out_reader = dynamic_cast<Readable<KeyType, ValType> *>(out);
        out_mover = dynamic_cast<Removable<KeyType, ValType> *>(out);
        out_combiner = dynamic_cast<Combinable<KeyType, ValType> *>(out);

        if (out_reader != NULL && out_mover != NULL) {
            migratable = true;
        }
        else {
            migratable = false;
        }

        ser = new Serializer<KeyType, ValType>(keycount, valcount);

        MPI_Comm_rank(shuffle_comm, &shuffle_rank);
        MPI_Comm_size(shuffle_comm, &shuffle_size);
        shuffle_times = 0;

        done_flag = 0;
        done_count = 0;
        kvcount = 0;

        this->split_hint = split_hint;
        this->h = h;

        if (BALANCE_LOAD) {
            if (split_hint) {
                std::random_device rd;
                gen = new std::minstd_rand(rd());
                d = new std::uniform_int_distribution<>(0, shuffle_size - 1);
            }

            // Split communicator on shared-memory node
            MPI_Comm_split_type(shuffle_comm, MPI_COMM_TYPE_SHARED,
                                shuffle_rank, MPI_INFO_NULL, &shared_comm);
            MPI_Comm_rank(shared_comm, &shared_rank);
            MPI_Comm_size(shared_comm, &shared_size);

            // Get groups
            MPI_Comm_group(shuffle_comm, &shuffle_group);
            MPI_Comm_group(shared_comm, &shared_group);

            // Get node communicator
            MPI_Comm_split(shuffle_comm, shared_rank, shuffle_rank, &node_comm);
            MPI_Comm_rank(node_comm, &node_rank);
            MPI_Comm_size(node_comm, &node_size);
            MPI_Bcast(&node_rank, 1, MPI_INT, 0, shared_comm);
            MPI_Bcast(&node_size, 1, MPI_INT, 0, shared_comm);

            // KV per proc
            if (shared_rank == 0) {
                MPI_Win_allocate_shared(
                    sizeof(int64_t) * shuffle_size, sizeof(int64_t),
                    MPI_INFO_NULL, shared_comm, &kv_per_proc, &kv_proc_win);
                MPI_Win_allocate_shared(sizeof(int64_t) * shuffle_size,
                                        sizeof(int64_t), MPI_INFO_NULL,
                                        shared_comm, &unique_per_proc,
                                        &unique_proc_win);
                MPI_Win_allocate_shared(
                    sizeof(int64_t) * shared_size, sizeof(int64_t),
                    MPI_INFO_NULL, shared_comm, &kv_per_core, &kv_core_win);
                MPI_Win_allocate_shared(sizeof(int) * (node_size + 1),
                                        sizeof(int), MPI_INFO_NULL, shared_comm,
                                        &proc_map_off, &map_off_win);
                MPI_Win_allocate_shared(sizeof(int) * node_size, sizeof(int),
                                        MPI_INFO_NULL, shared_comm,
                                        &proc_map_count, &map_count_win);
                MPI_Win_allocate_shared(sizeof(int) * shuffle_size, sizeof(int),
                                        MPI_INFO_NULL, shared_comm,
                                        &proc_map_rank, &map_rank_win);
            }
            else {
                MPI_Aint tmp_size;
                int tmp_unit;
                MPI_Win_allocate_shared(0, sizeof(int64_t), MPI_INFO_NULL,
                                        shared_comm, &kv_per_proc,
                                        &kv_proc_win);
                MPI_Win_shared_query(kv_proc_win, 0, &tmp_size, &tmp_unit,
                                     &kv_per_proc);
                MPI_Win_allocate_shared(0, sizeof(int64_t), MPI_INFO_NULL,
                                        shared_comm, &unique_per_proc,
                                        &unique_proc_win);
                MPI_Win_shared_query(unique_proc_win, 0, &tmp_size, &tmp_unit,
                                     &unique_per_proc);
                MPI_Win_allocate_shared(0, sizeof(int64_t), MPI_INFO_NULL,
                                        shared_comm, &kv_per_core,
                                        &kv_core_win);
                MPI_Win_shared_query(kv_core_win, 0, &tmp_size, &tmp_unit,
                                     &kv_per_core);
                MPI_Win_allocate_shared(0, sizeof(int), MPI_INFO_NULL,
                                        shared_comm, &proc_map_off,
                                        &map_off_win);
                MPI_Win_shared_query(map_off_win, 0, &tmp_size, &tmp_unit,
                                     &proc_map_off);
                MPI_Win_allocate_shared(0, sizeof(int), MPI_INFO_NULL,
                                        shared_comm, &proc_map_count,
                                        &map_count_win);
                MPI_Win_shared_query(map_count_win, 0, &tmp_size, &tmp_unit,
                                     &proc_map_count);
                MPI_Win_allocate_shared(0, sizeof(int), MPI_INFO_NULL,
                                        shared_comm, &proc_map_rank,
                                        &map_rank_win);
                MPI_Win_shared_query(map_rank_win, 0, &tmp_size, &tmp_unit,
                                     &proc_map_rank);
            }

            if (shared_rank == 0) {
                MPI_Allgather(&shared_size, 1, MPI_INT, proc_map_count, 1,
                              MPI_INT, node_comm);
                proc_map_off[0] = 0;
                for (int i = 0; i < node_size; i++) {
                    proc_map_off[i + 1] = proc_map_off[i] + proc_map_count[i];
                }
                int shared_ranks[shared_size];
                int shuffle_ranks[shared_size];
                for (int i = 0; i < shared_size; i++) shared_ranks[i] = i;
                MPI_Group_translate_ranks(shared_group, shared_size,
                                          shared_ranks, shuffle_group,
                                          shuffle_ranks);
                MPI_Allgatherv(shuffle_ranks, shared_size, MPI_INT,
                               proc_map_rank, proc_map_count, proc_map_off,
                               MPI_INT, node_comm);
                for (int i = 0; i < shuffle_size; i++) {
                    kv_per_proc[i] = 0;
                    unique_per_proc[i] = 0;
                }
            }

            this->local_kv_count = 0;
            this->global_kv_count = 0;
            this->local_unique_count = 0;
            this->global_unique_count = 0;
            for (int i = 0; i < BIN_COUNT; i++) {
                bin_table.insert({shuffle_rank + i * shuffle_size, {0, 0}});
            }
        }
    }

    virtual ~BaseShuffler()
    {
        if (BALANCE_LOAD) {
            if (split_hint) {
                // Allgather the split keys
                int sendcount, recvcount;
                int recvcounts[shuffle_size], displs[shuffle_size];
                HashBucket<>::HashEntry *entry = NULL;

                // Get send size
                sendcount = 0;
                h->open();
                while ((entry = h->next()) != NULL) {
                    sendcount += entry->keysize;
                }
                h->close();

                MPI_Allgather(&sendcount, 1, MPI_INT, recvcounts, 1, MPI_INT,
                              shuffle_comm);

                // Get recv size
                recvcount = recvcounts[0];
                displs[0] = 0;
                for (int i = 1; i < shuffle_size; i++) {
                    displs[i] = displs[i - 1] + recvcounts[i - 1];
                    recvcount += recvcounts[i];
                }

                if (recvcount != 0) {
                    // Get send data
                    char sendbuf[sendcount], recvbuf[recvcount];
                    int off = 0;
                    h->open();
                    while ((entry = h->next()) != NULL) {
                        memcpy(sendbuf + off, entry->key, entry->keysize);
                        off += entry->keysize;
                    }
                    h->close();
                    if (off != sendcount) LOG_ERROR("Error!\n");
                    MPI_Allgatherv(sendbuf, sendcount, MPI_BYTE, recvbuf,
                                   recvcounts, displs, MPI_BYTE, shuffle_comm);
                    // Get recv data
                    typename SafeType<KeyType>::type key[keycount];
                    off = 0;
                    while (off < recvcount) {
                        char *keyptr = &recvbuf[0] + off;
                        int keysize
                            = ser->key_from_bytes(key, keyptr, recvcount - off);
                        if (keysize == 0) LOG_ERROR("Error!\n");
                        EmptyVal v;
                        if (h->findEntry(keyptr, keysize) == NULL) {
                            h->insertEntry(keyptr, keysize, &v);
                        }
                        off += keysize;
                    }
                }

                delete gen;
                delete d;
            }
            MPI_Group_free(&shared_group);
            MPI_Group_free(&shuffle_group);
            MPI_Comm_free(&node_comm);
            MPI_Comm_free(&shared_comm);
            MPI_Win_free(&map_off_win);
            MPI_Win_free(&map_count_win);
            MPI_Win_free(&map_rank_win);
            MPI_Win_free(&kv_proc_win);
            MPI_Win_free(&kv_core_win);
        }
        delete ser;
    }

    virtual int open() = 0;
    virtual int write(KeyType *key, ValType *val) = 0;
    virtual void close() = 0;
    virtual void make_progress(bool issue_new = false) = 0;

#if 0
    virtual BaseDatabase<KeyType,ValType>* get_tmp_db() {
        return NULL;
    }
    virtual void migrate_kvs() {
	if (redirect_table.size() == 0) return;

        LOG_PRINT(DBG_GEN, "migrate kvs start\n");

	PROFILER_RECORD_TIME_START;
	BaseDatabase<KeyType,ValType> *kv = get_tmp_db();
        // Change output DB
	Writable<KeyType,ValType>* tmp_out = out;
        out = kv;

        // Migrate data
        typename SafeType<KeyType>::type key[keycount];
        typename SafeType<ValType>::type val[valcount];
        kv->open();
        out_reader->seek(DB_START);
	while(out_reader->read(key,val) == true) {
	    uint32_t hid = ser->get_hash_code(key);
            uint32_t bid = hid % (uint32_t) (shuffle_size * BIN_COUNT);
            auto iter = redirect_table.find(bid);
	    if (iter != redirect_table.end()
                && iter->second != shuffle_rank) {
                this->write(key, val);
            }
        }
        this->wait();

        // Copy back
        out = tmp_out;
        kv->seek(DB_START);
 	while(out_reader->read(key,val) == true) {
            this->write(key, val);
        }
        kv->close();
        delete kv;
        PROFILER_RECORD_TIME_END(TIMER_LB_MIGRATE);

        LOG_PRINT(DBG_GEN, "migrate kvs end\n");
    }
#endif

    virtual int seek(DB_POS pos)
    {
        LOG_WARNING("FileReader doesnot support seek methods!\n");
        return false;
    }
    virtual uint64_t get_record_count() { return kvcount; }

  protected:
    int get_target_rank(KeyType *key, ValType *val)
    {
        int target = 0;
        if (user_hash != NULL) {
            target = user_hash(key, val, shuffle_size) % shuffle_size;
        }
        else {
            uint32_t hid = ser->get_hash_code(key);
            if (!BALANCE_LOAD) {
                target = (int) (hid % (uint32_t) shuffle_size);
            }
            else {
                // split this key
                if (split_hint && split_table.find(hid) != split_table.end()) {
                    target = (*d)((*gen));
                    int keysize = this->ser->get_key_bytes(key);
                    char *keyptr = this->ser->get_key_ptr(key);
                    EmptyVal v;
                    if (h->findEntry(keyptr, keysize) == NULL) {
                        h->insertEntry(keyptr, keysize, &v);
                    }
                }
                else {
                    // search item in the redirect table
                    uint32_t bid = hid % (uint32_t)(shuffle_size * BIN_COUNT);
                    auto iter = redirect_table.find(bid);
                    // find the item in the redirect table
                    if (iter != redirect_table.end()) {
                        target = iter->second;
                    }
                    else {
                        target = (int) (bid % (uint32_t) shuffle_size);
                    }
                }
            }
        }
        if (target < 0 || target >= shuffle_size) {
            LOG_ERROR("Error: target process (%d) isn't correct!\n", target);
        }

        return target;
    }

    void record_bin_info(KeyType *key, int ret)
    {
        uint32_t hid = ser->get_hash_code(key);
        int bidx = (int) (hid % (uint32_t)(shuffle_size * BIN_COUNT));
        if (ret) {
            auto iter = bin_table.find(bidx);
            if (iter != bin_table.end()) {
                iter->second.first += 1;
                local_kv_count += 1;
                if (ret == 2) {
                    iter->second.second += 1;
                    local_unique_count += 1;
                }
            }
            else {
                LOG_ERROR("Wrong bin index=%d\n", bidx);
            }
        }
    }

#if 0
    bool check_load_balance() {
        if (!migratable) return true;

        PROFILER_RECORD_TIME_START;

        int64_t min_kv_count = 0x7fffffffffffffff, max_kv_count = 0;
        int64_t min_unique_count = 0x7fffffffffffffff, max_unique_count = 0;

        if (!out_combiner) {
            MPI_Allreduce(&local_kv_count, &min_kv_count, 1,
                          MPI_INT64_T, MPI_MIN, shuffle_comm);
            MPI_Allreduce(&local_kv_count, &max_kv_count, 1,
                          MPI_INT64_T, MPI_MAX, shuffle_comm);
         } else {
            MPI_Allreduce(&local_unique_count, &min_unique_count, 1,
                          MPI_INT64_T, MPI_MIN, shuffle_comm);
            MPI_Allreduce(&local_unique_count, &max_unique_count, 1,
                          MPI_INT64_T, MPI_MAX, shuffle_comm);
        }

        if (!out_combiner) {
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

        if (!out_combiner) {
            MPI_Gather(&local_kv_count, 1, MPI_INT64_T,
                    kv_per_core, 1, MPI_INT64_T, 0, shared_comm);
            if (shared_rank == 0) {
                MPI_Allgatherv(kv_per_core, shared_size, MPI_INT64_T,
                            kv_per_proc, proc_map_count, proc_map_off,
                            MPI_INT64_T, node_comm);
            }
        } else {
            MPI_Gather(&local_unique_count, 1, MPI_INT64_T,
                       kv_per_core, 1, MPI_INT64_T, 0, shared_comm);
            if (shared_rank == 0) {
                MPI_Allgatherv(kv_per_core, shared_size, MPI_INT64_T,
                               unique_per_proc, proc_map_count, proc_map_off,
                               MPI_INT64_T, node_comm);
            }
        }
        MPI_Barrier(shared_comm);

        global_kv_count = global_unique_count = 0;
        int i = 0;
        for (i = 0 ; i < shuffle_size; i++) {
            if (!out_combiner) global_kv_count += kv_per_proc[i];
            else global_unique_count += unique_per_proc[i];
        }

    }

    void print_kvs() {
        fprintf(stdout, "%d[%d] LB Info, %d, %ld\n",
                shuffle_rank, shuffle_size, shuffle_times, local_kv_count);
    }

    void print_node_kvs(const char* str) {
        int64_t memuse = get_mem_usage();
        int64_t nodemem, nodepeak;
        MPI_Reduce(&memuse, &nodemem, 1, MPI_INT64_T, MPI_SUM, 0, shared_comm);
        MPI_Reduce(&peakmem, &nodepeak, 1, MPI_INT64_T, MPI_SUM, 0, shared_comm);
        if (shared_rank == 0) {
            fprintf(stdout, "%s node=%d, kvs=%ld, nodemem=%ld, nodepeak=%ld\n",
                    str, node_rank, get_node_count(node_rank), nodemem, nodepeak);
        }
        KVContainer<KeyType,ValType>* kc = dynamic_cast<KVContainer<KeyType,ValType>*>(out);
        if (kc == NULL) LOG_ERROR("Error!\n");
        kc->print(shuffle_rank, shuffle_size);
    }
#endif

    int64_t get_node_count(int nodeid)
    {
        int64_t node_kv_count = 0;
        for (int i = 0; i < shared_size; i++) {
            int proc_idx = proc_map_rank[proc_map_off[nodeid] + i];
            node_kv_count += kv_per_proc[proc_idx];
        }
        return node_kv_count;
    }

    int64_t get_proc_count(int nodeid, int sharedid)
    {
        int proc_idx = proc_map_rank[proc_map_off[nodeid] + sharedid];
        return kv_per_proc[proc_idx];
    }

    int get_shuffle_rank(int nodeid, int sharedid)
    {
        return proc_map_rank[proc_map_off[nodeid] + sharedid];
    }

    int get_node_size(int nodeid)
    {
        return (proc_map_off[nodeid + 1] - proc_map_off[nodeid]);
    }

    int get_bin_target(uint32_t bid)
    {
        int target = 0;
        auto iter = redirect_table.find(bid);
        if (iter != redirect_table.end()) {
            target = iter->second;
        }
        else {
            target = (int) (bid % (uint32_t) shuffle_size);
        }
        return target;
    }

    uint64_t find_bins(
        std::map<uint32_t, int> &redirect_bins,
        std::map<uint32_t, std::pair<uint64_t, uint64_t>> &bin_counts,
        uint64_t redirect_count, int target)
    {
        uint64_t migrate_kv_count = 0;
        auto iter = bin_table_flip.rbegin();
        while (iter != bin_table_flip.rend()) {
            // this item has been redirected
            if (iter->second.first == std::numeric_limits<uint32_t>::max()) {
                iter++;
                continue;
            }
            // do not redirect small bins
            if (iter->first == 0) {
                break;
            }
            //if (iter->first < 1024) {
            //    break;
            //}
            // Ignore some bins
            if (split_hint
                && ignore_table.find(iter->second.first)
                       != ignore_table.end()) {
                iter++;
                continue;
            }
            if (iter->first < redirect_count) {
                LOG_PRINT(DBG_REPAR, "Redirect bin %d-> P%d (%ld, %.6lf)\n",
                          iter->second.first, target, iter->first,
                          (double) iter->first / (double) global_kv_count);
                migrate_kv_count += iter->first;
                redirect_count -= iter->first;
                redirect_bins[iter->second.first] = target;
                bin_counts[iter->second.first]
                    = {iter->first, iter->second.second};
                // make it invalid
                iter->second.first = std::numeric_limits<uint32_t>::max();
            }
            iter++;
        }

        return migrate_kv_count;
    }

    uint64_t find_bins_unique(
        std::map<uint32_t, int> &redirect_bins,
        std::map<uint32_t, std::pair<uint64_t, uint64_t>> &bin_counts,
        uint64_t redirect_count, int target)
    {
        uint64_t migrate_unique_count = 0;

        auto iter = bin_table_flip.begin();
        while (iter != bin_table_flip.end()) {
            // this item has been redirected
            if (iter->first == 0
                || iter->second.first == std::numeric_limits<uint32_t>::max()) {
                iter++;
                continue;
            }
            if (redirect_count >= iter->second.second) {
                LOG_PRINT(
                    DBG_REPAR,
                    "Redirect bin (combiner) %d-> P%d (%ld, %.6lf)\n",
                    iter->second.first, target, iter->second.second,
                    (double) iter->second.second / (double) global_kv_count);
                migrate_unique_count += iter->second.second;
                redirect_count -= iter->second.second;
                //migrate_kvs += iter->first;
                redirect_bins[iter->second.first] = target;
                bin_counts[iter->second.first]
                    = {iter->first, iter->second.second};
                // make it invalid
                iter->second.first = std::numeric_limits<uint32_t>::max();
            }
            if (redirect_count <= 0) break;
            iter++;
        }
        return migrate_unique_count;
    }

    uint64_t find_small_bins(std::map<uint32_t, int> &redirect_bins, int count,
                             int target)
    {
        uint64_t migrate_kv_count = 0;

        auto iter = bin_table_flip.begin();
        while (iter != bin_table_flip.end()) {
            // this item has been redirected
            if (iter->first == 0
                || iter->second.first == std::numeric_limits<uint32_t>::max()) {
                iter++;
                continue;
            }
            if (count > 0) {
                LOG_PRINT(DBG_REPAR,
                          "Redirect bin (small) %d-> P%d (%ld, %.6lf)\n",
                          iter->second.first, target, iter->first,
                          (double) iter->first / (double) global_kv_count);
                migrate_kv_count += iter->first;
                count--;
                redirect_bins[iter->second.first] = target;
                // make it invalid
                iter->second.first = std::numeric_limits<uint32_t>::max();
            }
            if (count <= 0) break;
            iter++;
        }
        return migrate_kv_count;
    }

    uint64_t find_node_bins(std::map<uint32_t, uint64_t> &redirect_bins,
                            uint64_t &redirect_count)
    {
        uint64_t migrate_kv_count = 0;

        auto iter = bin_table.begin();
        while (iter != bin_table.end()) {
            if (iter->second.first == 0) {
                iter++;
                continue;
            }
            if (iter->second.first < redirect_count) {
                LOG_PRINT(
                    DBG_REPAR, "Find bin %d (%ld, %.6lf)\n", iter->first,
                    iter->second.first,
                    (double) iter->second.first / (double) global_kv_count);
                redirect_bins[iter->first] = iter->second.first;
                redirect_count -= iter->second.first;
                migrate_kv_count += iter->second.first;
                iter->second.first = 0;
            }
            if (redirect_count <= 0) break;
            iter++;
        }

        return migrate_kv_count;
    }

    void prepare_redirect()
    {
        //gather_counts();
        bin_table_flip.clear();
        count_per_proc.clear();
        for (auto iter : bin_table) {
            bin_table_flip[iter.second.first]
                = {iter.first, iter.second.second};
        }
        if (!out_combiner) {
            for (int i = 0; i < shuffle_size; i++) {
                count_per_proc[kv_per_proc[i]] = i;
            }
        }
        else {
            for (int i = 0; i < shuffle_size; i++) {
                count_per_proc[unique_per_proc[i]] = i;
            }
        }
    }

    void compute_redirect_bins(
        std::map<uint32_t, int> &redirect_bins,
        std::map<uint32_t, std::pair<uint64_t, uint64_t>> &bin_counts)
    {
        //uint64_t migrate_kv_count = 0, migrate_unique_count = 0;

        prepare_redirect();

        // Balance KVs
        int64_t proc_kv_mean = global_kv_count / shuffle_size;
        if (out_combiner) proc_kv_mean = global_unique_count / shuffle_size;
        //int i = shuffle_size - 1, j = 0;
        auto iter_i = count_per_proc.rbegin();
        auto iter_j = count_per_proc.begin();
        int64_t kv_count_i = iter_i->first;
        int64_t kv_count_j = iter_j->first;
        int rank_i = iter_i->second;
        int rank_j = iter_j->second;
        while (iter_i != count_per_proc.rend() && iter_j != count_per_proc.end()
               && rank_i != rank_j) {
            if ((double) kv_count_i <= (double) proc_kv_mean * 1.01
                || (double) kv_count_j >= (double) proc_kv_mean * 0.99) {
                break;
            }
            int64_t redirect_count = 0.0;
            bool flag = true;
            if (proc_kv_mean - kv_count_j < kv_count_i - proc_kv_mean) {
                redirect_count = proc_kv_mean - kv_count_j;
                flag = true;
            }
            else {
                redirect_count = kv_count_i - proc_kv_mean;
                flag = false;
            }
            if (rank_i == shuffle_rank) {
                LOG_PRINT(
                    DBG_REPAR,
                    "Redirect proc %ld from %d[%ld] -> %d[%ld] mean=%ld\n",
                    redirect_count, rank_i, kv_count_i, rank_j, kv_count_j,
                    proc_kv_mean);
                if (!out_combiner) {
                    find_bins(redirect_bins, bin_counts, redirect_count,
                              rank_j);
                }
                else {
                    find_bins_unique(redirect_bins, bin_counts, redirect_count,
                                     rank_j);
                }
            }
            if (flag) {
                kv_count_i -= redirect_count;
                kv_count_j = proc_kv_mean;
                iter_j++;
                if (iter_j != count_per_proc.end()) {
                    kv_count_j = iter_j->first;
                    rank_j = iter_j->second;
                }
                else {
                    break;
                }
            }
            else {
                kv_count_j += redirect_count;
                kv_count_i = proc_kv_mean;
                iter_i++;
                if (iter_i != count_per_proc.rend()) {
                    kv_count_i = iter_i->first;
                    rank_i = iter_i->second;
                }
                else {
                    break;
                }
            }
        }

        //this->local_kv_count -= migrate_kv_count;
        //this->local_unique_count -= migrate_unique_count;
        //if (!out_combiner) {
        //    PROFILER_RECORD_COUNT(COUNTER_MIGRATE_KVS, migrate_kv_count, OPSUM);
        //} else {
        //    PROFILER_RECORD_COUNT(COUNTER_MIGRATE_KVS, migrate_unique_count, OPSUM);
        //}

        //return migrate_kv_count;
    }

#if 0
    void balance_load() {
        PROFILER_RECORD_TIME_START;

        LOG_PRINT(DBG_GEN, "shuffle index=%d: load balance start\n", this->shuffle_times);
        // Get redirect bins
        std::map<uint32_t,int> redirect_bins;

        compute_redirect_bins(redirect_bins);

        LOG_PRINT(DBG_REPAR, "compute redirect bins end.\n");

        // Update redirect table
        int sendcount, recvcount;
        int recvcounts[shuffle_size], displs[shuffle_size];

        sendcount = (int)redirect_bins.size() * 2;

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

        //PROFILER_RECORD_TIME_START;
        MPI_Allgather(&sendcount, 1, MPI_INT,
                       recvcounts, 1, MPI_INT, shuffle_comm);
        //PROFILER_RECORD_TIME_END(TIMER_COMM_ALLGATHER);

        TRACKER_RECORD_EVENT(EVENT_COMM_ALLGATHER);

        recvcount  = recvcounts[0];
        displs[0] = 0;
        for (int i = 1; i < shuffle_size; i++) {
            displs[i] = displs[i - 1] + recvcounts[i - 1];
            recvcount += recvcounts[i];
        }

        if (recvcount == 0) return;

        int sendbuf[sendcount], recvbuf[recvcount];
        std::set<int> send_procs, recv_procs;
        int k = 0;
        for (auto iter : redirect_bins) {
            sendbuf[2*k] = iter.first;
            sendbuf[2*k+1] = iter.second;
            k ++;
        }

        //PROFILER_RECORD_TIME_START;
        MPI_Allgatherv(sendbuf, sendcount, MPI_INT,
                       recvbuf, recvcounts, displs, MPI_INT, shuffle_comm);
        //PROFILER_RECORD_TIME_END(TIMER_COMM_ALLGATHERV);

        TRACKER_RECORD_EVENT(EVENT_COMM_ALLGATHERV);

        redirect_bins.clear();
        for (int i = 0; i < recvcount / 2; i++) {
            uint32_t binid = recvbuf[2*i];
            int rankid = recvbuf[2*i+1];
            auto iter = bin_table.find(binid);
            if (iter != bin_table.end()) {
                send_procs.insert(rankid);
                bin_table.erase(iter);
                redirect_bins[binid] = rankid;
            }
            if (rankid == shuffle_rank) {
                recv_procs.insert(get_bin_target(binid));
                bin_table[binid] = {0,0};
            }
            redirect_table[binid] = rankid;
        }

        PROFILER_RECORD_COUNT(COUNTER_REDIRECT_BINS, redirect_table.size(), OPMAX);
        PROFILER_RECORD_COUNT(COUNTER_BALANCE_TIMES, 1, OPSUM);
        LOG_PRINT(DBG_GEN, "shuffle index=%d: load balance end\n", this->shuffle_times);
        PROFILER_RECORD_TIME_END(TIMER_LB_RP);

        PROFILER_RECORD_TIME_START;
        if (split_hint) split_keys();
        PROFILER_RECORD_TIME_END(TIMER_LB_SPLIT);
     }

     void split_keys() {

        std::unordered_set<uint32_t> suspect_table;
        auto iter = bin_table_flip.rbegin();
        while (iter != bin_table_flip.rend()) {
            if (iter->second.first == std::numeric_limits<uint32_t>::max()) {
                iter ++;
                continue;
            }
            if (iter->first * shuffle_size > global_kv_count) {
                suspect_table.insert(iter->second.first);
                LOG_PRINT(DBG_REPAR, "Find split suspect bid=%u (%ld,%lf)\n",
                          iter->second.first, iter->first,
                          (double)iter->first / (double)global_kv_count);
            } else {
                break;
            }
            iter ++;
        }

        std::unordered_set<uint32_t> local_split_table;
        std::unordered_map<uint32_t, uint64_t> suspect_stat;
	if (suspect_table.size() != 0) {
	    typename SafeType<KeyType>::type key[keycount];
            typename SafeType<ValType>::type val[valcount];

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
                if ((double)iter.second * shuffle_size > (double)global_kv_count * 0.8) {
                    uint32_t hid = iter.first;
                    uint32_t bid = hid % (uint32_t)(shuffle_size * BIN_COUNT);
                    if (split_table.find(hid) == split_table.end())
                    {
                        LOG_PRINT(DBG_REPAR, "Find split key hid=%u, bid=%u\n", hid, bid);
                        local_split_table.insert(hid);
                    }
                }
            }

        }

        int sendcount, recvcount;
        int recvcounts[shuffle_size], displs[shuffle_size];

        sendcount = (int)local_split_table.size();
        MPI_Allgather(&sendcount, 1, MPI_INT,
                       recvcounts, 1, MPI_INT, shuffle_comm);
        recvcount  = recvcounts[0];
        displs[0] = 0;
        for (int i = 1; i < shuffle_size; i++) {
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
                           recvbuf, recvcounts, displs, MPI_INT, shuffle_comm);
            for (idx = 0; idx < recvcount; idx ++) {
                uint32_t hid = recvbuf[idx];
                uint32_t bid = hid % (uint32_t) (shuffle_size * BIN_COUNT);
                split_table.insert(hid);
                ignore_table.insert(bid);
                PROFILER_RECORD_COUNT(COUNTER_SPLIT_KEYS, split_table.size(), OPMAX);
                if (bin_table.find(bid) == bin_table.end()) {
                    bin_table.insert({bid, {0,0}});
                }
            }
        }
    }
#endif

    int (*user_hash)(KeyType *key, ValType *val, int npartition);
    Writable<KeyType, ValType> *out;
    bool migratable;

    Serializer<KeyType, ValType> *ser;

    int done_flag, done_count;

    uint64_t kvcount;

    MPI_Comm shuffle_comm;
    int shuffle_rank;
    int shuffle_size;
    int shuffle_times;

    int keycount, valcount;

    MPI_Comm shared_comm, node_comm;
    MPI_Group shared_group, shuffle_group;
    int shared_rank, shared_size, node_rank, node_size;
    int64_t *kv_per_proc, *unique_per_proc, *kv_per_core;

    int *proc_map_off, *proc_map_count, *proc_map_rank;
    MPI_Win kv_proc_win, unique_proc_win, kv_core_win;
    MPI_Win map_off_win, map_count_win, map_rank_win;

    Readable<KeyType, ValType> *out_reader;
    Removable<KeyType, ValType> *out_mover;
    Combinable<KeyType, ValType> *out_combiner;
    std::unordered_map<uint32_t, int> redirect_table;
    std::unordered_map<uint32_t, std::pair<uint64_t, uint64_t>> bin_table;
    std::map<uint64_t, std::pair<uint32_t, uint64_t>> bin_table_flip;
    std::map<int64_t, int> count_per_proc;
    std::unordered_set<uint32_t> split_table;
    std::unordered_set<uint32_t> ignore_table;
    uint64_t global_kv_count;
    uint64_t local_kv_count;
    uint64_t global_unique_count;
    uint64_t local_unique_count;
    bool ismigrate;
    bool split_hint;
    std::minstd_rand *gen;
    std::uniform_int_distribution<> *d;
    HashBucket<> *h;
};

} // namespace MIMIR_NS
#endif
