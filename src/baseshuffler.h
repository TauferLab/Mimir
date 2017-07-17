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

            // Split communicator on shared-memory node
            MPI_Comm_split_type(shuffle_comm, MPI_COMM_TYPE_SHARED, shuffle_rank,
                                MPI_INFO_NULL, &shared_comm);
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
                MPI_Win_allocate_shared(sizeof(int64_t)*shuffle_size,
                                        sizeof(int64_t), MPI_INFO_NULL,
                                        shared_comm, &kv_per_proc, &kv_proc_win);
                MPI_Win_allocate_shared(sizeof(int64_t)*shared_size,
                                        sizeof(int64_t), MPI_INFO_NULL,
                                        shared_comm, &kv_per_core, &kv_core_win);
                MPI_Win_allocate_shared(sizeof(int)*(node_size+1),
                                        sizeof(int), MPI_INFO_NULL,
                                        shared_comm, &proc_map_off, &map_off_win);
                MPI_Win_allocate_shared(sizeof(int)*node_size,
                                        sizeof(int), MPI_INFO_NULL,
                                        shared_comm, &proc_map_count, &map_count_win);
                MPI_Win_allocate_shared(sizeof(int)*shuffle_size,
                                        sizeof(int), MPI_INFO_NULL,
                                        shared_comm, &proc_map_rank, &map_rank_win);
            } else {
                MPI_Aint tmp_size;
                int tmp_unit;
                MPI_Win_allocate_shared(0, sizeof(int64_t), MPI_INFO_NULL,
                                        shared_comm, &kv_per_proc, &kv_proc_win);
                MPI_Win_shared_query(kv_proc_win, 0, &tmp_size, &tmp_unit, &kv_per_proc);
                MPI_Win_allocate_shared(0, sizeof(int64_t), MPI_INFO_NULL,
                                        shared_comm, &kv_per_core, &kv_core_win);
                MPI_Win_shared_query(kv_core_win, 0, &tmp_size, &tmp_unit, &kv_per_core);
                MPI_Win_allocate_shared(0, sizeof(int), MPI_INFO_NULL,
                                        shared_comm, &proc_map_off, &map_off_win);
                MPI_Win_shared_query(map_off_win, 0, &tmp_size, &tmp_unit, &proc_map_off);
                MPI_Win_allocate_shared(0, sizeof(int), MPI_INFO_NULL,
                                        shared_comm, &proc_map_count, &map_count_win);
                MPI_Win_shared_query(map_count_win, 0, &tmp_size, &tmp_unit, &proc_map_count);
                MPI_Win_allocate_shared(0, sizeof(int), MPI_INFO_NULL,
                                        shared_comm, &proc_map_rank, &map_rank_win);
                MPI_Win_shared_query(map_rank_win, 0, &tmp_size, &tmp_unit, &proc_map_rank);
            }

            if (shared_rank == 0) {
                MPI_Allgather(&shared_size, 1, MPI_INT,
                              proc_map_count, 1, MPI_INT, node_comm);
                proc_map_off[0] = 0;
                for (int i = 0; i < node_size; i ++) {
                    proc_map_off[i+1] = proc_map_off[i] + proc_map_count[i];
                }
                int shared_ranks[shared_size];
                int shuffle_ranks[shared_size];
                for (int i = 0; i < shared_size; i++) shared_ranks[i] = i;
                MPI_Group_translate_ranks(shared_group, shared_size, shared_ranks,
                                          shuffle_group, shuffle_ranks);
                MPI_Allgatherv(shuffle_ranks, shared_size, MPI_INT,
                               proc_map_rank, proc_map_count, proc_map_off,
                               MPI_INT, node_comm);
            }

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
            kv_per_core[shared_rank] = -1;
            MPI_Barrier(shared_comm);
            if (shared_rank == 0) {
                MPI_Allgatherv(kv_per_core, shared_size, MPI_INT64_T,
                               kv_per_proc, proc_map_count, proc_map_off,
                               MPI_INT64_T, node_comm);
            }
        } else {
            kv_per_core[shared_rank] = local_kv_count;
            MPI_Barrier(shared_comm);
            if (shared_rank == 0) {
                MPI_Allgatherv(kv_per_core, shared_size, MPI_INT64_T,
                               kv_per_proc, proc_map_count, proc_map_off,
                               MPI_INT64_T, node_comm);
            }
        }
        MPI_Barrier(shared_comm);

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

    int64_t get_node_count(int nodeid) {
        int64_t node_kv_count = 0;
        for (int i = 0; i < shared_size; i++) {
            int proc_idx = proc_map_rank[proc_map_off[nodeid] + i];
            node_kv_count += kv_per_proc[proc_idx];
        }
        return node_kv_count;
    }

    int64_t get_proc_count(int nodeid, int sharedid) {
        int proc_idx = proc_map_rank[proc_map_off[nodeid] + sharedid];
        return kv_per_proc[proc_idx];
    }

    int get_shuffle_rank(int nodeid, int sharedid) {
        return proc_map_rank[proc_map_off[nodeid] + sharedid];
    }

    int get_node_size(int nodeid) {
        return (proc_map_off[nodeid + 1] - proc_map_off[nodeid]);
    }

    void find_bins(std::map<uint32_t,int> &redirect_bins, uint64_t redirect_count,
                   int target, uint64_t &migrate_kv_count) {

        auto iter = bin_table.begin();
        while (iter != bin_table.end()) {
            if (iter->second == 0) {
                iter++;
                continue;
            }
            if (iter->second < redirect_count) {
                LOG_PRINT(DBG_REPAR, "Redirect bin %d-> P%d (%ld, %.6lf)\n",
                          iter->first, target, iter->second,
                          (double)iter->second/(double)global_kv_count);
                migrate_kv_count += iter->second;
                redirect_count -= iter->second;
                iter->second = 0;
                redirect_bins[iter->first] = target;
            }
            if (redirect_count <= 0) break;
            iter ++;
        }
    }

    void compute_redirect_bins(std::map<uint32_t,int> &redirect_bins,
                               std::set<int> &send_procs,
                               std::set<int> &recv_procs,
                               uint64_t &migrate_kv_count) {
        // balance among nodes
        if (BALANCE_NODE) {

            // Compute inter-node redirect
            int64_t send_kv_count = 0;
            int64_t node_kv_mean = global_kv_count / node_size;

            int i = 0, j = 0;
            int ni = 0, nj = 0;
            while (i < node_size && j < node_size) {

                int64_t kv_count_i = get_node_count(i);
                int64_t kv_count_j = get_node_count(j);
                while ((double)kv_count_i > (double)node_kv_mean * 1.01) {

                    while ((double)kv_count_j > (double)node_kv_mean * 0.99 && j < node_size) {
                        j ++;
                        nj = 0;
                        kv_count_j = get_node_count(j);
                    }
                    if (j >= node_size) break;

                    //if (shuffle_rank == 0) {
                    //    fprintf(stdout, "inter-node: %d[%ld]->%d[%ld]\n",
                    //            i, kv_count_i, j, kv_count_j);
                    //}

                    int64_t node_redirect_count = 0;

                    if (node_kv_mean - kv_count_j < kv_count_i - node_kv_mean) {
                        node_redirect_count = node_kv_mean - kv_count_j;
                        kv_count_i -= node_redirect_count;
                        kv_count_j = node_kv_mean;
                    } else {
                        node_redirect_count = kv_count_i - node_kv_mean;
                        kv_count_j += node_redirect_count;
                        kv_count_i = node_kv_mean;
                    }
                    int64_t proc_kv_meani = get_node_count(i)/get_node_size(i);
                    int64_t proc_kv_meanj = get_node_count(j)/get_node_size(j);

                    while (node_redirect_count > 0 && ni < shared_size && nj < shared_size) {

                        int64_t kv_count_ni = get_proc_count(i, ni);
                        int64_t kv_count_nj = get_proc_count(j, nj);

                        while ((double)kv_count_ni > (double)proc_kv_meani * 1.01) {

                            if (node_redirect_count > kv_count_ni - proc_kv_meani) {
                                node_redirect_count -= (kv_count_ni - proc_kv_meani);
                            } else {
                                kv_count_ni = proc_kv_meani + node_redirect_count;
                                node_redirect_count = 0;
                            }

                            while ((double)kv_count_nj > (double)proc_kv_meanj * 0.99 && nj < shared_size) {
                                nj ++;
                                kv_count_nj = get_proc_count(j, nj);
                            }

                            int64_t proc_redirect_count = 0;
                            if ((kv_count_ni - proc_kv_meani) < (proc_kv_meanj - kv_count_nj)) {
                                proc_redirect_count = kv_count_ni - proc_kv_meani;
                                kv_count_ni = proc_kv_meani;
                                kv_count_nj += proc_redirect_count;
                            } else {
                                proc_redirect_count = proc_kv_meanj - kv_count_nj;
                                kv_count_nj = proc_kv_meanj;
                                kv_count_ni -= proc_redirect_count;
                            }

                            if (get_shuffle_rank(i, ni) == shuffle_rank) {
                                find_bins(redirect_bins, proc_redirect_count,
                                          get_shuffle_rank(j,nj), migrate_kv_count);
                                //printf("%d[%d] inter-node send to %d\n", shuffle_rank,
                                //       shuffle_size, get_shuffle_rank(j,nj));
                                send_kv_count += proc_redirect_count;
                                send_procs.insert(get_shuffle_rank(j,nj));
                            }
                            if (get_shuffle_rank(j, nj) == shuffle_rank) {
                                //printf("%d[%d] inter-node recv from %d\n", shuffle_rank,
                                //       shuffle_size, get_shuffle_rank(i, ni));
                                send_kv_count -= proc_redirect_count;
                                recv_procs.insert(get_shuffle_rank(i, ni));
                            }
                        }
                        ni ++;
                    }
                }
                i ++;
                ni = 0;
            }

            // Compute intra-node redirect
            MPI_Barrier(shared_comm);
            kv_per_proc[get_shuffle_rank(node_rank, shared_rank)] -= send_kv_count;
            MPI_Barrier(shared_comm);

            int64_t proc_kv_mean = get_node_count(node_rank)/shared_size;
            i = 0; j = 0;
            while (i < shared_size && j < shared_size) {
                int64_t kv_count_i = get_proc_count(node_rank, i);
                int64_t kv_count_j = get_proc_count(node_rank, j);
                //if (shuffle_rank == 0) {
                //    fprintf(stdout, "intra-node: %d[%ld]->%d[%ld] proc_kv_mean=%ld\n",
                //            i, kv_count_i, j, kv_count_j, proc_kv_mean);
                //}
                while ((double)kv_count_i > 1.01 * (double)proc_kv_mean) {
                    while ((double)kv_count_j > 0.99 * (double)proc_kv_mean && j < shared_size) {
                        j ++;
                        kv_count_j = get_proc_count(node_rank, j);
                        //if (shuffle_rank == 0) {
                        //    fprintf(stdout, "intra-node: %d[%ld]->%d[%ld]\n",
                        //            i, kv_count_i, j, kv_count_j);
                        //}
                    }
                    if (j >= shared_size) break;

                    //if (shuffle_rank == 0) {
                    //    fprintf(stdout, "intra-node: %d[%ld]->%d[%ld]\n",
                    //            i, kv_count_i, j, kv_count_j);
                    //}

                    int64_t redirect_count = 0;
                    if (kv_count_i - proc_kv_mean > proc_kv_mean - kv_count_j) {
                        redirect_count = proc_kv_mean - kv_count_j;
                        kv_count_j = proc_kv_mean;
                        kv_count_i -= redirect_count;
                    } else {
                        redirect_count = kv_count_i - proc_kv_mean;
                        kv_count_i = proc_kv_mean;
                        kv_count_j += redirect_count;
                    }
                    if (i == shared_rank) {
                        find_bins(redirect_bins, redirect_count,
                                  get_shuffle_rank(node_rank,j), migrate_kv_count);
                        //printf("%d[%d] intra-node send to %d\n", shuffle_rank,
                        //       shuffle_size, get_shuffle_rank(node_rank,j));
                        send_kv_count += redirect_count;
                        send_procs.insert(get_shuffle_rank(node_rank,j));
                    }
                    if (j == shared_rank) {
                        //printf("%d[%d] intra-node recv from %d\n", shuffle_rank,
                        //       shuffle_size, get_shuffle_rank(node_rank, i));
                        send_kv_count -= redirect_count;
                        recv_procs.insert(get_shuffle_rank(node_rank, i));
                    }
                }
                i ++;
            }

        } else {

            int64_t proc_kv_mean = global_kv_count / shuffle_size;

            int i = 0, j = 0;
            while (i < shuffle_size && j < shuffle_size) {
                int64_t kv_count_i = kv_per_proc[i];
                int64_t kv_count_j = kv_per_proc[j];
                while ((double)kv_count_i > (double)proc_kv_mean * 1.01) {
                    while ((double)kv_count_j > (double)proc_kv_mean * 0.99 && j < shuffle_size) {
                        j ++;
                        kv_count_j = kv_per_proc[j];
                    }
                    if (j >= shuffle_size) break;

                    int64_t redirect_count = 0.0;
                    if (proc_kv_mean - kv_count_j < kv_count_i - proc_kv_mean) {
                        redirect_count = proc_kv_mean - kv_count_j;
                        kv_count_i -= redirect_count;
                        kv_count_j = proc_kv_mean;
                    } else {
                        redirect_count = kv_count_i - proc_kv_mean;
                        kv_count_j += redirect_count;
                        kv_count_i = proc_kv_mean;
                    }
                    if (i == shuffle_rank) {
                        find_bins(redirect_bins, redirect_count, j, migrate_kv_count);
                        //printf("%d[%d] send to %d\n", shuffle_rank, shuffle_size, j);
                        send_procs.insert(j);
                    }
                    if (j == shuffle_rank) {
                        //printf("%d[%d] recv from %d\n", shuffle_rank, shuffle_size, i);
                        recv_procs.insert(i);
                    }
                }
                i ++;
            }
        }
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
            LOG_ERROR("Cannot convert to removable object! out_db=%p, recv count=%ld, send count=%ld\n", 
                      out_db, recv_procs.size(), send_procs.size());
        }

        //printf("%d[%d] send counts=%ld, recv_counts=%ld\n",\
        //       shuffle_rank, shuffle_size, 
        //       send_procs.size(), recv_procs.size());

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

                //printf("%d[%d] %d Send: %d->%d %d bintag=%d, count=%d\n",
                //       shuffle_rank, shuffle_size, shuffle_times, 
                //       shuffle_rank, dst, datasize, bintag, kvcount);

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

                //printf("%d[%d] %d Send 0: %d->%d\n",
                //       shuffle_rank, shuffle_size, shuffle_times, shuffle_rank, dst);

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
    MPI_Comm                      shared_comm, node_comm;
    MPI_Group                shared_group, shuffle_group;
    int    shared_rank, shared_size, node_rank, node_size;
    int64_t                    *kv_per_proc, *kv_per_core;
    int    *proc_map_off, *proc_map_count, *proc_map_rank;
    MPI_Win                       kv_proc_win, kv_core_win;
    MPI_Win      map_off_win, map_count_win, map_rank_win;

    BinContainer<KeyType,ValType>          *out_db;
    std::unordered_map<uint32_t, int>       redirect_table;
    std::unordered_map<uint32_t, uint64_t>  bin_table;
    uint64_t                                global_kv_count;
    uint64_t                                local_kv_count;
    bool                                    isrepartition;
};

}
#endif
