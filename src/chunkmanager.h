/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */

#ifndef MIMIR_CHUNCK_MANAGER_H
#define MIMIR_CHUNCK_MANAGER_H

#include <mpi.h>
#include <vector>
#include <string>

#include "config.h"
#include "globals.h"
#include "inputsplit.h"
#include "filesplitter.h"
#include "baseshuffler.h"

#include <unordered_map>

namespace MIMIR_NS {

#define   PROC_RANK_PENDING     -1

struct Chunk {
    FileSeg    *fileseg;
    uint64_t    fileoff;
    uint64_t    chunksize;
    int         procrank;
    int         localid;
    int64_t     globalid;
};

enum MsgState {MsgReady, MsgPending, MsgCompete};

struct BorderMsg {
    int         target_rank;
    MsgState    msg_state;
    MPI_Request msg_req;
    int         msg_size;
    char       *msg_buf;
    Chunk       msg_chunk;
};

template <typename KeyType, typename ValType>
class ChunkManager {
  public:
    ChunkManager(MPI_Comm comm, std::vector<std::string> input_dir, SplitPolicy policy = BYNAME) {
        chunk_mgr_comm = comm;
        MPI_Comm_rank(chunk_mgr_comm, &chunk_mgr_rank);
        MPI_Comm_size(chunk_mgr_comm, &chunk_mgr_size);
        // get file list
        InputSplit filelist;
        if (chunk_mgr_rank == 0) {
            for (std::vector<std::string>::const_iterator iter = input_dir.begin();
                 iter != input_dir.end(); iter++) {
                std::string file = *(iter);
                filelist.add(file.c_str());
            }
        }
        FileSplitter::getFileSplitter(chunk_mgr_comm)->split(&filelist, file_list, policy);
        file_list[chunk_mgr_rank].print();
        // get file chunk number
        this->total_chunk = 0;
        this->chunk_nums = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * chunk_mgr_size);
        for (int i = 0; i < chunk_mgr_size; i++) {
            this->chunk_nums[i] = get_chunk_num(i);
            this->total_chunk += this->chunk_nums[i];
        }
        chunk_id = 0;
        shuffler = NULL;
        LOG_PRINT(DBG_CHUNK, "Chunk: chuck count=%d, total chucnks=%ld\n",
                  this->chunk_nums[chunk_mgr_rank], this->total_chunk);
    }

    virtual ~ChunkManager() {
        mem_aligned_free(this->chunk_nums);
        LOG_PRINT(DBG_CHUNK, "Chunk: uninit\n");
    }

    virtual void set_shuffler(BaseShuffler<KeyType,ValType> *shuffler) {
        this->shuffler = shuffler;
    }

    virtual void wait() {
        for (size_t i = 0; i < msg_list.size(); i++) {
            while (msg_list[i].msg_state != MsgCompete) {
                make_progress();
                if (shuffler) shuffler->make_progress();
            }
            if (msg_list[i].msg_buf != NULL) {
                mem_aligned_free(msg_list[i].msg_buf);
                msg_list[i].msg_buf = NULL;
            }
        }
        msg_list.clear();
    }

    virtual bool has_head(Chunk& chunk) {
        if (chunk.fileoff != 0) return true;
        return false;
    }

    virtual bool has_tail(Chunk& chunk) {
        if (chunk.fileoff + chunk.chunksize < chunk.fileseg->filesize)
            return true;
        return false;
    }

    virtual int send_head(Chunk& chunk, char *buffer, int bufsize) {
        if (bufsize > MAX_RECORD_SIZE)
            LOG_ERROR("Send head size (%d) is larger than max value (%d)\n",
                      bufsize, MAX_RECORD_SIZE);

        int prev_rank = prev_chunk_worker(chunk);
        size_t msg_idx = 0, i = 0;
        for (i = 0; i < msg_list.size(); i++) {
            if (msg_list[i].msg_state == MsgCompete) {
                msg_idx = i;
                break;
            }
        }
        if (i >= msg_list.size()) {
            BorderMsg msg;
            msg.msg_buf = (char*)mem_aligned_malloc(MEMPAGE_SIZE, MAX_RECORD_SIZE);
            msg_list.push_back(msg);
            msg_idx = msg_list.size() - 1;
        }

        msg_list[msg_idx].target_rank = prev_rank;
        msg_list[msg_idx].msg_state = MsgReady;
        msg_list[msg_idx].msg_req = MPI_REQUEST_NULL;
        msg_list[msg_idx].msg_size = bufsize;
        msg_list[msg_idx].msg_chunk = chunk;
        memcpy(msg_list[msg_idx].msg_buf, buffer, bufsize);

        if (msg_list[msg_idx].target_rank != PROC_RANK_PENDING) {
            LOG_PRINT(DBG_CHUNK, "Chunk: send head (%d) of chunk <%d,%d> to %d\n",
                      bufsize, chunk.procrank, chunk.localid, msg_list[msg_idx].target_rank);
            MPI_Isend(msg_list[msg_idx].msg_buf, msg_list[msg_idx].msg_size, 
                      MPI_BYTE, msg_list[msg_idx].target_rank, CHUNK_TAIL_TAG,
                      chunk_mgr_comm, &(msg_list[msg_idx].msg_req));
            msg_list[msg_idx].msg_state = MsgPending;
        }

        return bufsize;
    }

    virtual int recv_tail(Chunk& chunk, char *buffer, int bufsize) {
        int next_rank = 0, recv_count = 0, flag = 0;
        MPI_Request req;
        MPI_Status st;

        LOG_PRINT(DBG_CHUNK, "Chunk: recv tail of chunk <%d,%d> from %d\n",
                  chunk.procrank, chunk.localid, next_rank);

        while ((next_rank = next_chunk_worker(chunk)) == PROC_RANK_PENDING) {
            make_progress();
            if (shuffler) shuffler->make_progress(true);
        }

        MPI_Irecv(buffer, bufsize, MPI_BYTE, next_rank,
                  CHUNK_TAIL_TAG, chunk_mgr_comm, &req);
        while (!flag) {
            MPI_Test(&req, &flag, &st);
            make_progress();
            if (shuffler) shuffler->make_progress(true);
        }
        MPI_Get_count(&st, MPI_BYTE, &recv_count);

        LOG_PRINT(DBG_CHUNK, "Chunk: recv tail (length=%d) of chunk <%d,%d> from %d\n",
                  recv_count, chunk.procrank, chunk.localid, next_rank);

        return recv_count;
    }

    virtual bool acquire_chunk(Chunk& chunk) {
        make_progress();
        if (chunk_id >= this->chunk_nums[chunk_mgr_rank]) return false;
        int my_chunk_id = chunk_id;
        chunk_id ++;
        return get_chunk(chunk, chunk_mgr_rank, my_chunk_id);
    }

    virtual bool acquire_local_chunk(Chunk& chunk, int localid) {
        make_progress();
        if (chunk_id >= this->chunk_nums[chunk_mgr_rank]) return false;
        if (chunk_id == localid) {
            chunk_id += 1;
            return get_chunk(chunk, chunk_mgr_rank, localid);
        }
        return false;
    }

    virtual bool is_file_end(Chunk& chunk) {
        if (chunk.fileoff + chunk.chunksize == chunk.fileseg->filesize)
            return true;
        return false;
    }

    virtual void make_progress() {
        for (size_t i = 0; i < msg_list.size(); i++) {
            // Test pending messgae
            if (msg_list[i].msg_state == MsgPending) {
                int flag = 0;
                MPI_Status st;
                MPI_Test(&(msg_list[i].msg_req), &flag, &st);
                if (flag) msg_list[i].msg_state = MsgCompete;
            // Start sending
            } else if (msg_list[i].msg_state == MsgReady) {
                msg_list[i].target_rank = prev_chunk_worker(msg_list[i].msg_chunk);
                if (msg_list[i].target_rank != PROC_RANK_PENDING) {
                    LOG_PRINT(DBG_CHUNK, "Chunk: send head (%d) of chunk <%d,%d> to %d\n",
                              msg_list[i].msg_size, msg_list[i].msg_chunk.procrank,
                              msg_list[i].msg_chunk.localid, msg_list[i].target_rank);
                    MPI_Isend(msg_list[i].msg_buf, msg_list[i].msg_size, 
                              MPI_BYTE, msg_list[i].target_rank, CHUNK_TAIL_TAG,
                              chunk_mgr_comm, &(msg_list[i].msg_req));
                    msg_list[i].msg_state = MsgPending;
                }
            }
        }
    }

  protected:

    virtual int prev_chunk_worker(Chunk &chunk) {
        if (chunk.procrank > 0 && chunk.localid == 0)
            return chunk.procrank - 1;
        LOG_ERROR("Chunk (%ld: %d, %d) is not a border chunk!\n",
                  chunk.globalid, chunk.procrank, chunk.localid);
        return -1;
    }

    virtual int next_chunk_worker(Chunk &chunk) {
        if (chunk.procrank < chunk_mgr_size - 1
            && chunk.localid == this->chunk_nums[chunk.procrank] - 1)
            return chunk.procrank + 1;
        LOG_ERROR("Chunk (%ld: %d, %d) is not a border chunk!\n",
                  chunk.globalid, chunk.procrank, chunk.localid);
        return -1;
    }

    void LocaltoGlobal(int procrank, int localid, int64_t& globalid) {
        uint64_t localoff = 0;
        for (int i = 0; i < procrank; i++) localoff += this->chunk_nums[i];
        globalid = localoff + localid;
    }

    void GlobaltoLocal(int64_t globalid, int& procrank, int& localid) {
        int64_t startoff = 0;
        int64_t endoff = 0;
        for (int i = 0; i < chunk_mgr_size; i++) {
            endoff = startoff + this->chunk_nums[i];
            if (globalid >= startoff && globalid < endoff) {
                procrank = i;
                localid = (int)(globalid - startoff);
                break;
            }
            startoff = endoff;
        }
    }

    bool get_chunk(Chunk &chunk, int rank, int chunk_id) {
        int64_t total_chunk = 0;
        std::vector<FileSeg>& filesegs = file_list[rank].get_file_segs();
        for (size_t i = 0; i < filesegs.size(); i++) {
            int file_chunk = (int)ROUNDUP(filesegs[i].segsize, INPUT_BUF_SIZE);
            if (total_chunk + file_chunk > chunk_id) {
                chunk.fileoff = (chunk_id - total_chunk) * INPUT_BUF_SIZE 
                    + filesegs[i].startpos;
                if (filesegs[i].filesize - chunk.fileoff < (uint64_t)INPUT_BUF_SIZE)
                    chunk.chunksize = filesegs[i].filesize - chunk.fileoff;
                else
                    chunk.chunksize = INPUT_BUF_SIZE;
                chunk.fileseg = &filesegs[i];
                chunk.procrank = rank;
                chunk.localid = chunk_id;
                LocaltoGlobal(rank, chunk_id, chunk.globalid);
                LOG_PRINT(DBG_CHUNK, "Chunk: get chunk <%d,%d> from %d (fileoff=%ld, chunksize=%ld)\n",
                          chunk.procrank, chunk.localid, chunk.procrank, chunk.fileoff, chunk.chunksize);
                return true;
            }
            total_chunk += file_chunk;
        }
        LOG_ERROR("Cannot find chunk %d, %d, total_chunk=%ld\n", rank, chunk_id, total_chunk);
        return false;
    }

    int get_chunk_num(int rank) {
        int total_chunk = 0;
        std::vector<FileSeg>& filesegs = file_list[rank].get_file_segs();
        for (size_t i = 0; i < filesegs.size(); i++) {
            total_chunk += (int)ROUNDUP(filesegs[i].segsize, INPUT_BUF_SIZE);
        }
        return total_chunk;
    }

    std::vector<InputSplit>      file_list;
    int*                         chunk_nums;
    int                          chunk_id;
    int64_t                      total_chunk;
    BaseShuffler<KeyType,ValType>* shuffler;
    std::vector<BorderMsg>       msg_list;
    MPI_Comm                     chunk_mgr_comm;
    int                          chunk_mgr_rank;
    int                          chunk_mgr_size;
};

template <typename KeyType, typename ValType>
class StealChunkManager : public ChunkManager<KeyType,ValType> {
  public:
    StealChunkManager(MPI_Comm comm, std::vector<std::string> input_dir, SplitPolicy policy = BYNAME)
        : ChunkManager<KeyType,ValType>(comm, input_dir, policy) {
        steal_off = 0;
        chunk_map = (int*)mem_aligned_malloc(MEMPAGE_SIZE,
                                             sizeof(int) * this->chunk_nums[this->chunk_mgr_rank]);
        for (int i = 0; i < this->chunk_nums[this->chunk_mgr_rank]; i++)
            chunk_map[i] = PROC_RANK_PENDING;
        MPI_Win_create(&(this->chunk_id), sizeof(int), sizeof(int),
                       MPI_INFO_NULL, this->chunk_mgr_comm, &chunk_id_win);
        MPI_Win_create(&steal_off, sizeof(int), sizeof(int),
                       MPI_INFO_NULL, this->chunk_mgr_comm, &steal_off_win);
        MPI_Win_create(chunk_map, sizeof(int) * this->chunk_nums[this->chunk_mgr_rank], 
                       sizeof(int), MPI_INFO_NULL, this->chunk_mgr_comm, &chunk_map_win);
    }

    virtual ~StealChunkManager() {
        MPI_Win_free(&chunk_map_win);
        MPI_Win_free(&chunk_id_win);
        MPI_Win_free(&steal_off_win);
        mem_aligned_free(chunk_map);
    }

    virtual bool acquire_chunk(Chunk& chunk) {
        int one = 1, my_chunk_id = 0;

        this->make_progress();

        if (this->chunk_id >= this->chunk_nums[this->chunk_mgr_rank])
            return steal_chunk(chunk);

        MPI_Win_lock(MPI_LOCK_SHARED, this->chunk_mgr_rank, 0, chunk_id_win);
#ifdef MPI_FETCH_AND_OP
        MPI_Fetch_and_op(&one, &my_chunk_id,
                         MPI_INT, this->chunk_mgr_rank, 0,
                         MPI_SUM, chunk_id_win);
#else
        MPI_Get_accumulate(&one, 1, MPI_INT, &my_chunk_id, 1, MPI_INT,
                           this->chunk_mgr_rank, 0, 1, MPI_INT, MPI_SUM, chunk_id_win);
#endif
        MPI_Win_unlock(this->chunk_mgr_rank, chunk_id_win);

        if (my_chunk_id >= this->chunk_nums[this->chunk_mgr_rank])
            return steal_chunk(chunk);

        MPI_Win_lock(MPI_LOCK_SHARED, this->chunk_mgr_rank, 0, chunk_map_win);
        MPI_Accumulate(&(this->chunk_mgr_rank), 1, MPI_INT,
                       this->chunk_mgr_rank, my_chunk_id, 1, MPI_INT,
                       MPI_REPLACE, chunk_map_win);
        MPI_Win_unlock(this->chunk_mgr_rank, chunk_map_win);

        return this->get_chunk(chunk, this->chunk_mgr_rank, my_chunk_id);
    }

    virtual bool acquire_local_chunk(Chunk& chunk, int localid) {

        this->make_progress();

        if (this->chunk_id >= this->chunk_nums[this->chunk_mgr_rank]) {
            return false;
        }

        int add_idx = localid + 1, ret_idx = 0;
        MPI_Win_lock(MPI_LOCK_SHARED, this->chunk_mgr_rank, 0, chunk_id_win);
        MPI_Compare_and_swap(&add_idx, &localid, &ret_idx, MPI_INT,
                             this->chunk_mgr_rank, 0, chunk_id_win);
        MPI_Win_unlock(this->chunk_mgr_rank, chunk_id_win);
        if (ret_idx == localid) {

            MPI_Win_lock(MPI_LOCK_SHARED, this->chunk_mgr_rank, 0, chunk_map_win);
            MPI_Accumulate(&(this->chunk_mgr_rank), 1, MPI_INT, this->chunk_mgr_rank,
                           localid, 1, MPI_INT, MPI_REPLACE, chunk_map_win);
            MPI_Win_unlock(this->chunk_mgr_rank, chunk_map_win);

            return this->get_chunk(chunk, this->chunk_mgr_rank, localid);
        }

        return false;
    }

  protected:

    virtual bool steal_chunk(Chunk& chunk) {
        int one = 1;
        int local_chunk_id = 0;
        if (steal_off == 0) steal_off = 1;
        while (steal_off < this->chunk_mgr_size) {

            int victim_rank = (this->chunk_mgr_rank + steal_off) % this->chunk_mgr_size;
            int victim_steal_off = 0;
            MPI_Win_lock(MPI_LOCK_SHARED, victim_rank, 0, steal_off_win);
            int tmp;
#ifdef MPI_FETCH_AND_OP
            MPI_Fetch_and_op(&tmp, &victim_steal_off,
                             MPI_INT, victim_rank, 0, 
                             MPI_NO_OP, steal_off_win);
#else
            MPI_Get_accumulate(&tmp, 1, MPI_INT, &victim_steal_off, 1, MPI_INT,
                               victim_rank, 0, 1, MPI_INT, MPI_NO_OP, steal_off_win);
#endif
            MPI_Win_unlock(victim_rank, steal_off_win);
            LOG_PRINT(DBG_CHUNK, "Chunk: try to steal from %d (steal offset=%d)\n",
                      victim_rank, victim_steal_off);
            if (victim_steal_off == 0) {
                MPI_Win_lock(MPI_LOCK_SHARED, victim_rank, 0, chunk_id_win);
#ifdef MPI_FETCH_AND_OP
                MPI_Fetch_and_op(&one, &local_chunk_id,
                                 MPI_INT, victim_rank, 0,
                                 MPI_SUM, chunk_id_win);
#else
                MPI_Get_accumulate(&one, 1, MPI_INT, &local_chunk_id, 1, MPI_INT,
                                   victim_rank, 0, 1, MPI_INT, MPI_SUM, chunk_id_win);
#endif
                MPI_Win_unlock(victim_rank, chunk_id_win);

                LOG_PRINT(DBG_CHUNK, "Chunk: try to steal from %d FOP ret=%d\n",
                      victim_rank, local_chunk_id);

                // steal success
                if (local_chunk_id < this->chunk_nums[victim_rank]) {
                    // write
                    LOG_PRINT(DBG_CHUNK, "Chunk: steal chunk <%d,%d> from %d\n",
                              victim_rank, local_chunk_id, victim_rank);
                    MPI_Win_lock(MPI_LOCK_SHARED, victim_rank, 0, chunk_map_win);
                    MPI_Accumulate(&(this->chunk_mgr_rank), 1, MPI_INT, victim_rank,
                                   local_chunk_id, 1, MPI_INT, MPI_REPLACE,
                                   chunk_map_win);
                    MPI_Win_unlock(victim_rank, chunk_map_win);
                    return this->get_chunk(chunk, victim_rank, local_chunk_id);
                } else {
                    steal_off += 1;
                }
            } else {
                steal_off += victim_steal_off;
            }
        }
        return false;
    }

    virtual int prev_chunk_worker(Chunk &chunk) {
        return get_chunk_worker(chunk.globalid - 1);
    }

    virtual int next_chunk_worker(Chunk &chunk) {
        return get_chunk_worker(chunk.globalid + 1);
    }

    int get_chunk_worker(int64_t globalid) {
        int worker_rank = 0, chunk_owner_rank = 0, chunk_owner_id = 0;

        if (globalid >= this->total_chunk)
            LOG_ERROR("Global id %ld is error!\n", globalid);

        this->GlobaltoLocal(globalid, chunk_owner_rank, chunk_owner_id);

        MPI_Win_lock(MPI_LOCK_SHARED, chunk_owner_rank, 0, chunk_map_win);
        int tmp;
#ifdef MPI_FETCH_AND_OP
        MPI_Fetch_and_op(&tmp, &worker_rank,
    MPI_INT, chunk_owner_rank, chunk_owner_id,
                         MPI_NO_OP, chunk_map_win);
#else
        MPI_Get_accumulate(&tmp, 1, MPI_INT, &worker_rank, 1, MPI_INT,
                           chunk_owner_rank, chunk_owner_id, 1, MPI_INT,
                           MPI_NO_OP, chunk_map_win);
#endif
        MPI_Win_unlock(chunk_owner_rank, chunk_map_win);

        return worker_rank;
    }

    void print_chunk_map() {
        for (int i = 0; i < this->chunk_nums[this->chunk_mgr_rank]; i++) {
            printf("%d[%d] %d:%d\n",
                   this->chunk_mgr_rank, this->chunk_mgr_size, i, chunk_map[i]);
        }
    }

  private:
    int        steal_off;
    int*       chunk_map;
    MPI_Win    chunk_id_win;
    MPI_Win    steal_off_win;
    MPI_Win    chunk_map_win;
};

}

#endif
