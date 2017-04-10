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
    uint64_t    localid;
    uint64_t    globalid;
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

class ChunkManager {
  public:
    ChunkManager(std::vector<std::string> input_dir, SplitPolicy policy = BYNAME) {
        // get file list
        InputSplit filelist;
        for (std::vector<std::string>::const_iterator iter = input_dir.begin();
             iter != input_dir.end(); iter++) {
            std::string file = *(iter);
            filelist.add(file.c_str());
        }
        FileSplitter::getFileSplitter()->split(&filelist, file_list, policy);
        file_list[mimir_world_rank].print();
        // get file chunk number
        total_chunk = 0;
        chunk_nums = (uint64_t*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(uint64_t) * mimir_world_size);
        for (int i = 0; i < mimir_world_size; i++) {
            chunk_nums[i] = get_chunk_num(i);
            total_chunk += chunk_nums[i];
        }
        chunk_id = 0;
        shuffler = NULL;
        LOG_PRINT(DBG_CHUNK, "Chunk: init count=%ld\n", chunk_nums[mimir_world_rank]);
    }

    virtual ~ChunkManager() {
        mem_aligned_free(chunk_nums);
        for (size_t i = 0; i < msg_list.size(); i++) {
            while (msg_list[i].msg_state != MsgCompete) {
                make_progress();
            }
            mem_aligned_free(msg_list[i].msg_buf);
        }
        msg_list.clear();
        LOG_PRINT(DBG_CHUNK, "Chunk: uninit\n");
    }

    virtual void set_shuffler(BaseShuffler *shuffler) {
        this->shuffler = shuffler;
    }

    virtual void wait() {
        for (size_t i = 0; i < msg_list.size(); i++) {
            while (msg_list[i].msg_state != MsgCompete) {
                make_progress();
            }
            mem_aligned_free(msg_list[i].msg_buf);
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
            LOG_PRINT(DBG_CHUNK, "Chunk: send head (%d) of chunk %ld to %d\n",
                      bufsize, chunk.globalid, msg_list[msg_idx].target_rank);
            MPI_Isend(msg_list[msg_idx].msg_buf, msg_list[msg_idx].msg_size, 
                      MPI_BYTE, msg_list[msg_idx].target_rank, CHUNK_TAIL_TAG,
                      mimir_world_comm, &(msg_list[msg_idx].msg_req));
            msg_list[msg_idx].msg_state = MsgPending;
        }

        return bufsize;
    }

    virtual int recv_tail(Chunk& chunk, char *buffer, int bufsize) {
        int next_rank = 0, recv_count = 0, flag = 0;
        MPI_Request req;
        MPI_Status st;

        while ((next_rank = next_chunk_worker(chunk)) == PROC_RANK_PENDING) {
            make_progress();
            if (shuffler) shuffler->make_progress();
        }

        LOG_PRINT(DBG_CHUNK, "Chunk: recv tail of chunk %ld from %d\n",
                  chunk.globalid, next_rank);

        MPI_Irecv(buffer, bufsize, MPI_BYTE, next_rank,
                  CHUNK_TAIL_TAG, mimir_world_comm, &req);
        while (!flag) {
            MPI_Test(&req, &flag, &st);
            make_progress();
            if (shuffler) shuffler->make_progress();
        }
        MPI_Get_count(&st, MPI_BYTE, &recv_count);

        LOG_PRINT(DBG_CHUNK, "Chunk: recv tail (%d) of chunk %ld from %d\n",
                  recv_count, chunk.globalid, next_rank);

        return recv_count;
    }

    virtual bool acquire_chunk(Chunk& chunk) {
        make_progress();
        if (chunk_id >= chunk_nums[mimir_world_rank]) return false;
        uint64_t my_chunk_id = chunk_id;
        chunk_id ++;
        return get_chunk(chunk, mimir_world_rank, my_chunk_id);
    }

    virtual bool acquire_local_chunk(Chunk& chunk, uint64_t localid) {
        make_progress();
        if (chunk_id >= chunk_nums[mimir_world_rank]) return false;
        if (chunk_id == localid) {
            chunk_id += 1;
            return get_chunk(chunk, mimir_world_rank, localid);
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
                    LOG_PRINT(DBG_CHUNK, "Chunk: send head (%d) of chunk %ld to %d\n",
                              msg_list[i].msg_size, msg_list[i].msg_chunk.globalid, msg_list[i].target_rank);
                    MPI_Isend(msg_list[i].msg_buf, msg_list[i].msg_size, 
                              MPI_BYTE, msg_list[i].target_rank, CHUNK_TAIL_TAG,
                              mimir_world_comm, &(msg_list[i].msg_req));
                    msg_list[i].msg_state = MsgPending;
                }
            }
        }
    }

  protected:

    virtual int prev_chunk_worker(Chunk &chunk) {
        if (chunk.procrank > 0 && chunk.localid == 0)
            return chunk.procrank - 1;
        LOG_ERROR("Chunk (%ld: %d, %ld) is not a border chunk!\n",
                  chunk.globalid, chunk.procrank, chunk.localid);
        return -1;
    }

    virtual int next_chunk_worker(Chunk &chunk) {
        if (chunk.procrank < mimir_world_size - 1
            && chunk.localid == chunk_nums[chunk.procrank] - 1)
            return chunk.procrank + 1;
        LOG_ERROR("Chunk (%ld: %d, %ld) is not a border chunk!\n",
                  chunk.globalid, chunk.procrank, chunk.localid);
        return -1;
    }

    void LocaltoGlobal(int procrank, uint64_t localid, uint64_t& globalid) {
        uint64_t localoff = 0;
        for (int i = 0; i < procrank; i++) localoff += chunk_nums[i];
        globalid = localoff + localid;
    }

    void GlobaltoLocal(uint64_t globalid, int& procrank, uint64_t& localid) {
        uint64_t startoff = 0;
        uint64_t endoff = 0;
        for (int i = 0; i < mimir_world_size; i++) {
            endoff = startoff + chunk_nums[i];
            if (globalid >= startoff && globalid < endoff) {
                procrank = i;
                localid = globalid - startoff;
                break;
            }
            startoff = endoff;
        }
    }

    bool get_chunk(Chunk &chunk, int rank, uint64_t chunk_id) {
        uint64_t total_chunk = 0;
        std::vector<FileSeg>& filesegs = file_list[rank].get_file_segs();
        for (size_t i = 0; i < filesegs.size(); i++) {
            uint64_t file_chunk = ROUNDUP(filesegs[i].segsize, INPUT_BUF_SIZE);
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
                LOG_PRINT(DBG_CHUNK, "Chunk: get chunk %ld\n", chunk.globalid);
                return true;
            }
            total_chunk += file_chunk;
        }
        return false;
    }

    uint64_t get_chunk_num(int rank) {
        uint64_t total_chunk = 0;
        std::vector<FileSeg>& filesegs = file_list[rank].get_file_segs();
        for (size_t i = 0; i < filesegs.size(); i++) {
            total_chunk += ROUNDUP(filesegs[i].segsize, INPUT_BUF_SIZE);
        }
        return total_chunk;
    }

    std::vector<InputSplit>      file_list;
    uint64_t*                    chunk_nums;
    uint64_t                     chunk_id;
    uint64_t                     total_chunk;
    BaseShuffler*                shuffler;
    std::vector<BorderMsg>       msg_list;
};

class StealChunkManager : public ChunkManager {
  public:
    StealChunkManager(std::vector<std::string> input_dir, SplitPolicy policy = BYNAME)
        : ChunkManager(input_dir, policy) {
        steal_off = 0;
        chunk_map = (int*)mem_aligned_malloc(MEMPAGE_SIZE,
                                             sizeof(int) * chunk_nums[mimir_world_rank]);
        for (uint64_t i = 0; i < chunk_nums[mimir_world_rank]; i++)
            chunk_map[i] = PROC_RANK_PENDING;
        MPI_Win_create(&chunk_id, sizeof(uint64_t), sizeof(uint64_t),
                       MPI_INFO_NULL, mimir_world_comm, &chunk_id_win);
        MPI_Win_create(&steal_off, sizeof(uint64_t), sizeof(uint64_t),
                       MPI_INFO_NULL, mimir_world_comm, &steal_off_win);
        MPI_Win_create(chunk_map, sizeof(int) * chunk_nums[mimir_world_rank], 
                       sizeof(int), MPI_INFO_NULL, mimir_world_comm, &chunk_map_win);
    }

    virtual ~StealChunkManager() {
        MPI_Win_free(&chunk_map_win);
        MPI_Win_free(&chunk_id_win);
        MPI_Win_free(&steal_off_win);
        mem_aligned_free(chunk_map);
    }

    virtual bool acquire_chunk(Chunk& chunk) {
        uint64_t one = 1, my_chunk_id = 0;

        make_progress();

        if (chunk_id >= chunk_nums[mimir_world_rank])
            return steal_chunk(chunk);

        MPI_Win_lock(MPI_LOCK_SHARED, mimir_world_rank, 0, chunk_id_win);
        MPI_Fetch_and_op(&one, &my_chunk_id,
                         MPI_UINT64_T, mimir_world_rank, 0,
                         MPI_SUM, chunk_id_win);
        MPI_Win_unlock(mimir_world_rank, chunk_id_win);

        if (my_chunk_id >= chunk_nums[mimir_world_rank])
            return steal_chunk(chunk);

        MPI_Win_lock(MPI_LOCK_EXCLUSIVE, mimir_world_rank, 0, chunk_map_win);
        MPI_Put(&mimir_world_rank, 1, MPI_INT, mimir_world_rank,
                my_chunk_id, 1, MPI_INT, chunk_map_win);
        MPI_Win_unlock(mimir_world_rank, chunk_map_win);

        return get_chunk(chunk, mimir_world_rank, my_chunk_id);
    }

    virtual bool acquire_local_chunk(Chunk& chunk, uint64_t localid) {

        make_progress();

        if (chunk_id >= chunk_nums[mimir_world_rank]) {
            return false;
        }

        uint64_t add_idx = localid + 1, ret_idx = 0;
        MPI_Win_lock(MPI_LOCK_SHARED, mimir_world_rank, 0, chunk_id_win);
        MPI_Compare_and_swap(&add_idx, &localid, &ret_idx, MPI_UINT64_T,
                             mimir_world_rank, 0, chunk_id_win);
        MPI_Win_unlock(mimir_world_rank, chunk_id_win);
        if (ret_idx == localid) {

            MPI_Win_lock(MPI_LOCK_EXCLUSIVE, mimir_world_rank, 0, chunk_map_win);
            MPI_Put(&mimir_world_rank, 1, MPI_INT, mimir_world_rank,
                    localid, 1, MPI_INT, chunk_map_win);
            MPI_Win_unlock(mimir_world_rank, chunk_map_win);

            return get_chunk(chunk, mimir_world_rank, localid);
        }

        return false;
    }

  protected:

    virtual bool steal_chunk(Chunk& chunk) {
        uint64_t one = 1;
        uint64_t local_chunk_id = 0;
        while (steal_off < mimir_world_size - 1) {
            int victim_rank = (mimir_world_rank + steal_off + 1) % mimir_world_size;
            int victim_steal_off = 0;
            MPI_Win_lock(MPI_LOCK_SHARED, victim_rank, 0, steal_off_win);
            MPI_Get(&victim_steal_off, 1, MPI_INT, victim_rank, 0, 1, MPI_INT, steal_off_win);
            MPI_Win_unlock(victim_rank, steal_off_win);
            LOG_PRINT(DBG_CHUNK, "Chunk: try to steal from %d (steal offset=%ld)\n",
                      victim_rank, local_chunk_id);
            if (victim_steal_off == 0) {
                MPI_Win_lock(MPI_LOCK_SHARED, victim_rank, 0, chunk_id_win);
                MPI_Fetch_and_op(&one, &local_chunk_id,
                                 MPI_UINT64_T, victim_rank, 0,
                                 MPI_SUM, chunk_id_win);
                MPI_Win_unlock(victim_rank, chunk_id_win);
                // steal success
                if (local_chunk_id < chunk_nums[victim_rank]) {
                    // write
                    LOG_PRINT(DBG_CHUNK, "Chunk: steal %ld from %d\n", local_chunk_id, victim_rank);
                    MPI_Win_lock(MPI_LOCK_EXCLUSIVE, victim_rank, 0, chunk_map_win);
                    MPI_Put(&mimir_world_rank, 1, MPI_INT, 
                            victim_rank, local_chunk_id,
                            1, MPI_INT, chunk_map_win);
                    MPI_Win_unlock(victim_rank, chunk_map_win);
                    return get_chunk(chunk, victim_rank, local_chunk_id);
                }
            }
            //steal_off += (victim_steal_off + 1);
            uint64_t new_steal_off = steal_off + victim_steal_off + 1;
            MPI_Win_lock(MPI_LOCK_EXCLUSIVE, mimir_world_rank, 0, steal_off_win);
            MPI_Put(&new_steal_off, 1, MPI_INT, mimir_world_rank, 0, 1, MPI_INT, steal_off_win);
            MPI_Win_unlock(mimir_world_rank, steal_off_win);
        }
        return false;
    }

    virtual int prev_chunk_worker(Chunk &chunk) {
        return get_chunk_worker(chunk.globalid - 1);
    }

    virtual int next_chunk_worker(Chunk &chunk) {
        return get_chunk_worker(chunk.globalid + 1);
    }

    int get_chunk_worker(uint64_t globalid) {
        int worker_rank = 0, chunk_owner_rank = 0;
        uint64_t chunk_owner_id = 0;

        if (globalid >= total_chunk)
            LOG_ERROR("Global id %ld is error!\n", globalid);

        GlobaltoLocal(globalid, chunk_owner_rank, chunk_owner_id);

        //if (chunk_owner_rank != mimir_world_rank) {
            MPI_Win_lock(MPI_LOCK_SHARED, chunk_owner_rank, 0, chunk_map_win);
            MPI_Get(&worker_rank, 1, MPI_INT, chunk_owner_rank, chunk_owner_id,
                    1, MPI_INT, chunk_map_win);
            MPI_Win_unlock(chunk_owner_rank, chunk_map_win);
        //} else {
        //    worker_rank = chunk_map[chunk_owner_id];
        //}

        return worker_rank;
    }

    void print_chunk_map() {
        for (uint64_t i = 0; i < chunk_nums[mimir_world_rank]; i++) {
            printf("%d[%d] %ld:%d\n",
                   mimir_world_rank, mimir_world_size, i, chunk_map[i]);
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
