/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#include <stdio.h>
#include <stdlib.h>
#include "log.h"
#include "stat.h"
#include "config.h"
#include "memory.h"
#include "hash.h"
#include "recordformat.h"
#include "kvcontainer.h"
#include "nbshuffler.h"

using namespace MIMIR_NS;

NBShuffler::NBShuffler(Writable *out, HashCallback user_hash)
    :  BaseShuffler(out, user_hash)
{
    if (COMM_BUF_SIZE < (int64_t) COMM_UNIT_SIZE * (int64_t) mimir_world_size) {
        LOG_ERROR("Error: send buffer(%ld) should be larger than COMM_UNIT_SIZE(%d)*size(%d).\n", COMM_BUF_SIZE, COMM_UNIT_SIZE, mimir_world_size);
    }
    buf_size = (COMM_BUF_SIZE / COMM_UNIT_SIZE / mimir_world_size) * COMM_UNIT_SIZE;
    buf_count = 2;
}

NBShuffler::~NBShuffler()
{
}

bool NBShuffler::open() {
    done_count = 0;

    char *buffer = NULL;
    int *offset = NULL;
    for (int i = 0; i < buf_count; i++) {
        buffer = (char*)mem_aligned_malloc(MEMPAGE_SIZE, buf_size * mimir_world_size);
        offset = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * mimir_world_size);
        send_buffers.push_back(buffer);
        send_offsets.push_back(offset);
    }
    recv_buffer = (char*)mem_aligned_malloc(MEMPAGE_SIZE, buf_size * mimir_world_size);
    cur_idx = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * mimir_world_size);
    send_reqs = (MPI_Request*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(MPI_Request) * mimir_world_size);
    recv_reqs = (MPI_Request*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(MPI_Request) * mimir_world_size);

    for (int i = 0; i < mimir_world_size; i++) {
        for (int j = 0; j < buf_count; j++)
            send_offsets[j][i] = 0;
        cur_idx[i] = 0;
        send_reqs[i] = MPI_REQUEST_NULL;
        recv_reqs[i] = MPI_REQUEST_NULL;
        if (i == mimir_world_rank) continue;
        char *buffer = recv_buffer + i * (int64_t)buf_size;
        PROFILER_RECORD_TIME_START;
        MPI_Irecv(buffer, (int)buf_size, MPI_BYTE, i,
                  SHUFFLER_KV_TAG, mimir_world_comm, &recv_reqs[i]);
        PROFILER_RECORD_TIME_END(TIMER_COMM_IRECV);
    }

    LOG_PRINT(DBG_GEN, "NBShuffler open: buf_size=%ld\n", buf_size);

    return true;
}

void NBShuffler::close() {
    wait();

    mem_aligned_free(recv_buffer);
    mem_aligned_free(cur_idx);
    mem_aligned_free(send_reqs);
    mem_aligned_free(recv_reqs);

    for (int i = 0; i < buf_count; i++) {
        mem_aligned_free(send_buffers[i]);
        mem_aligned_free(send_offsets[i]);
    }

    LOG_PRINT(DBG_GEN, "NBShuffler close.\n");

}

void NBShuffler::write(BaseRecordFormat *record)
{
    int target = get_target_rank(((KVRecord*)record)->get_key(), 
                                 ((KVRecord*)record)->get_key_size());

    int kvsize = record->get_record_size();
    if (kvsize > buf_size)
        LOG_ERROR("Error: KV size (%d) is larger than buf_size (%ld)\n", 
                  kvsize, buf_size);

    if (target == mimir_world_rank) {
       out->write(record);
       //make_progress();
       kvcount++;
       return;
    }

    int idx = cur_idx[target];
    if ((int64_t)send_offsets[idx][target] + (int64_t)kvsize > buf_size) {
        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
        while (send_reqs[target] != MPI_REQUEST_NULL) {
            make_progress();
        }
        TRACKER_RECORD_EVENT(EVENT_SYN_COMM);
        start_send(target);
    }

    idx = cur_idx[target];
    char *buffer = send_buffers[idx] + target * (int64_t)buf_size 
        + send_offsets[idx][target];
    kv.set_buffer(buffer);
    kv.convert((KVRecord*)record);
    send_offsets[idx][target] += kvsize;

    //make_progress();
    kvcount++;

    return;
}

void NBShuffler::wait()
{
    LOG_PRINT(DBG_COMM, "Comm: start wait.\n");

    for (int i = 0; i < mimir_world_size; i++) {
        if (i == mimir_world_rank) continue;
        int idx = cur_idx[i];
        if (send_offsets[idx][i] > 0) {
            while (send_reqs[i] != MPI_REQUEST_NULL) {
                make_progress();
            }
            start_send(i);
        }
    }

    TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

    for (int i = 0; i < mimir_world_size; i++) {
        if (i == mimir_world_rank) continue;
         while (send_reqs[i] != MPI_REQUEST_NULL) {
            make_progress();
        }
        start_send(i);
    }

    do {
        make_progress();
    } while (done_count < mimir_world_size - 1);

    TRACKER_RECORD_EVENT(EVENT_SYN_COMM);

    LOG_PRINT(DBG_COMM, "Comm: finish wait.\n");
}

void NBShuffler::save_data(int recv_rank, int recv_count)
{
    KVRecord record;
    char *src_buf = recv_buffer + buf_size * recv_rank;
    int count = 0;
    while (count < recv_count) {
        int kvsize = 0;
        record.set_buffer(src_buf);
        kvsize = record.get_record_size();
        out->write(&record);
        src_buf += kvsize;
        count += kvsize;
    }
}

void NBShuffler::start_send(int target_rank)
{
    int idx = cur_idx[target_rank];
    uint64_t sendcount = send_offsets[idx][target_rank];
    PROFILER_RECORD_COUNT(COUNTER_SEND_BYTES, sendcount, OPSUM);

    LOG_PRINT(DBG_COMM, "Comm: MPI_Isend start. (target=%d, idx=%d, count=%ld)\n",
              target_rank, idx, sendcount);

    if (target_rank == mimir_world_rank) {
        LOG_ERROR("Error: the KVs to ourself have been bypassed!\n");
    }

    //TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

    PROFILER_RECORD_TIME_START;
    char *buffer = send_buffers[idx] + target_rank * (int64_t)buf_size;
    int count = send_offsets[idx][target_rank];
    MPI_Isend(buffer, count, MPI_BYTE, target_rank, SHUFFLER_KV_TAG,
              mimir_world_comm, &send_reqs[target_rank]);
    PROFILER_RECORD_TIME_END(TIMER_COMM_ISEND);

    //TRACKER_RECORD_EVENT(EVENT_COMM_ISEND);

    cur_idx[target_rank] = (cur_idx[target_rank] + 1) % buf_count;
    idx = cur_idx[target_rank];
    send_offsets[idx][target_rank] = 0;
}

void NBShuffler::make_progress() {
    //uint64_t recvcount = 0;
    int flag = 0;
    MPI_Status st;

    for (int i = 0; i < mimir_world_size; i++) {
        if (i == mimir_world_rank) continue;
        if (send_reqs[i] != MPI_REQUEST_NULL) {
            PROFILER_RECORD_TIME_START;
            MPI_Test(&send_reqs[i], &flag, &st);
            PROFILER_RECORD_TIME_END(TIMER_COMM_TEST);
            if (flag) {
                send_reqs[i] = MPI_REQUEST_NULL;
                LOG_PRINT(DBG_COMM, "Comm: MPI_Isend finish.\n");
            }
        }
        if (recv_reqs[i] != MPI_REQUEST_NULL) {
            PROFILER_RECORD_TIME_START;
            MPI_Test(&recv_reqs[i], &flag, &st);
            PROFILER_RECORD_TIME_END(TIMER_COMM_TEST);
            if (flag) {
                int count;
                MPI_Get_count(&st, MPI_BYTE, &count);
                if (count > 0) {
                    LOG_PRINT(DBG_COMM, "Comm: MPI_Irecv (count=%d, done_count=%d).\n",
                              count, done_count);
                    PROFILER_RECORD_COUNT(COUNTER_RECV_BYTES, (uint64_t)count, OPSUM);
                    TRACKER_RECORD_EVENT(EVENT_SYN_COMM);
                    save_data(i, count);
                    TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
                    char *buffer = recv_buffer + i * (int64_t)buf_size;
                    PROFILER_RECORD_TIME_START;
                    MPI_Irecv(buffer, (int)buf_size, MPI_BYTE, i,
                              SHUFFLER_KV_TAG, mimir_world_comm, &recv_reqs[i]);
                    PROFILER_RECORD_TIME_END(TIMER_COMM_IRECV);
                } else {
                    LOG_PRINT(DBG_COMM, "Comm: MPI_Irecv (done).\n");
                    recv_reqs[i] = MPI_REQUEST_NULL;
                    done_count += 1;
                }
            }
        }
    }

    return;
}
