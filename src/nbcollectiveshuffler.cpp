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
#include "nbcollectiveshuffler.h"

using namespace MIMIR_NS;

NBCollectiveShuffler::NBCollectiveShuffler(Writable *out, HashCallback user_hash)
    :  BaseShuffler(out, user_hash)
{
    if (COMM_BUF_SIZE < (int64_t) COMM_UNIT_SIZE * (int64_t) mimir_world_size) {
        LOG_ERROR("Error: send buffer(%ld) should be larger than COMM_UNIT_SIZE(%d)*size(%d).\n", COMM_BUF_SIZE, COMM_UNIT_SIZE, mimir_world_size);
    }
    buf_size = (COMM_BUF_SIZE / COMM_UNIT_SIZE / mimir_world_size) * COMM_UNIT_SIZE;
    type_log_bytes = 0;
    int type_bytes = 0x1;
    while ((int64_t) type_bytes * (int64_t) MAX_COMM_SIZE 
           < buf_size * mimir_world_size) {
        type_bytes <<= 1;
        type_log_bytes++;
    }
    buf_count = 2;
}

NBCollectiveShuffler::~NBCollectiveShuffler()
{
}

bool NBCollectiveShuffler::open() {
    done_flag = 0;
    done_count = 0;

    char *buffer = NULL;
    int *offset = NULL;
    for (int i = 0; i < buf_count; i++) {
        buffer = (char*)mem_aligned_malloc(MEMPAGE_SIZE, buf_size * mimir_world_size);
        offset = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * mimir_world_size);
        send_buffers.push_back(buffer);
        send_offsets.push_back(offset);
    }
    cur_idx = pre_idx = 0;

    recv_buffer = (char*)mem_aligned_malloc(MEMPAGE_SIZE, buf_size * mimir_world_size);
    recv_count = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * mimir_world_size);
    a2a_s_count = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * mimir_world_size);
;
    a2a_s_displs = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * mimir_world_size);
;
    a2a_r_count = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * mimir_world_size);
;
    a2a_r_displs = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * mimir_world_size);
;
    for (int i = 0; i < mimir_world_size; i++) {
        for (int j = 0; j < buf_count; j++)
            send_offsets[j][i] = 0;
	recv_count[i] = 0;
	a2a_s_count[i] = 0;
	a2a_s_displs[i] = 0;
	a2a_r_count[i] = 0;
	a2a_r_displs[i] = 0;
    }

    MPI_Type_contiguous((0x1 << type_log_bytes), MPI_BYTE, &comm_type);
    MPI_Type_commit(&comm_type);

    done_req = MPI_REQUEST_NULL;
    a2a_req = MPI_REQUEST_NULL;
    a2av_req = MPI_REQUEST_NULL;

    LOG_PRINT(DBG_GEN, "NBCollectiveShuffler open: buf_size=%ld\n", buf_size);

    return true;
}

void NBCollectiveShuffler::close() {
    wait();

    MPI_Type_free(&comm_type);

    mem_aligned_free(a2a_r_displs);
    mem_aligned_free(a2a_r_count);
    mem_aligned_free(a2a_s_count);
    mem_aligned_free(a2a_s_displs);
    mem_aligned_free(recv_buffer);
    mem_aligned_free(recv_count);

    for (int i = 0; i < buf_count; i++) {
        mem_aligned_free(send_buffers[i]);
        mem_aligned_free(send_offsets[i]);
    }

    LOG_PRINT(DBG_GEN, "NBCollectiveShuffler close.\n");

}

void NBCollectiveShuffler::write(BaseRecordFormat *record)
{
    int target = get_target_rank(((KVRecord*)record)->get_key(), 
                                 ((KVRecord*)record)->get_key_size());

    int kvsize = record->get_record_size();
    if (kvsize > buf_size)
        LOG_ERROR("Error: KV size (%d) is larger than buf_size (%ld)\n", 
                  kvsize, buf_size);

    if ((int64_t)send_offsets[cur_idx][target] + (int64_t)kvsize > buf_size) {
        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
        while (!done_kv_exchange()) {
            push_kv_exchange();
        }
        start_kv_exchange();
        TRACKER_RECORD_EVENT(EVENT_SYN_COMM);
    }

    char *buffer = send_buffers[cur_idx] + target * (int64_t)buf_size 
        + send_offsets[cur_idx][target];
    kv.set_buffer(buffer);
    kv.convert((KVRecord*)record);
    send_offsets[cur_idx][target] += kvsize;

    //push_kv_exchange();
    kvcount++;

    return;
}

void NBCollectiveShuffler::wait()
{
    LOG_PRINT(DBG_COMM, "Comm: start wait.\n");

    TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
    while (!done_kv_exchange()) {
        push_kv_exchange();
    }

    done_flag = 1;

     do {
        start_kv_exchange();
        while (!done_kv_exchange())
            push_kv_exchange();
    } while (done_count < mimir_world_size);

    TRACKER_RECORD_EVENT(EVENT_SYN_COMM);

    LOG_PRINT(DBG_COMM, "Comm: finish wait.\n");
}

void NBCollectiveShuffler::save_data()
{
    KVRecord record;
    char *src_buf = recv_buffer;
    int k = 0;
    for (k = 0; k < mimir_world_size; k++) {
        int count = 0;
        while (count < recv_count[k]) {
            int kvsize = 0;
            record.set_buffer(src_buf);
            kvsize = record.get_record_size();
            out->write(&record);
	    src_buf += kvsize;
            count += kvsize;
        }
        int padding = recv_count[k] & ((0x1 << type_log_bytes) - 0x1);
        src_buf += padding;
    }
}

void NBCollectiveShuffler::start_kv_exchange()
{
    uint64_t sendcount = 0;

    pre_idx = cur_idx;

    for (int i = 0; i < mimir_world_size; i++)
        sendcount += (uint64_t) send_offsets[cur_idx][i];
    PROFILER_RECORD_COUNT(COUNTER_SEND_BYTES, sendcount, OPSUM);

    LOG_PRINT(DBG_COMM, "Comm: MPI_Ialltoall start. (count=%ld)\n", sendcount);

    TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

    PROFILER_RECORD_TIME_START;
    MPI_Ialltoall(send_offsets[cur_idx], 1, MPI_INT,
                  recv_count, 1, MPI_INT, mimir_world_comm, &a2a_req);
    PROFILER_RECORD_TIME_END(TIMER_COMM_IA2A);

    TRACKER_RECORD_EVENT(EVENT_COMM_IALLTOALL);

    PROFILER_RECORD_TIME_START;
    MPI_Iallreduce(&done_flag, &done_count, 1,
                   MPI_INT, MPI_SUM, mimir_world_comm, &done_req);
    PROFILER_RECORD_TIME_END(TIMER_COMM_IRDC);

    TRACKER_RECORD_EVENT(EVENT_COMM_IREDUCE);

    cur_idx = (cur_idx + 1) % buf_count;
}

void NBCollectiveShuffler::push_kv_exchange() {
    uint64_t recvcount = 0;
    int flag = 0;
    MPI_Status st;

    if (a2a_req != MPI_REQUEST_NULL) {
        PROFILER_RECORD_TIME_START;
        MPI_Test(&a2a_req, &flag, &st);
        PROFILER_RECORD_TIME_END(TIMER_COMM_TEST);
        if (flag) {
            recvcount = (uint64_t) recv_count[0];
            for (int i = 1; i < mimir_world_size; i++) {
                recvcount += (int64_t) recv_count[i];
            }

            LOG_PRINT(DBG_COMM, "Comm: MPI_Ialltoall finish. (count=%ld)\n", recvcount);

            for (int i = 0; i < mimir_world_size; i++) {
                a2a_s_count[i] = (send_offsets[pre_idx][i] + (0x1 << type_log_bytes) - 1)
                    >> type_log_bytes;
                a2a_r_count[i] = (recv_count[i] + (0x1 << type_log_bytes) - 1)
                    >> type_log_bytes;
                a2a_s_displs[i] = (i * (int) buf_size) >> type_log_bytes;
            }
            a2a_r_displs[0] = 0;
            for (int i = 1; i < mimir_world_size; i++)
                a2a_r_displs[i] = a2a_r_displs[i - 1] + a2a_r_count[i - 1];

            LOG_PRINT(DBG_COMM, "Comm: MPI_Ialltoallv start.\n");

            TRACKER_RECORD_EVENT(EVENT_SYN_COMM);
            PROFILER_RECORD_TIME_START;
            MPI_Ialltoallv(send_buffers[pre_idx], a2a_s_count, a2a_s_displs, comm_type,
                           recv_buffer, a2a_r_count, a2a_r_displs, comm_type, 
                           mimir_world_comm, &a2av_req);
            PROFILER_RECORD_TIME_END(TIMER_COMM_IA2AV);
            TRACKER_RECORD_EVENT(EVENT_COMM_IALLTOALLV);

            PROFILER_RECORD_COUNT(COUNTER_RECV_BYTES, (uint64_t) recvcount, OPSUM);

            a2a_req = MPI_REQUEST_NULL;
        }
    }

    if (a2av_req != MPI_REQUEST_NULL) {
        PROFILER_RECORD_TIME_START;
        MPI_Test(&a2av_req, &flag, &st);
        PROFILER_RECORD_TIME_END(TIMER_COMM_TEST);
        if (flag) {
            LOG_PRINT(DBG_COMM, "Comm: MPI_Ialltoallv finish.\n");

            TRACKER_RECORD_EVENT(EVENT_SYN_COMM);
            save_data();
            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

            for (int i = 0; i < mimir_world_size; i++)
                send_offsets[pre_idx][i] = 0;

            a2av_req = MPI_REQUEST_NULL;
        }
    }

    if (done_req != MPI_REQUEST_NULL) {
        PROFILER_RECORD_TIME_START;
        MPI_Test(&done_req, &flag, &st);
        PROFILER_RECORD_TIME_END(TIMER_COMM_TEST);
        if (flag) {
            done_req = MPI_REQUEST_NULL;

            LOG_PRINT(DBG_COMM, "Comm: done=%d.\n", done_count);

        }
    }

    return;
}

bool NBCollectiveShuffler::done_kv_exchange() {
    if (a2a_req == MPI_REQUEST_NULL
        && a2av_req == MPI_REQUEST_NULL 
        && done_req == MPI_REQUEST_NULL) {
        return true;
    }

    return false;
}
