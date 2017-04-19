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
    MPI_Comm_dup(mimir_world_comm, &a2a_comm);
    MPI_Comm_dup(mimir_world_comm, &a2av_comm);
    MPI_Comm_dup(mimir_world_comm, &done_comm);
    buf_count = 0;
}

NBCollectiveShuffler::~NBCollectiveShuffler()
{
    MPI_Comm_free(&done_comm);
    MPI_Comm_free(&a2a_comm);
    MPI_Comm_free(&a2av_comm);
}

bool NBCollectiveShuffler::open() {
    done_flag = 0;
    done_count = 0;
    buf_count = 0;
    cur_idx = 0;
    a2a_token = 0;
    a2av_token = 0;
    pending_msg = 0;
    insert_comm_buffer();
    insert_comm_buffer();

    a2a_s_count = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * mimir_world_size);
;
    a2a_s_displs = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * mimir_world_size);
;
    a2a_r_count = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * mimir_world_size);
;
    a2a_r_displs = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * mimir_world_size);
;
    for (int i = 0; i < mimir_world_size; i++) {
        a2a_s_count[i] = 0;
        a2a_s_displs[i] = 0;
        a2a_r_count[i] = 0;
        a2a_r_displs[i] = 0;
    }

    MPI_Type_contiguous((0x1 << type_log_bytes), MPI_BYTE, &comm_type);
    MPI_Type_commit(&comm_type);

    LOG_PRINT(DBG_GEN, "NBCollectiveShuffler open: buf_size=%ld\n", buf_size);

    return true;
}

void NBCollectiveShuffler::close() {
    wait();

    PROFILER_RECORD_COUNT(COUNTER_COMM_BUFS, buf_count, OPMAX);

    MPI_Type_free(&comm_type);

    mem_aligned_free(a2a_r_displs);
    mem_aligned_free(a2a_r_count);
    mem_aligned_free(a2a_s_count);
    mem_aligned_free(a2a_s_displs);

    for (int i = 0; i < buf_count; i++) {
        mem_aligned_free(msg_buffers[i].send_buffer);
        mem_aligned_free(msg_buffers[i].recv_buffer);
        mem_aligned_free(msg_buffers[i].send_offset);
        mem_aligned_free(msg_buffers[i].recv_count);
    }

    LOG_PRINT(DBG_GEN, "NBCollectiveShuffler close.\n");

}

void NBCollectiveShuffler::insert_comm_buffer() {
    ShuffleMsgBuf buf;
    buf.send_buffer = (char*)mem_aligned_malloc(MEMPAGE_SIZE, buf_size * mimir_world_size);
    buf.recv_buffer = (char*)mem_aligned_malloc(MEMPAGE_SIZE, buf_size * mimir_world_size);
    buf.send_offset = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * mimir_world_size);
    buf.recv_count = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * mimir_world_size);
    buf.a2a_req = MPI_REQUEST_NULL;
    buf.a2av_req = MPI_REQUEST_NULL;
    buf.done_req = MPI_REQUEST_NULL;
    buf.send_bytes = 0;
    buf.recv_bytes = 0;
    buf.msg_state = ShuffleMsgComplete;
    msg_buffers.push_back(buf);
    for (int i = 0; i < mimir_world_size; i++) {
        msg_buffers[buf_count].send_offset[i] = 0;
        msg_buffers[buf_count].recv_count[i] = 0;
    }
    buf_count ++;

    LOG_PRINT(DBG_COMM, "Comm: add a comm buffer. (count=%ld)\n",
              buf_count);
}

void NBCollectiveShuffler::write(BaseRecordFormat *record)
{
    int target = get_target_rank(((KVRecord*)record)->get_key(), 
                                 ((KVRecord*)record)->get_key_size());

    if (target == mimir_world_rank) {
        out->write(record);
        return;
    }

    int kvsize = record->get_record_size();
    if (kvsize > buf_size)
        LOG_ERROR("Error: KV size (%d) is larger than buf_size (%ld)\n", 
                  kvsize, buf_size);

    if ((int64_t)msg_buffers[cur_idx].send_offset[target] + (int64_t)kvsize > buf_size) {
        start_kv_exchange();
    }

    char *buffer = msg_buffers[cur_idx].send_buffer + target * (int64_t)buf_size 
        + msg_buffers[cur_idx].send_offset[target];
    kv.set_buffer(buffer);
    kv.convert((KVRecord*)record);
    msg_buffers[cur_idx].send_offset[target] += kvsize;

    push_kv_exchange();
    kvcount++;

    return;
}

void NBCollectiveShuffler::wait()
{
    MPI_Status st;
    int flag = 0;

    for (int i = 0; i < mimir_world_size; i++)
        msg_buffers[cur_idx].send_bytes += (uint64_t)msg_buffers[cur_idx].send_offset[i];

    if (msg_buffers[cur_idx].send_bytes > 0)
        start_kv_exchange();

    LOG_PRINT(DBG_COMM, "Comm: start wait.\n");

    TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

    wait_all();

    done_flag = 1;
    do {
        PROFILER_RECORD_TIME_START;
        start_kv_exchange();
        wait_all();
        PROFILER_RECORD_TIME_END(TIMER_COMM_BLOCK);
        //printf("%d[%d] done_count=%ld\n",
        //       mimir_world_rank, mimir_world_size, done_count);
    } while (done_count < mimir_world_size);

    TRACKER_RECORD_EVENT(EVENT_SYN_COMM);


    //uint64_t max_token = 0;
    //MPI_Iallreduce(&a2a_token, &max_token, 1,
    //               MPI_UINT64_T, MPI_MAX, done_comm, &done_req);
    //flag = 0;
    //while (!flag) {
    //    MPI_Test(&done_req, &flag, &st);
    //    push_kv_exchange();
    //}

    //LOG_PRINT(DBG_COMM, "Comm: begin issue last comms (my_token=%ld, max_token=%ld).\n",
    //          a2a_token, max_token);

    //for (uint64_t i = a2a_token; i < max_token; i++) {
    //    start_kv_exchange();
    //}

    //wait_all();

    LOG_PRINT(DBG_COMM, "Comm: finish wait.\n");
}

void NBCollectiveShuffler::save_data(int idx)
{
    KVRecord record;
    char *src_buf = msg_buffers[idx].recv_buffer;
    int k = 0;
    for (k = 0; k < mimir_world_size; k++) {
        int count = 0;
        while (count < msg_buffers[idx].recv_count[k]) {
            int kvsize = 0;
            record.set_buffer(src_buf);
            kvsize = record.get_record_size();
            out->write(&record);
	    src_buf += kvsize;
            count += kvsize;
        }
        int padding = msg_buffers[idx].recv_count[k] & ((0x1 << type_log_bytes) - 0x1);
        src_buf += padding;
    }
}

void NBCollectiveShuffler::start_kv_exchange()
{
    for (int i = 0; i < mimir_world_size; i++)
        msg_buffers[cur_idx].send_bytes += (uint64_t)msg_buffers[cur_idx].send_offset[i];
    PROFILER_RECORD_COUNT(COUNTER_SEND_BYTES, msg_buffers[cur_idx].send_bytes, OPSUM);
    PROFILER_RECORD_COUNT(COUNTER_SHUFFLE_TIMES, 1, OPSUM);

    MPI_Ialltoall(msg_buffers[cur_idx].send_offset, 1, MPI_INT,
                  msg_buffers[cur_idx].recv_count, 1, MPI_INT,
                  a2a_comm, &(msg_buffers[cur_idx].a2a_req));

    msg_buffers[cur_idx].done_flag = done_flag;
    msg_buffers[cur_idx].done_count = 0;
    MPI_Iallreduce(&(msg_buffers[cur_idx].done_flag),
                   &(msg_buffers[cur_idx].done_count),
                   1, MPI_INT, MPI_SUM, done_comm,
                   &(msg_buffers[cur_idx].done_req));

    msg_buffers[cur_idx].msg_state = ShuffleMsgStart;
    msg_buffers[cur_idx].msg_token = a2a_token;
    a2a_token += 1;
    pending_msg += 1;

    LOG_PRINT(DBG_COMM, "Comm: MPI_Ialltoall start. (token=%ld, send=%ld)\n",
              msg_buffers[cur_idx].msg_token, msg_buffers[cur_idx].send_bytes);

    push_kv_exchange();
    cur_idx = -1;
    for (int i = 0; i < buf_count; i++) {
        if (done_kv_exchange(i)) {
            cur_idx = i;
        }
    }

    if (cur_idx == -1) {
        if (buf_count < MAX_SBUF_COUNT) {
            insert_comm_buffer();
            cur_idx = buf_count - 1;
        }
        else {
            cur_idx = 0;
            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);
            PROFILER_RECORD_TIME_START;
            while (1) {
                push_kv_exchange();
                if (done_kv_exchange(cur_idx))
                    break;
                cur_idx = (cur_idx + 1) % buf_count;
            }
            PROFILER_RECORD_TIME_END(TIMER_COMM_BLOCK);
            TRACKER_RECORD_EVENT(EVENT_SYN_COMM);
        }
    }
}

void NBCollectiveShuffler::push_kv_exchange() {
    uint64_t recvcount = 0;
    int flag = 0;
    MPI_Status st;

    for (int k = 0; k < buf_count; k++) {

        if (msg_buffers[k].msg_state == ShuffleMsgComplete) continue;

        if (msg_buffers[k].msg_state == ShuffleMsgStart) {

            if (msg_buffers[k].a2a_req != MPI_REQUEST_NULL) {

                MPI_Test(&(msg_buffers[k].a2a_req), &flag, &st);

                if (flag) {
                    LOG_PRINT(DBG_COMM, "Comm: MPI_Ialltoall finish (token=%ld).\n",
                              msg_buffers[k].msg_token);
                    msg_buffers[k].a2a_req = MPI_REQUEST_NULL;
                }
            }

            if (msg_buffers[k].msg_token == a2av_token 
                && msg_buffers[k].a2a_req == MPI_REQUEST_NULL) {

                for (int i = 0; i < mimir_world_size; i++) {
                    msg_buffers[k].recv_bytes += (int64_t) msg_buffers[k].recv_count[i];
                }

                for (int i = 0; i < mimir_world_size; i++) {
                    a2a_s_count[i] = (msg_buffers[k].send_offset[i] 
                                      + (0x1 << type_log_bytes) - 1) >> type_log_bytes;
                    a2a_r_count[i] = (msg_buffers[k].recv_count[i] 
                                      + (0x1 << type_log_bytes) - 1) >> type_log_bytes;
                    a2a_s_displs[i] = (i * (int) buf_size) >> type_log_bytes;
                }
                a2a_r_displs[0] = 0;
                for (int i = 1; i < mimir_world_size; i++)
                    a2a_r_displs[i] = a2a_r_displs[i - 1] + a2a_r_count[i - 1];

                LOG_PRINT(DBG_COMM, "Comm: MPI_Ialltoallv start (token=%ld,send=%ld,recv=%ld).\n",
                          msg_buffers[k].msg_token, msg_buffers[k].send_bytes, msg_buffers[k].recv_bytes);

                MPI_Ialltoallv(msg_buffers[k].send_buffer, a2a_s_count, a2a_s_displs, comm_type,
                               msg_buffers[k].recv_buffer, a2a_r_count, a2a_r_displs, comm_type, 
                               a2av_comm, &(msg_buffers[k].a2av_req));

                PROFILER_RECORD_COUNT(COUNTER_RECV_BYTES, (uint64_t) msg_buffers[k].recv_bytes, OPSUM);

                msg_buffers[k].msg_state = ShuffleMsgPending;
                a2av_token += 1;
            }
        }

        if (msg_buffers[k].msg_state == ShuffleMsgPending) {
            if (msg_buffers[k].a2av_req != MPI_REQUEST_NULL) {
                MPI_Test(&(msg_buffers[k].a2av_req), &flag, &st);
                if (flag) {
                    msg_buffers[k].a2av_req = MPI_REQUEST_NULL;
                }
            }
            if (msg_buffers[k].done_req != MPI_REQUEST_NULL) {
                MPI_Test(&(msg_buffers[k].done_req), &flag, &st);
                if (flag) {
                    msg_buffers[k].done_req = MPI_REQUEST_NULL;
                }
            }
            if (msg_buffers[k].a2av_req == MPI_REQUEST_NULL
                && msg_buffers[k].done_req == MPI_REQUEST_NULL) {
                if (msg_buffers[k].done_count > done_count)
                    done_count = msg_buffers[k].done_count;
                if (done_count > mimir_world_size)
                    LOG_ERROR("done_count=%d, msg_done_count=%d, msg_done_flag=%d\n",
                              done_count, msg_buffers[k].done_count, msg_buffers[k].done_flag);
                LOG_PRINT(DBG_COMM, "Comm: MPI_Ialltoallv finish (token=%ld, done=%d).\n",
                          msg_buffers[k].msg_token, done_count);
                save_data(k);
                msg_buffers[k].msg_token = 0;
                msg_buffers[k].send_bytes = 0;
                msg_buffers[k].recv_bytes = 0;
                for (int i = 0; i < mimir_world_size; i++) {
                    msg_buffers[k].send_offset[i] = 0;
                    msg_buffers[k].recv_count[i] = 0;
                }
                msg_buffers[k].msg_state = ShuffleMsgComplete;
                pending_msg -= 1;
            }
        }

    }

    return;
}
