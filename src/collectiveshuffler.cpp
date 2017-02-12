#include <stdio.h>
#include <stdlib.h>
#include "log.h"
#include "config.h"
#include "collectiveshuffler.h"
#include "const.h"
#include "memory.h"
#include "kvcontainer.h"

#include "globals.h"
#include "hash.h"
#include "stat.h"
#include "log.h"

#include "recordformat.h"

using namespace MIMIR_NS;

CollectiveShuffler::CollectiveShuffler(Writable *out, HashCallback user_hash)
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
}

CollectiveShuffler::~CollectiveShuffler()
{
}

bool CollectiveShuffler::open() {
    done_flag = 0;
    done_count = 0;

    send_buffer = (char*)mem_aligned_malloc(MEMPAGE_SIZE, buf_size * mimir_world_size);
    recv_buffer = (char*)mem_aligned_malloc(MEMPAGE_SIZE, buf_size * mimir_world_size);
    send_offset = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * mimir_world_size);
    recv_count = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * mimir_world_size);
    a2a_s_count = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * mimir_world_size);
;
    a2a_s_displs = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * mimir_world_size);
;
    a2a_r_count = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * mimir_world_size);
;
    a2a_r_displs = (int*)mem_aligned_malloc(MEMPAGE_SIZE, sizeof(int) * mimir_world_size);
;

    MPI_Type_contiguous((0x1 << type_log_bytes), MPI_BYTE, &comm_type);
    MPI_Type_commit(&comm_type);

    LOG_PRINT(DBG_GEN, "CollectiveShuffler open: buf_size=%ld\n", buf_size);

    return true;
}

void CollectiveShuffler::close() {
    wait();

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

void CollectiveShuffler::write(BaseRecordFormat *record)
{
    int target = get_target_rank(((KVRecord*)record)->get_key(), 
                                 ((KVRecord*)record)->get_key_size());

    int kvsize = record->get_record_size();
    if (kvsize > buf_size)
        LOG_ERROR("Error: KV size (%d) is larger than buf_size (%ld)\n", 
                  kvsize, buf_size);

    if ((int64_t)send_offset[target] + (int64_t)kvsize > buf_size)
        exchange_kv();

    char *buffer = send_buffer + target * (int64_t)buf_size + send_offset[target];
    kv.set_buffer(buffer);
    kv.convert((KVRecord*)record);
    send_offset[target] += kvsize;
    return;
}

void CollectiveShuffler::wait()
{
    done_flag = 1;

    do {
        exchange_kv();
    } while (done_count < mimir_world_size);

    LOG_PRINT(DBG_COMM, "Comm: finish wait.\n");
}

void CollectiveShuffler::save_data()
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
	    kvcount++;
	    src_buf += kvsize;
            count += kvsize;
        }
        int padding = recv_count[k] & ((0x1 << type_log_bytes) - 0x1);
        src_buf += padding;
    }
}

void CollectiveShuffler::exchange_kv()
{
    uint64_t sendcount = 0, recvcount = 0;

    for (int i = 0; i < mimir_world_size; i++)
        sendcount += (uint64_t) send_offset[i];
    PROFILER_RECORD_COUNT(COUNTER_SEND_BYTES, sendcount, OPSUM);

    TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

    PROFILER_RECORD_TIME_START;
    MPI_Alltoall(send_offset, 1, MPI_INT,
                 recv_count, 1, MPI_INT, mimir_world_comm);
    PROFILER_RECORD_TIME_END(TIMER_COMM_A2A);

    TRACKER_RECORD_EVENT(EVENT_COMM_ALLTOALL);

    recvcount = (uint64_t) recv_count[0];
    for (int i = 1; i < mimir_world_size; i++) {
        recvcount += (int64_t) recv_count[i];
    }

    for (int i = 0; i < mimir_world_size; i++) {
        a2a_s_count[i] = (send_offset[i] + (0x1 << type_log_bytes) - 1)
            >> type_log_bytes;
        a2a_r_count[i] = (recv_count[i] + (0x1 << type_log_bytes) - 1)
            >> type_log_bytes;
        a2a_s_displs[i] = (i * (int) buf_size) >> type_log_bytes;
    }
    a2a_r_displs[0] = 0;
    for (int i = 1; i < mimir_world_size; i++)
        a2a_r_displs[i] = a2a_r_displs[i - 1] + a2a_r_count[i - 1];

    PROFILER_RECORD_TIME_START;
    MPI_Alltoallv(send_buffer, a2a_s_count, a2a_s_displs, comm_type,
                  recv_buffer, a2a_r_count, a2a_r_displs, comm_type, 
                  mimir_world_comm);
    PROFILER_RECORD_TIME_END(TIMER_COMM_A2AV);

    TRACKER_RECORD_EVENT(EVENT_COMM_ALLTOALLV);

    LOG_PRINT(DBG_COMM, "Comm: receive data. (count=%ld)\n", recvcount);

    PROFILER_RECORD_COUNT(COUNTER_RECV_BYTES, (uint64_t) recvcount, OPSUM);
    if (recvcount > 0) {
        save_data();
    }

    for (int i = 0; i < mimir_world_size; i++) send_offset[i] = 0;

    PROFILER_RECORD_TIME_START;
    MPI_Allreduce(&done_flag, &done_count, 1, MPI_INT, MPI_SUM, mimir_world_comm);
    PROFILER_RECORD_TIME_END(TIMER_COMM_RDC);

    TRACKER_RECORD_EVENT(EVENT_COMM_ALLREDUCE);

    LOG_PRINT(DBG_COMM, "Comm: exchange KV. (send count=%ld, recv count=%ld, done count=%d)\n", sendcount, recvcount, done_count);
}
