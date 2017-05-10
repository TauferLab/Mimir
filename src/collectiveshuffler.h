/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_COLLECTIVE_SHUFFLER_H
#define MIMIR_COLLECTIVE_SHUFFLER_H

#include <mpi.h>
#include <vector>

#include "container.h"
#include "baseshuffler.h"

namespace MIMIR_NS {

class CollectiveShuffler : public BaseShuffler {
public:
    CollectiveShuffler(MPI_Comm comm, Writable *out, HashCallback user_hash);
    virtual ~CollectiveShuffler();

    virtual bool open();
    virtual void close();
    virtual void write(BaseRecordFormat *record);
    virtual void make_progress(bool issue_new = false) { exchange_kv(); }

protected:
    void wait();
    void exchange_kv();
    void save_data();

    int64_t buf_size;

    MPI_Datatype comm_type;
    int type_log_bytes;
    int *a2a_s_count;
    int *a2a_s_displs;
    int *a2a_r_count;
    int *a2a_r_displs;

    char *send_buffer;
    int  *send_offset;
    char *recv_buffer;
    int  *recv_count;

    KVRecord kv;
};

}
#endif
