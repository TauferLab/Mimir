#ifndef ALLTOALL_H
#define ALLTOALL_H

#include <mpi.h>
#include <vector>

#include "container.h"
#include "baseshuffler.h"

namespace MIMIR_NS {

class CollectiveShuffler : public BaseShuffler {
public:
    CollectiveShuffler(Writable *out, HashCallback user_hash);
    virtual ~CollectiveShuffler();

    virtual bool open();
    virtual void close();
    virtual void add(const char*, int, const char*, int);

private:
    virtual void wait();
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
};

}
#endif
