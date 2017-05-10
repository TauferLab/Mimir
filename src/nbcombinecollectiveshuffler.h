/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_COMBINE_NB_COLLECTIVE_SHUFFLER_H
#define MIMIR_COMBINE_NB_COLLECTIVE_SHUFFLER_H

#include <mpi.h>
#include <vector>

#include <unordered_map>

#include "container.h"
#include "nbcollectiveshuffler.h"

namespace MIMIR_NS {

class NBCombineCollectiveShuffler
    : public NBCollectiveShuffler, public Combinable {
public:
    NBCombineCollectiveShuffler(MPI_Comm comm,
                                CombineCallback user_combine,
                                void *user_ptr,
                                Writable *out,
                                HashCallback user_hash);
    virtual ~NBCombineCollectiveShuffler();

    virtual bool open();
    virtual void close();
    virtual void write(BaseRecordFormat *record);
    virtual void update(BaseRecordFormat *);
    virtual void make_progress(bool issue_new = false) {
        if(issue_new && pending_msg == 0) {
            garbage_collection();
            start_kv_exchange();
        }
        push_kv_exchange();
    }

protected:
    void garbage_collection();

    CombineCallback user_combine;
    void *user_ptr;
    std::unordered_map<char*, int> slices;
    CombinerHashBucket *bucket;
    CombinerUnique *u;
};

}
#endif
