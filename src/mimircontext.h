/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_CONTEXT_H
#define MIMIR_CONTEXT_H

#include "config.h"
#include "log.h"
#include "interface.h"

namespace MIMIR_NS {

class MimirContext {
  public:
    MimirContext() {
        user_map = NULL;
        user_reduce = NULL;
        user_combine = NULL;
        user_hash = NULL;
        do_shuffle = true;
    }

    ~MimirContext() {
    }

    void set_shuffle_flag(bool do_shuffle = true) {
        this->do_shuffle = do_shuffle;
    }

    void set_map_callback(MapCallback user_map) {
        this->user_map = user_map;
    }

    void set_reduce_callback(ReduceCallback user_reduce) {
        this->user_reduce = user_reduce;
    }

    void set_combine_callback(CombineCallback user_combine) {
        this->user_combine = user_combine;
    }

    void set_hash_callback(HashCallback user_hash) {
        this->user_hash = user_hash;
    }

    void set_key_length(int keysize) {
        KTYPE = keysize;
    }

    void set_val_length(int valsize) {
        VTYPE = valsize;
    }

    uint64_t mapreduce(Readable *input, Writable *output, void *ptr = NULL);

  private:
    bool        do_shuffle;
    MapCallback user_map;
    ReduceCallback user_reduce;
    CombineCallback user_combine;
    HashCallback user_hash;
};

}

#endif
