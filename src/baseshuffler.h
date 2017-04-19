/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_BASE_SHUFFLER_H
#define MIMIR_BASE_SHUFFLER_H

#include <vector>
#include "config.h"
#include "interface.h"
#include "hashbucket.h"

namespace MIMIR_NS {

class BaseShuffler : public Writable {
public:
    BaseShuffler(Writable *out, HashCallback user_hash = NULL) {
        if (out == NULL) LOG_ERROR("Output shuffler cannot be NULL!\n");
        this->out = out;
        this->user_hash = user_hash;
        done_flag = 0;
        done_count = 0;
	kvcount = 0;
    }
    virtual ~BaseShuffler() {
    }

    virtual bool open() = 0;
    virtual void write(BaseRecordFormat *) = 0;
    virtual void close() = 0;
    virtual void make_progress(bool issue_new = false) = 0;
    virtual uint64_t get_record_count() { return kvcount; }
protected:
    int get_target_rank(const char *key, int keysize) {
        int target = 0;
        if (user_hash != NULL) {
            target = user_hash(key, keysize) % mimir_world_size;
        }
        else {
            uint32_t hid = 0;
            hid = hashlittle(key, keysize, 0);
            target = (int) (hid % (uint32_t) mimir_world_size);
        }

        if (target < 0 || target >= mimir_world_size) {
            LOG_ERROR("Error: target process (%d) isn't correct!\n", target);
        }

        return target;
    }

    HashCallback user_hash;
    Writable *out;

    int done_flag, done_count;

    uint64_t kvcount;
};

}
#endif
