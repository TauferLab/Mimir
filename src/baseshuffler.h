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
#include "serializer.h"

namespace MIMIR_NS {

template <typename KeyType, typename ValType>
class BaseShuffler : public Writable<KeyType, ValType> {
public:
    BaseShuffler(MPI_Comm comm,
                 Writable<KeyType, ValType> *out,
                 HashCallback user_hash,
                 int keycount, int valcount) {

        if (out == NULL) LOG_ERROR("Output shuffler cannot be NULL!\n");

        this->shuffle_comm = comm;
        this->out = out;
        this->user_hash = user_hash;
        this->keycount = keycount;
        this->valcount = valcount;

        MPI_Comm_rank(shuffle_comm, &shuffle_rank);
        MPI_Comm_size(shuffle_comm, &shuffle_size);

        done_flag = 0;
        done_count = 0;
	kvcount = 0;
    }

    virtual ~BaseShuffler() {
    }

    virtual int open() = 0;
    virtual int write(KeyType *key, ValType *val) = 0;
    virtual void close() = 0;
    virtual void make_progress(bool issue_new = false) = 0;
    virtual uint64_t get_record_count() { return kvcount; }
protected:
    int get_target_rank(KeyType *key) {
        char tmpkey[MAX_RECORD_SIZE];
        int keysize = Serializer::get_bytes<KeyType>(key, keycount);
        if (keysize > MAX_RECORD_SIZE) LOG_ERROR("The key is too long!\n");
        Serializer::to_bytes<KeyType>(key, keycount, tmpkey, 1024);

        int target = 0;
        if (user_hash != NULL) {
            target = user_hash((const char*)key, keycount) % shuffle_size;
        }
        else {
            uint32_t hid = 0;
            hid = hashlittle(tmpkey, keysize, 0);
            target = (int) (hid % (uint32_t) shuffle_size);
        }

        if (target < 0 || target >= shuffle_size) {
            LOG_ERROR("Error: target process (%d) isn't correct!\n", target);
        }

        return target;
    }

    HashCallback user_hash;
    Writable<KeyType,ValType> *out;

    int done_flag, done_count;

    uint64_t kvcount;

    MPI_Comm shuffle_comm;
    int      shuffle_rank;
    int      shuffle_size;

    int      keycount, valcount;
};

}
#endif
