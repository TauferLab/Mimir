#ifndef MIMIR_CONTEXT_H
#define MIMIR_CONTEXT_H

#include "interface.h"
#include "kvcontainer.h"
#include "kmvcontainer.h"
#include "baseshuffler.h"

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

    void set_map_callback(MapCallback user_map) {
        this->user_map = user_map;
    }

    void set_reduce_callback(ReduceCallback user_reduce) {
        this->user_reduce = user_reduce;
    }

    void set_combine_callback(CombineCallback user_combine) {
        this->user_combine = user_combine;
    }

    uint64_t mapreduce(Readable *input, Writable *output, void *ptr) {
        BaseShuffler *c = NULL;
        KVContainer *kv = NULL;
        KMVContainer *kmv = NULL;
        BaseOutput *map_output = output;

        if (user_map == NULL)
            LOG_ERROR("Please set map callback\n");

        if (user_reduce != NULL) {
            kv = new KVContainer(ksize, vsize);
            map_output = kv;
        }

        LOG_PRINT(DBG_GEN, "MapReduce: map start\n");

        // map phase
        if (do_shuffle) {
            //c = Shuffler::Create(KV_EXCH_COMM);
            //c->setup(COMM_BUF_SIZE, map_output, user_combine, user_hash);
            c->open();
            input->open();
            user_map(input, c, ptr);
            input->close();
            c->close();
            delete c;
        } else{
            map_output->open();
            input->open();
            user_map(input, map_output, ptr);
            input->close();
            map_output->close();
        }

        // reduce phase
        if (user_reduce != NULL) {
            LOG_PRINT(DBG_GEN, "MapReduce: reduce start\n");

            kmv = new KMVContainer(ksize, vsize);
            kmv->convert(kv);
            delete kv;
            user_reduce(kmv, output, ptr);
            delete kmv;
         }

        LOG_PRINT(DBG_GEN, "MapReduce: done\n");

        return 0;
    }
  private:
    bool        do_shuffle;
    MapCallback user_map;
    ReduceCallback user_reduce;
    CombineCallback user_combine;
    HashCallback user_hash;
};

}

#endif
