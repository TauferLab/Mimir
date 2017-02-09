#include "mimircontext.h"
#include "kvcontainer.h"
#include "combinekvcontainer.h"
#include "kmvcontainer.h"
#include "collectiveshuffler.h"
#include "combinecollectiveshuffler.h"

using namespace MIMIR_NS;

uint64_t MimirContext::mapreduce(Readable *input, Writable *output, void *ptr) {
    BaseShuffler *c = NULL;
    KVContainer *kv = NULL;
    KMVContainer *kmv = NULL;
    Writable *map_output = output;

    if (user_map == NULL)
        LOG_ERROR("Please set map callback\n");

    if (user_reduce != NULL) {
        if (!user_combine) kv = new KVContainer();
        else kv = new CombineKVContainer(user_combine, ptr);
        map_output = kv;
    }

    LOG_PRINT(DBG_GEN, "MapReduce: map start\n");

    if (do_shuffle) {
        if (!user_combine)
            c = new CollectiveShuffler(map_output, user_hash);
        else
            c = new CombineCollectiveShuffler(user_combine, ptr,
                                              map_output, user_hash);
        map_output->open();
        input->open();
        c->open();
        user_map(input, c, ptr);
        c->close();
        input->close();
        map_output->close();
        delete c;
    } else{
        map_output->open();
        input->open();
        user_map(input, map_output, ptr);
        input->close();
        map_output->close();
    }

    if (user_reduce != NULL) {
        LOG_PRINT(DBG_GEN, "MapReduce: reduce start\n");

        kmv = new KMVContainer();
        kmv->convert(kv);
        delete kv;
        user_reduce(kmv, output, ptr);
        delete kmv;
    }

    LOG_PRINT(DBG_GEN, "MapReduce: done\n");

    return 0;
}

