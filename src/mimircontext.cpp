/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#include "log.h"
#include "stat.h"
#include "globals.h"
#include "mimircontext.h"
#include "kvcontainer.h"
#include "combinekvcontainer.h"
#include "kmvcontainer.h"
#include "collectiveshuffler.h"
#include "nbcollectiveshuffler.h"
#include "combinecollectiveshuffler.h"
#include "nbcombinecollectiveshuffler.h"
#include "filereader.h"

using namespace MIMIR_NS;

uint64_t MimirContext::mapreduce(Readable *input, Writable *output, void *ptr) {
    BaseShuffler *c = NULL;
    KVContainer *kv = NULL;
    KMVContainer *kmv = NULL;
    Writable *map_output = output;

    TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

    if (user_map == NULL)
        LOG_ERROR("Please set map callback\n");

    if (user_reduce != NULL) {
        if (!user_combine) kv = new KVContainer();
        else kv = new CombineKVContainer(user_combine, ptr);
        map_output = kv;
    }

    LOG_PRINT(DBG_GEN, "MapReduce: map start\n");

    if (do_shuffle) {
        if (!user_combine) {
            if (SHUFFLE_TYPE == 0)
                c = new CollectiveShuffler(map_output, user_hash);
            else if (SHUFFLE_TYPE == 1)
                c = new NBCollectiveShuffler(map_output, user_hash);
            else LOG_ERROR("Shuffle type %d error!\n", SHUFFLE_TYPE);
        } else {
            if (SHUFFLE_TYPE == 0)
                c = new CombineCollectiveShuffler(user_combine, ptr,
                                                  map_output, user_hash);
            else if (SHUFFLE_TYPE == 1)
                c = new NBCombineCollectiveShuffler(user_combine, ptr,
                                                    map_output, user_hash);
            else LOG_ERROR("Shuffle type %d error!\n", SHUFFLE_TYPE);
        }
        map_output->open();
        c->open();
        if (input->get_object_name() == "FileReader") {
            FileReader<ByteRecord> *reader = (FileReader<ByteRecord>*)input;
            reader->set_shuffler(c);
        }
        input->open();
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

    TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

    if (user_reduce != NULL) {
        LOG_PRINT(DBG_GEN, "MapReduce: reduce start, %ld\n", Container::mem_bytes);

        kmv = new KMVContainer();
        kmv->convert(kv);
        delete kv;

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_CVT);

        kmv->open();
        output->open();
        user_reduce(kmv, output, ptr);
        output->close();
        kmv->close();
        delete kmv;

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_RDC);
    }

    LOG_PRINT(DBG_GEN, "MapReduce: done\n");

    return output->get_record_count();
}
