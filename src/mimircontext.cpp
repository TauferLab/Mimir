/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#include "mimircontext.h"
#include "kvcontainer.h"
#include "combinekvcontainer.h"
#include "kmvcontainer.h"
#include "collectiveshuffler.h"
#include "combinecollectiveshuffler.h"
#include "filereader.h"
#include "stat.h"
#include "globals.h"

using namespace MIMIR_NS;

uint64_t MimirContext::mapreduce(Readable *input, Writable *output, void *ptr) {
    BaseShuffler *c = NULL;
    KVContainer *kv = NULL;
    KMVContainer *kmv = NULL;
    Writable *map_output = output;
    uint64_t record_count = 0;
    uint64_t total_count = 0;

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

    if (user_reduce != NULL) {
        LOG_PRINT(DBG_GEN, "MapReduce: reduce start\n");

        kmv = new KMVContainer();
        kmv->convert(kv);
        delete kv;
	kmv->open();
        output->open();
        user_reduce(kmv, output, ptr);
        output->close();
        kmv->close();
        delete kmv;
    }

    //PROFILER_RECORD_TIME_START;
    //record_count = output->get_record_count();
    //MPI_Allreduce(&record_count, &total_count, 1, 
    //		  MPI_INT64_T, MPI_SUM, mimir_world_comm);
    //PROFILER_RECORD_TIME_END(TIMER_COMM_RDC);

    LOG_PRINT(DBG_GEN, "MapReduce: done\n");

    return output->get_record_count();
}
