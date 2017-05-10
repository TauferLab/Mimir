/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_CONTEXT_H
#define MIMIR_CONTEXT_H

#include <typeinfo>

#include "log.h"
#include "mimir.h"
#include "config.h"
#include "interface.h"
#include "globals.h"
#include "log.h"
#include "stat.h"
#include "globals.h"
#include "tools.h"
#include "chunkmanager.h"
#include "kvcontainer.h"
#include "combinekvcontainer.h"
#include "kmvcontainer.h"
#include "collectiveshuffler.h"
#include "nbcollectiveshuffler.h"
#include "combinecollectiveshuffler.h"
#include "nbcombinecollectiveshuffler.h"
#include "filereader.h"
#include "filewriter.h"

#include <vector>
#include <string>

namespace MIMIR_NS {

enum OUTPUT_MODE {IMPLICIT_OUTPUT, EXPLICIT_OUTPUT};

#define MIMIR_COPY (MapCallback)0x01

template <typename KeyType, typename ValType>
class MimirContext {
  public:
    MimirContext(MPI_Comm mimir_comm,
                 MapCallback map_fn,
                 ReduceCallback reduce_fn,
                 std::vector<std::string> input_dir,
                 std::string output_dir,
                 RepartitionCallback repartition_fn = NULL,
                 CombineCallback combine_fn = NULL,
                 HashCallback hash_fn = NULL,
                 bool do_shuffle = true,
                 OUTPUT_MODE output_mode = EXPLICIT_OUTPUT) {
        KeyType tmpkey;
        ValType tmpval;

        if (type_name<decltype(tmpkey)>() == "char*")
            KTYPE = KVString;
        else
            KTYPE = sizeof(KeyType);

        if (type_name<decltype(tmpval)>() == "char*")
            VTYPE = KVString;
        else
            VTYPE = sizeof(ValType);

        _init(mimir_comm, map_fn, reduce_fn, combine_fn,
              hash_fn, repartition_fn, do_shuffle,
              input_dir, output_dir, output_mode);

        if (mimir_ctx_count == 0) {
            ::mimir_init();
            mimir_ctx_count +=1;
        }
    }

    MimirContext(MPI_Comm mimir_comm,
                 int ktype, int vtype,
                 MapCallback map_fn,
                 ReduceCallback reduce_fn,
                 std::vector<std::string> input_dir,
                 std::string output_dir,
                 RepartitionCallback repartition_fn = NULL,
                 CombineCallback combine_fn = NULL,
                 HashCallback hash_fn = NULL,
                 bool do_shuffle = true,
                 OUTPUT_MODE output_mode = EXPLICIT_OUTPUT) {
        KeyType tmpkey;
        ValType tmpval;

        if (type_name<decltype(tmpkey)>() != "char*" 
            || type_name<decltype(tmpval)>() != "char*")
            LOG_ERROR("The template type of <key,value> should be char*\n");
        KTYPE = (enum KVType)ktype;
        VTYPE = (enum KVType)vtype;

        //printf("vtype=%d, ktype=%d\n", vtype, vtype);

        _init(mimir_comm, map_fn, reduce_fn, combine_fn,
              hash_fn, repartition_fn, do_shuffle,
              input_dir, output_dir, output_mode);

        if (mimir_ctx_count == 0) {
            ::mimir_init();
            mimir_ctx_count +=1;
        }

    }


    ~MimirContext() {
        _uinit();
        mimir_ctx_count -= 1;
        if (mimir_ctx_count = 0) {
            ::mimir_finalize();
        }
    }

    void set_user_database(void *database) {
        user_database = (BaseDatabase*)database;
    }

    void *get_output_handle() {
        return database;
    }

    void insert_data(void *handle) {
        if (database != NULL) {
            BaseDatabase::subRef(database);
            database = NULL;
        }
        database = (BaseDatabase*)handle;
        BaseDatabase::addRef(database);
    }

    uint64_t map(void *ptr = NULL) {

        BaseShuffler *c = NULL;
        KVContainer *kv = NULL;
        Readable *input = NULL;
        Writable *output = NULL;
        FileReader<StringRecord> *reader = NULL;
        FileWriter *writer = NULL;
        ChunkManager *chunk_mgr = NULL;

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

        if (user_map == NULL)
            LOG_ERROR("Please set map callback\n");

        LOG_PRINT(DBG_GEN, "MapReduce: map start\n");

        // input from current database
        if (database != NULL) input = database;
        // input from files
        else {
            //InputSplit filelist;
            //for (std::vector<std::string>::const_iterator iter = input_dir.begin();
            //     iter != input_dir.end(); iter++) {
            //    std::string file = *(iter);
            //    filelist.add(file.c_str());
            //}
            StringRecord::set_whitespaces("\n");
            //InputSplit* splitinput = NULL;
            //if (user_repartition != NULL)
            //    splitinput = FileSplitter::getFileSplitter()->split(&filelist, BYSIZE);
            //else
            //    splitinput = FileSplitter::getFileSplitter()->split(&filelist, BYNAME);
            //splitinput->print();
            if (user_repartition != NULL) {
                if (WORK_STEAL) {
                    chunk_mgr = new StealChunkManager(mimir_ctx_comm, input_dir, BYSIZE);
                } else {
                    chunk_mgr = new ChunkManager(mimir_ctx_comm, input_dir, BYSIZE);
                }
            }
            else
                chunk_mgr = new ChunkManager(mimir_ctx_comm, input_dir, BYNAME);
            reader = FileReader<StringRecord>::getReader(mimir_ctx_comm, chunk_mgr, user_repartition);
            input = reader;
        }

        // output to customized database
        if (user_database != NULL) {
            output = user_database;
            // output to stage area
        } else if (user_reduce != NULL 
                   || output_mode == EXPLICIT_OUTPUT) {
            if (!user_combine) kv = new KVContainer();
            else kv = new CombineKVContainer(user_combine, ptr);
            output = kv;
            // output to files
        } else {
            writer = FileWriter::getWriter(mimir_ctx_comm, output_dir.c_str());
            output = writer;
        }

        // map with shuffle
        if (do_shuffle) {
            if (!user_combine) {
                if (SHUFFLE_TYPE == 0)
                    c = new CollectiveShuffler(mimir_ctx_comm, output, user_hash);
                else if (SHUFFLE_TYPE == 1)
                    c = new NBCollectiveShuffler(mimir_ctx_comm, output, user_hash);
                //else if (SHUFFLE_TYPE == 2)
                //    c = new NBShuffler(output, user_hash);
                else LOG_ERROR("Shuffle type %d error!\n", SHUFFLE_TYPE);
            } else {
                if (SHUFFLE_TYPE == 0)
                    c = new CombineCollectiveShuffler(mimir_ctx_comm, user_combine, ptr,
                                                      output, user_hash);
                else if (SHUFFLE_TYPE == 1)
                    c = new NBCombineCollectiveShuffler(mimir_ctx_comm, user_combine, ptr,
                                                        output, user_hash);
                else LOG_ERROR("Shuffle type %d error!\n", SHUFFLE_TYPE);
            }
            if (output) {
                if (writer != NULL) {
                    writer->set_shuffler(c);
                }
                output->open();
            }
            c->open();
            if (reader != NULL) {
                reader->set_shuffler(c);
            }
            if (input) input->open();
            if (user_map == (MapCallback)MIMIR_COPY) {
                ::mimir_copy(input, c, NULL);
            } else {
                user_map(input, c, ptr);
            }
            c->close();
            if (input) {
                input->close();
                input_records = input->get_record_count();
            }
            if (output) {
                output->close();
                kv_records = output->get_record_count();
            }
            delete c;
            // map without shuffle
        } else {
            if (output) output->open();
            if (input) input->open();
            if (user_map == (MapCallback)MIMIR_COPY) {
                ::mimir_copy(input, output, NULL);
            } else {
                user_map(input, output, ptr);
            }
            if (input) {
                input->close();
                input_records = input->get_record_count();
            }
            if (output) {
                output->close();
                kv_records = output->get_record_count();
            }
        }

        if (database != NULL) {
            BaseDatabase::subRef(database);
            database = NULL;
        } else {
            delete reader;
        }

        if (user_database != NULL) {
            database = user_database;
            BaseDatabase::addRef(database);
            user_database = NULL;
        } else if (user_reduce != NULL 
                   || output_mode == EXPLICIT_OUTPUT) {
            database = kv;
            BaseDatabase::addRef(database);
        } else {
            database = NULL;
            delete writer;
        }

        if (chunk_mgr != NULL) delete chunk_mgr;

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

        uint64_t total_records = 0;
        MPI_Allreduce(&kv_records, &total_records, 1,
                      MPI_INT64_T, MPI_SUM, mimir_ctx_comm);

        LOG_PRINT(DBG_GEN, "MapReduce: map done\n");

        return total_records;
    }

    uint64_t reduce(void *ptr = NULL) {
        KVContainer *kv = NULL;
        KMVContainer *kmv = NULL;
        FileWriter *writer = NULL;
        Readable *input = NULL;
        Writable *output = NULL;

        if (user_reduce == NULL) {
            LOG_ERROR("Please set reduce callback!\n");
        }

        if (database == NULL) {
            LOG_ERROR("No data to reduce!\n");
        }

        LOG_PRINT(DBG_GEN, "MapReduce: reduce start, %ld\n", Container::mem_bytes);

        input = database;
        // output to user database
        if (user_database != NULL) {
            output = user_database;
        // output to stage area
        } else if (output_mode == EXPLICIT_OUTPUT) {
            kv = new KVContainer();
            output = kv;
        // output to disk files
        } else {
            writer = FileWriter::getWriter(mimir_ctx_comm, output_dir.c_str());
            output = writer;
        }

        kmv = new KMVContainer();
        kmv->convert(input);
        BaseDatabase::subRef(database);
        database = NULL;

        kmv_records = kmv->get_record_count();

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_CVT);

        kmv->open();
        output->open();
        user_reduce(kmv, output, ptr);
        output->close();
        kmv->close();
        delete kmv;

        if (output) {
            output_records = output->get_record_count();
        }

        if (user_database != NULL) {
            database = user_database;
            BaseDatabase::addRef(database);
            user_database = NULL;
        } else if (output_mode == EXPLICIT_OUTPUT) {
            database = kv;
            BaseDatabase::addRef(database);
            // output to disk files
        } else {
            delete writer;
            database = NULL;
        }

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_RDC);
        LOG_PRINT(DBG_GEN, "MapReduce: done\n");

        uint64_t total_records = 0;
        MPI_Allreduce(&output_records, &total_records, 1,
                      MPI_INT64_T, MPI_SUM, mimir_ctx_comm);

        LOG_PRINT(DBG_GEN, "MapReduce: reduce done\n");

        return total_records;
    }

    uint64_t output(void *ptr = NULL) {
        if (database == NULL)
            LOG_ERROR("No data to output!\n");

        LOG_PRINT(DBG_GEN, "MapReduce: output start\n");

        FileWriter *writer = FileWriter::getWriter(mimir_ctx_comm, output_dir.c_str());
        KVRecord *record = NULL;
        database->open();
        writer->open();
        while ((record = (KVRecord*)(database->read())) != NULL) {
            writer->write(record);
        }
        writer->close();
        database->close();
        uint64_t output_records = writer->get_record_count();
        delete writer;

        BaseDatabase::subRef(database);
        database = NULL;

        uint64_t total_records = 0;
        MPI_Allreduce(&output_records, &total_records, 1,
                      MPI_INT64_T, MPI_SUM, mimir_ctx_comm);

        LOG_PRINT(DBG_GEN, "MapReduce: output done\n");

        return total_records;
    }

    //uint64_t reduce(void *ptr = NULL);
    //uint64_t output(void *ptr = NULL);
    //uint64_t mapreduce(Readable *input, Writable *output, void *ptr = NULL);

    uint64_t get_input_record_count() { return input_records; }
    uint64_t get_output_record_count() { return output_records; }
    uint64_t get_kv_record_count() { return kv_records; }
    uint64_t get_kmv_record_count() { return kmv_records; }

    void print_record_count () {
        printf("%d[%d] input=%ld, kv=%ld, kmv=%ld, output=%ld\n",
               mimir_ctx_rank, mimir_ctx_size, input_records,
               kv_records, kmv_records, output_records);
    }

  private:
    void _init(MPI_Comm ctx_comm,
               MapCallback map_fn,
               ReduceCallback reduce_fn,
               CombineCallback combine_fn, 
               HashCallback partition_fn,
               RepartitionCallback repartition_fn,
               bool do_shuffle,
               std::vector<std::string> &input_dir,
               std::string output_dir,
               OUTPUT_MODE output_mode) {

        MPI_Comm_dup(ctx_comm, &mimir_ctx_comm);
        MPI_Comm_rank(mimir_ctx_comm, &mimir_ctx_rank);
        MPI_Comm_size(mimir_ctx_comm, &mimir_ctx_size);

        this->user_map = map_fn;
        this->user_reduce = reduce_fn;
        this->user_combine = combine_fn;
        this->user_hash = partition_fn;
        this->user_repartition = repartition_fn;
        this->do_shuffle = do_shuffle;
        this->input_dir = input_dir;
        this->output_dir = output_dir;
        this->output_mode = output_mode;

        database = user_database = NULL;
        input_records = output_records = 0;
        kv_records = kmv_records = 0;
    }

    void _uinit() {
        MPI_Comm_free(&mimir_ctx_comm);
    }

    MapCallback         user_map;
    ReduceCallback      user_reduce;
    CombineCallback     user_combine;
    HashCallback        user_hash;
    RepartitionCallback user_repartition;
    bool                do_shuffle;

    std::vector<std::string> input_dir;
    std::string              output_dir;

    BaseDatabase*   database;
    BaseDatabase*   user_database;

    OUTPUT_MODE   output_mode;

    uint64_t    input_records;
    uint64_t    kv_records;
    uint64_t    kmv_records;
    uint64_t    output_records;

    MPI_Comm    mimir_ctx_comm;
    int         mimir_ctx_rank;
    int         mimir_ctx_size;

  private:
    static int  mimir_ctx_count;
};

template<typename KeyType, typename ValType>
int MimirContext<KeyType, ValType>::mimir_ctx_count = 0;

}

#endif
