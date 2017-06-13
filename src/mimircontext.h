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
#include <type_traits>

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

//#define MIMIR_COPY (MapCallback)0x01

template <typename KeyType, typename ValType,
         typename InKeyType = KeyType, typename InValType = ValType,
         typename OutKeyType= KeyType, typename OutValType = ValType>
class MimirContext {
  public:
    MimirContext(MPI_Comm mimir_comm,
                 void (*map_fn)(Readable<InKeyType,InValType> *input, 
                                Writable<KeyType,ValType> *output, void *ptr),
                 void (*reduce_fn)(Readable<KeyType,ValType> *input, 
                                   Writable<OutKeyType,OutValType> *output, void *ptr),
                 std::vector<std::string> input_dir,
                 std::string output_dir,
                 RepartitionCallback repartition_fn = NULL,
                 void (*combine_fn)(Combinable<KeyType,ValType> *output,
                                    KeyType* key, ValType* val1, ValType* val2, void *ptr) = NULL,
                 int (*hash_fn)(KeyType* key, ValType *val, int npartition) = NULL,
                 bool do_shuffle = true,
                 OUTPUT_MODE output_mode = EXPLICIT_OUTPUT) {

        if (repartition_fn == NULL) {
            _init(1, 1, 1, 1, 1, 1,
                  mimir_comm, map_fn, reduce_fn, combine_fn,
                  hash_fn, text_file_repartition, do_shuffle,
                  input_dir, output_dir, output_mode);
        } else {
            _init(1, 1, 1, 1, 1, 1,
                  mimir_comm, map_fn, reduce_fn, combine_fn,
                  hash_fn, repartition_fn, do_shuffle,
                  input_dir, output_dir, output_mode);
        }
    }

    MimirContext(MPI_Comm mimir_comm,
                 void (*map_fn)(Readable<InKeyType,InValType> *input, 
                                Writable<KeyType,ValType> *output, void *ptr),
                 void (*reduce_fn)(Readable<KeyType,ValType> *input, 
                                   Writable<OutKeyType,OutValType> *output, void *ptr),
                 void (*combine_fn)(Combinable<KeyType,ValType> *output,
                                    KeyType* key, ValType* val1, ValType* val2, void *ptr) = NULL,
                 int (*hash_fn)(KeyType* key, ValType *val, int npartition) = NULL,
                 bool do_shuffle = true,
                 OUTPUT_MODE output_mode = EXPLICIT_OUTPUT) {

        std::vector<std::string> input_dir;
        std::string output_dir;

        _init(1, 1, 1, 1, 1, 1,
              mimir_comm, map_fn, reduce_fn, combine_fn,
              hash_fn, NULL, do_shuffle,
              input_dir, output_dir, output_mode);
    }

    MimirContext(MPI_Comm mimir_comm,
                 int keycount, int valcount,
                 int inkeycount, int invalcount,
                 int outkeycount, int outvalcount,
                 void (*map_fn)(Readable<InKeyType,InValType> *input, 
                                Writable<KeyType,ValType> *output, void *ptr),
                 void (*reduce_fn)(Readable<KeyType,ValType> *input, 
                                   Writable<OutKeyType,OutValType> *output, void *ptr),
                 std::vector<std::string> input_dir,
                 std::string output_dir,
                 RepartitionCallback repartition_fn = NULL,
                 void (*combine_fn)(Combinable<KeyType,ValType> *output,
                                    KeyType* key, ValType* val1, ValType* val2, void *ptr) = NULL,
                 int (*hash_fn)(KeyType* key, ValType* val, int npartition) = NULL,
                 bool do_shuffle = true,
                 OUTPUT_MODE output_mode = EXPLICIT_OUTPUT) {

        if (repartition_fn == NULL) {
            _init(keycount, valcount, inkeycount, invalcount,
                  outkeycount, outvalcount,
                  mimir_comm, map_fn, reduce_fn, combine_fn,
                  hash_fn, text_file_repartition, do_shuffle,
                  input_dir, output_dir, output_mode);
        } else {
            _init(keycount, valcount, inkeycount, invalcount,
                  outkeycount, outvalcount,
                  mimir_comm, map_fn, reduce_fn, combine_fn,
                  hash_fn, repartition_fn, do_shuffle,
                  input_dir, output_dir, output_mode);
        }
    }


    ~MimirContext() {
        _uinit();
    }

    void set_map_callback(void (*map_fn)(Readable<InKeyType,InValType> *input, 
                                         Writable<KeyType,ValType> *output, void *ptr)) {
        this->user_map = map_fn;
    }

    void set_user_database(void *database) {
        user_database = (BaseDatabase<KeyType,ValType>*)database;
    }

    void *get_output_handle() {
        return database;
    }

    void insert_data(void *handle) {
        if (database != NULL) {
            BaseDatabase<KeyType,ValType>::subRef(database);
            database = NULL;
        }
        in_database = (BaseDatabase<InKeyType,InValType>*)handle;
        BaseDatabase<InKeyType,InValType>::addRef(in_database);
    }

    uint64_t map(void *ptr = NULL) {

        BaseShuffler<KeyType,ValType> *c = NULL;
        KVContainer<KeyType,ValType> *kv = NULL;
        Readable<InKeyType,InValType> *input = NULL;
        Writable<KeyType,ValType> *output = NULL;
        FileReader<TextFileFormat,KeyType,ValType,InKeyType,InValType> *reader = NULL;
        FileWriter<KeyType,ValType> *writer = NULL;
        ChunkManager<KeyType,ValType> *chunk_mgr = NULL;

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

        if (user_map == NULL)
            LOG_ERROR("Please set map callback\n");

        LOG_PRINT(DBG_GEN, "MapReduce: map start\n");

        // input from current database
        if (database != NULL) input = dynamic_cast<Readable<InKeyType,InValType>*>(database);
        else if (in_database != NULL) input = in_database;
        // input from files
        else if (input_dir.size() > 0) {
            if (user_repartition != NULL) {
                if (WORK_STEAL) {
                    chunk_mgr = new StealChunkManager<KeyType,ValType>(mimir_ctx_comm, input_dir, BYSIZE);
                } else {
                    chunk_mgr = new ChunkManager<KeyType,ValType>(mimir_ctx_comm, input_dir, BYSIZE);
                }
            }
            else
                chunk_mgr = new ChunkManager<KeyType,ValType>(mimir_ctx_comm, input_dir, BYNAME);
            reader = FileReader<TextFileFormat,KeyType,ValType,InKeyType,InValType>::getReader(mimir_ctx_comm, chunk_mgr, user_repartition);
            input = reader;
        } else {
            input = NULL;
        }

        // output to customized database
        if (user_database != NULL) {
            output = user_database;
        // output to stage area
        } else if (user_reduce != NULL 
                   || output_mode == EXPLICIT_OUTPUT) {
            if (!user_combine) kv = new KVContainer<KeyType,ValType>(keycount, valcount);
            else kv = new CombineKVContainer<KeyType,ValType>(user_combine, ptr, keycount, valcount);
            output = kv;
        // output to files
        } else {
            writer = FileWriter<KeyType,ValType>::getWriter(mimir_ctx_comm, output_dir.c_str());
            writer->set_file_format(outfile_format.c_str());
            output = writer;
        }

        // map with shuffle
        if (do_shuffle) {
            if (!user_combine) {
                if (SHUFFLE_TYPE == 0)
                    c = new CollectiveShuffler<KeyType,ValType>(mimir_ctx_comm, output, user_hash, keycount, valcount);
                else if (SHUFFLE_TYPE == 1)
                    c = new NBCollectiveShuffler<KeyType,ValType>(mimir_ctx_comm, output, user_hash, keycount, valcount);
                //else if (SHUFFLE_TYPE == 2)
                //    c = new NBShuffler(output, user_hash);
                else LOG_ERROR("Shuffle type %d error!\n", SHUFFLE_TYPE);
            } else {
                if (SHUFFLE_TYPE == 0)
                    c = new CombineCollectiveShuffler<KeyType,ValType>(mimir_ctx_comm, user_combine, ptr,
                                                      output, user_hash, keycount, valcount);
                else if (SHUFFLE_TYPE == 1)
                    c = new NBCombineCollectiveShuffler<KeyType,ValType>(mimir_ctx_comm, user_combine, ptr,
                                                        output, user_hash, keycount, valcount);
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
            //if (user_map == (MapCallback)MIMIR_COPY) {
            //  ::mimir_copy(input, c, NULL);
            //} else {
            user_map(input, c, ptr);
            //}
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
            //if (user_map == (MapCallback)MIMIR_COPY) {
            //  ::mimir_copy(input, output, NULL);
            //} else {
                user_map(input, output, ptr);
            //}
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
            BaseDatabase<KeyType,ValType>::subRef(database);
            database = NULL;
        } else {
            delete reader;
        }

        if (user_database != NULL) {
            database = user_database;
            BaseDatabase<KeyType,ValType>::addRef(database);
            user_database = NULL;
        } else if (user_reduce != NULL 
                   || output_mode == EXPLICIT_OUTPUT) {
            database = kv;
            BaseDatabase<KeyType,ValType>::addRef(database);
        } else {
            database = NULL;
            delete writer;
        }

        if (chunk_mgr != NULL) delete chunk_mgr;

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

        uint64_t total_records = 0;
        MPI_Allreduce(&kv_records, &total_records, 1,
                      MPI_INT64_T, MPI_SUM, mimir_ctx_comm);

        LOG_PRINT(DBG_GEN, "MapReduce: map done (KVs=%ld)\n", kv_records);

        return total_records;
    }

    uint64_t reduce(void *ptr = NULL) {
        KVContainer<OutKeyType,OutValType> *kv = NULL;
        KMVContainer<KeyType,ValType> *kmv = NULL;
        FileWriter<OutKeyType,OutValType> *writer = NULL;
        Readable<KeyType,ValType> *input = NULL;
        Writable<OutKeyType,OutValType> *output = NULL;

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
            output = dynamic_cast<Writable<OutKeyType,OutValType>*>(user_database);
        // output to stage area
        } else if (output_mode == EXPLICIT_OUTPUT) {
            kv = new KVContainer<OutKeyType,OutValType>(keycount, valcount);
            output = kv;
        // output to disk files
        } else {
            writer = FileWriter<OutKeyType,OutValType>::getWriter(mimir_ctx_comm, output_dir.c_str());
            writer->set_file_format(outfile_format.c_str());
            output = writer;
        }

        kmv = new KMVContainer<KeyType,ValType>(keycount, valcount);
        kmv->convert(input);
        BaseDatabase<KeyType,ValType>::subRef(database);
        database = NULL;

        kmv_records = kmv->get_record_count();

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_CVT);

        kmv->open();
        output->open();
        KMVItem<KeyType,ValType>* item = NULL;
        while ((item = kmv->read()) != NULL) {
            item->open();
            user_reduce(item, output, ptr);
            item->close();
        }
        output->close();
        kmv->close();
        delete kmv;

        if (output) {
            output_records = output->get_record_count();
        }

        if (user_database != NULL) {
            database = user_database;
            BaseDatabase<KeyType,ValType>::addRef(database);
            user_database = NULL;
        } else if (output_mode == EXPLICIT_OUTPUT) {
            database = dynamic_cast<BaseDatabase<KeyType,ValType>*>(kv);
            BaseDatabase<KeyType,ValType>::addRef(database);
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

        typename SafeType<KeyType>::type key[keycount];
        typename SafeType<ValType>::type val[valcount];

        if (database == NULL)
            LOG_ERROR("No data to output!\n");

        LOG_PRINT(DBG_GEN, "MapReduce: output start\n");

        FileWriter<OutKeyType, OutValType> *writer = FileWriter<OutKeyType, OutValType>::getWriter(mimir_ctx_comm, output_dir.c_str());
        writer->set_file_format(outfile_format.c_str());
        database->open();
        writer->open();
        //output_fn(database, writer, ptr);
        while (database->read(key, val) == 0) {
            //printf("key=%s, val=%ld\n", key[0], val[0]);
            writer->write(key, val);
        }
        writer->close();
        database->close();
        uint64_t output_records = writer->get_record_count();
        delete writer;

        BaseDatabase<KeyType,ValType>::subRef(database);
        database = NULL;

        uint64_t total_records = 0;
        MPI_Allreduce(&output_records, &total_records, 1,
                      MPI_INT64_T, MPI_SUM, mimir_ctx_comm);

        LOG_PRINT(DBG_GEN, "MapReduce: output done\n");

        return total_records;
    }

    uint64_t scan(void (*scan_fn)(KeyType *key, ValType *val, void *ptr),
                  void *ptr = NULL) {

        typename SafeType<KeyType>::type key[keycount];
        typename SafeType<ValType>::type val[valcount];

        if (database == NULL)
            LOG_ERROR("No data to output!\n");

        LOG_PRINT(DBG_GEN, "MapReduce: scan start\n");

        database->open();
        while (database->read(key, val) == 0) {
            scan_fn(key, val, ptr);
        }
        database->close();

        LOG_PRINT(DBG_GEN, "MapReduce: scan done\n");

        return 0;
    }

    //uint64_t reduce(void *ptr = NULL);
    //uint64_t output(void *ptr = NULL);
    //uint64_t mapreduce(Readable *input, Writable *output, void *ptr = NULL);

    uint64_t get_input_record_count() { return input_records; }
    uint64_t get_output_record_count() { return output_records; }
    uint64_t get_kv_record_count() { return kv_records; }
    uint64_t get_kmv_record_count() { return kmv_records; }

    void set_outfile_format(const char *format) {
        outfile_format = format;
    }

    void print_record_count () {
        printf("%d[%d] input=%ld, kv=%ld, kmv=%ld, output=%ld\n",
               mimir_ctx_rank, mimir_ctx_size, input_records,
               kv_records, kmv_records, output_records);
    }

  private:
    void _init(int keycount, int valcount,
               int inkeycount, int invalcount,
               int outkeycount, int outvalcount,
               MPI_Comm ctx_comm,
               void (*map_fn)(Readable<InKeyType,InValType> *input, 
                              Writable<KeyType,ValType> *output, void *ptr),
               void (*reduce_fn)(Readable<KeyType,ValType> *input, 
                                 Writable<OutKeyType,OutValType> *output, void *ptr),
               void (*combine_fn)(Combinable<KeyType,ValType> *output,
                                  KeyType* key, ValType* val1, ValType* val2, void *ptr),
               int (*partition_fn)(KeyType* key, ValType* val, int npartition),
               RepartitionCallback repartition_fn,
               bool do_shuffle,
               std::vector<std::string> &input_dir,
               std::string output_dir,
               OUTPUT_MODE output_mode) {

        this->keycount = keycount;
        this->valcount = valcount;
        this->inkeycount = inkeycount;
        this->invalcount = invalcount;
        this->outkeycount = outkeycount;
        this->outvalcount = outvalcount;

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
        in_database = NULL;
        input_records = output_records = 0;
        kv_records = kmv_records = 0;

        if (mimir_ctx_count == 0) {
            ::mimir_init();
        }
        mimir_ctx_count +=1;
    }

    void _uinit() {
        MPI_Comm_free(&mimir_ctx_comm);
        mimir_ctx_count -= 1;
        if (mimir_ctx_count == 0) {
            ::mimir_finalize();
        }
    }

    void (*user_map)(Readable<InKeyType,InValType> *input, 
                     Writable<KeyType,ValType> *output, void *ptr);
    void (*user_reduce)(Readable<KeyType,ValType> *input, 
                        Writable<OutKeyType,OutValType> *output, void *ptr);
    void (*user_combine)(Combinable<KeyType,ValType> *output,
                         KeyType* key, ValType* val1, ValType* val2, void *ptr);
    int (*user_hash)(KeyType* key, ValType *val, int npartition);

    RepartitionCallback user_repartition;
    bool                do_shuffle;

    std::vector<std::string> input_dir;
    std::string              output_dir;

    BaseDatabase<KeyType,ValType>*         database;
    BaseDatabase<InKeyType,InValType>*     in_database;
    BaseDatabase<KeyType,ValType>*         user_database;

    OUTPUT_MODE   output_mode;

    uint64_t    input_records;
    uint64_t    kv_records;
    uint64_t    kmv_records;
    uint64_t    output_records;

    MPI_Comm    mimir_ctx_comm;
    int         mimir_ctx_rank;
    int         mimir_ctx_size;

    int         keycount, valcount;
    int         inkeycount, invalcount;
    int         outkeycount, outvalcount;

    // Configuration
    std::string outfile_format="binary";
};

}

#endif
