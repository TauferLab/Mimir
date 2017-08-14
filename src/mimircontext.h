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
#include "bincontainer.h"
#include "combinebincontainer.h"
#include "kmvcontainer.h"
#include "uniteddataset.h"
#include "collectiveshuffler.h"
#include "nbcollectiveshuffler.h"
#include "combinecollectiveshuffler.h"
#include "nbcombinecollectiveshuffler.h"
#include "filereader.h"
#include "filewriter.h"

#include <vector>
#include <string>

namespace MIMIR_NS {

//enum OUTPUT_MODE {IMPLICIT_OUTPUT, EXPLICIT_OUTPUT};
//#define MIMIR_COPY (MapCallback)0x01

template <typename KeyType, typename ValType,
         typename InKeyType = KeyType, typename InValType = ValType,
         typename OutKeyType = KeyType, typename OutValType = ValType>
class MimirContext {
  public:

    // Initialize Mimir Context
    MimirContext(std::vector<std::string> input_files = std::vector<std::string>(),
                 std::string output_files = std::string(),
                 MPI_Comm mimir_comm = MPI_COMM_WORLD,
                 void (*combine_fn)(Combinable<KeyType,ValType> *output,
                                    KeyType* key,
                                    ValType* val1, ValType* val2, ValType* val3,
                                    void *ptr) = NULL,
                 int (*partition_fn)(KeyType* key, ValType *val,
                                     int npartition) = NULL,
                 int (*padding_fn)(const char* buf, int buflen,
                                   bool islast) = NULL,
                 int keycount = 1, int valcount = 1,
                 int inkeycount = 1, int invalcount = 1,
                 int outkeycount = 1, int outvalcount = 1) {
        _init(input_files, output_files,
              mimir_comm,
              combine_fn, partition_fn, padding_fn,
              keycount, valcount,
              inkeycount, invalcount,
              outkeycount, outvalcount);
    }

    // Destory Mimir Context
    ~MimirContext() {
        _uinit();
    }

    // Set customized database
    void set_user_database(void *database) {
        this->user_database = (BaseDatabase<KeyType,ValType>*)database;
    }

    // Get data handle
    void *get_data_handle() {
        return database;
    }

    // Insert data handle
    void insert_data_handle(void *handle) {
        if (handle == NULL) LOG_ERROR("The handle inserted is not valid!\n");
        BaseDatabase<InKeyType,InValType>::addRef((BaseDatabase<InKeyType,InValType>*)handle);
        in_databases.push_back((BaseDatabase<InKeyType,InValType>*)handle);
    }

    // Map
    uint64_t map(void (*user_map)(Readable<InKeyType,InValType> *input, 
                                  Writable<KeyType,ValType> *output, void *ptr),
                 void *ptr = NULL,
                 bool do_shuffle = true,
                 bool output_file = false,
                 std::string outfile_format = "binary")
    {
        BaseShuffler<KeyType,ValType> *c = NULL;
        BaseDatabase<KeyType,ValType> *kv = NULL;
        std::vector<Readable<InKeyType,InValType>*> inputs;
        UnitedDataset<InKeyType,InValType> *united_input = NULL;
        Writable<KeyType,ValType> *output = NULL;
        FileReader<TextFileFormat,KeyType,ValType,InKeyType,InValType> *reader = NULL;
        FileWriter<KeyType,ValType> *writer = NULL;
        ChunkManager<KeyType,ValType> *chunk_mgr = NULL;

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_APP);

        if (user_map == NULL)
            LOG_ERROR("Please set map callback\n");

        LOG_PRINT(DBG_GEN, "MapReduce: map start\n");

        /////////////// get input objects ////////////////////
        // Input from outside
        if (in_databases.size() != 0) {
            for (auto iter : in_databases) {
                inputs.push_back(iter);
            }
        }
        // Input from this context
        if (database != NULL) {
            Readable<InKeyType,InValType>* input = (Readable<InKeyType,InValType>*)(database);
            inputs.push_back(input);
        }
        // Input from files
        if (input_dir.size() > 0) {
            if (user_padding != NULL) {
                if (WORK_STEAL) {
                    chunk_mgr = new StealChunkManager<KeyType,ValType>(mimir_ctx_comm, input_dir, BYSIZE);
                } else {
                    chunk_mgr = new ChunkManager<KeyType,ValType>(mimir_ctx_comm, input_dir, BYSIZE);
                }
            }
            else
                chunk_mgr = new ChunkManager<KeyType,ValType>(mimir_ctx_comm, input_dir, BYNAME);
            reader = FileReader<TextFileFormat,KeyType,ValType,InKeyType,InValType>::getReader(mimir_ctx_comm,
                                                                                               chunk_mgr, user_padding);
            inputs.push_back(reader);
        }

        ///////////////////// get output objects ////////////////////////
        // Output to customized database
        if (user_database != NULL) {
            output = (Writable<KeyType,ValType>*)user_database;
        }
        // Output to this context
        else if (!output_file) {
            if (BALANCE_LOAD) {
                if (!user_combine) kv = new BinContainer<KeyType,ValType>(bincount, keycount, valcount);
                else kv = new CombineBinContainer<KeyType,ValType>(user_combine, ptr, bincount, keycount, valcount);
            } else {
                if (CONTAINER_TYPE == 0) {
                    if (!user_combine) kv = new KVContainer<KeyType,ValType>(bincount, keycount, valcount);
                    else kv = new CombineKVContainer<KeyType,ValType>(user_combine, ptr, bincount, keycount, valcount);
                } else if (CONTAINER_TYPE == 1) {
                    if (!user_combine) kv = new BinContainer<KeyType,ValType>(bincount, keycount, valcount);
                    else kv = new CombineBinContainer<KeyType,ValType>(user_combine, ptr, bincount, keycount, valcount);
                }
            }
            output = kv;
        }
        // Output to files
        else {
            writer = FileWriter<KeyType,ValType>::getWriter(mimir_ctx_comm, output_dir.c_str());
            writer->set_file_format(outfile_format.c_str());
            output = writer;
        }

        // Map with shuffle
        if (do_shuffle) {
            // Map with combiner
            if (!user_combine) {
                // MPI_Alltoallv shuffler
                if (SHUFFLE_TYPE == 0)
                    c = new CollectiveShuffler<KeyType,ValType>(mimir_ctx_comm,
                                                                output,
                                                                user_partition,
                                                                keycount,
                                                                valcount);
                // MPI_Ialltoallv shuffler
                else if (SHUFFLE_TYPE == 1)
                    c = new NBCollectiveShuffler<KeyType,ValType>(mimir_ctx_comm,
                                                                  output,
                                                                  user_partition,
                                                                  keycount,
                                                                  valcount);
                else LOG_ERROR("Shuffle type %d error!\n", SHUFFLE_TYPE);
            // Map without combiner
            } else {
                // MPI_Alltoallv shuffler
                if (SHUFFLE_TYPE == 0)
                    c = new CombineCollectiveShuffler<KeyType,ValType>(mimir_ctx_comm,
                                                                       user_combine,
                                                                       ptr,
                                                                       output,
                                                                       user_partition,
                                                                       keycount,
                                                                       valcount);
                // MPI_Ialltoallv shuffler
                else if (SHUFFLE_TYPE == 1)
                    c = new NBCombineCollectiveShuffler<KeyType,ValType>(mimir_ctx_comm,
                                                                         user_combine,
                                                                         ptr,
                                                                         output,
                                                                         user_partition,
                                                                         keycount,
                                                                         valcount);
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
            if (inputs.size() != 0) {
                united_input = new UnitedDataset<InKeyType,InValType>(inputs);
                united_input->open();
            }
            //if (input) input->open();
            //if (user_map == (MapCallback)MIMIR_COPY) {
            //  ::mimir_copy(input, c, NULL);
            //} else {
            user_map(united_input, c, ptr);
            //}
            c->close();
            //if (input) {
            //    input->close();
            //    input_records = input->get_record_count();
            //}
            if (inputs.size() != 0) {
                united_input->close();
                input_records = united_input->get_record_count();
                delete united_input;
            }
            if (output) {
                output->close();
                kv_records = output->get_record_count();
            }
            delete c;
        }
        // Map without shuffler
        else {
            if (output) output->open();
            //if (input) input->open();
            if (inputs.size() != 0) {
                united_input = new UnitedDataset<InKeyType,InValType>(inputs);
                united_input->open();
            }
            //if (user_map == (MapCallback)MIMIR_COPY) {
            //  ::mimir_copy(input, output, NULL);
            //} else {
                user_map(united_input, output, ptr);
            //}
            //if (input) {
            //    input->close();
            //    input_records = input->get_record_count();
            //}
            if (inputs.size() != 0) {
                united_input->close();
                input_records = united_input->get_record_count();
                delete united_input;
            }
            if (output) {
                output->close();
                kv_records = output->get_record_count();
            }
        }

        // Cleanup input objects
        if (in_databases.size() != 0) {
            for (auto iter : in_databases) {
                BaseDatabase<InKeyType,InValType>::subRef(iter);
            }
            in_databases.clear();
	}
        if (database != NULL) {
            BaseDatabase<InKeyType,InValType>::subRef((BaseDatabase<InKeyType,InValType>*)database);
            database = NULL;
        }
        if (reader != NULL) {
            delete reader;
        }

        // Cleanup output objects
        if (user_database != NULL) {
            database = user_database;
            BaseDatabase<KeyType,ValType>::addRef((BaseDatabase<KeyType,ValType>*)database);
            user_database = NULL;
        } else if (!output_file) {
            database = kv;
            isoutkv = false;
            BaseDatabase<KeyType,ValType>::addRef((BaseDatabase<KeyType,ValType>*)database);
        } else {
            database = NULL;
            delete writer;
        }

        if (chunk_mgr != NULL) delete chunk_mgr;

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

        uint64_t total_records = 0;
        PROFILER_RECORD_TIME_START;
        MPI_Allreduce(&kv_records, &total_records, 1,
                      MPI_INT64_T, MPI_SUM, mimir_ctx_comm);
        PROFILER_RECORD_TIME_END(TIMER_COMM_RDC);

        PROFILER_RECORD_COUNT(COUNTER_MAX_KVS, kv_records, OPMAX);

        LOG_PRINT(DBG_GEN, "MapReduce: map done (KVs=%ld, peakmem=%ld)\n", kv_records, peakmem);

        return total_records;
    }

    uint64_t reduce(void (*user_reduce)(Readable<KeyType,ValType> *input,
                                        Writable<OutKeyType,OutValType> *output, void *ptr) = NULL,
                    void *ptr = NULL,
                    bool output_file = true,
                    std::string outfile_format = "binary") {

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

        input = (Readable<KeyType,ValType>*)database;
        // output to user database
        if (user_database != NULL) {
            output = (Writable<OutKeyType,OutValType>*)(user_database);
        // output to stage area
        } else if (!output_file) {
            kv = new KVContainer<OutKeyType,OutValType>(bincount, keycount, valcount);
            output = kv;
        // output to disk files
        } else {
            writer = FileWriter<OutKeyType,OutValType>::getWriter(mimir_ctx_comm, output_dir.c_str());
            writer->set_file_format(outfile_format.c_str());
            output = writer;
        }

        kmv = new KMVContainer<KeyType,ValType>(keycount, valcount);
        kmv->convert(input);
        BaseDatabase<KeyType,ValType>::subRef((BaseDatabase<KeyType,ValType>*)database);
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
            BaseDatabase<OutKeyType,OutValType>::addRef((BaseDatabase<OutKeyType,OutValType>*)database);
            user_database = NULL;
        } else if (!output_file) {
            BaseDatabase<OutKeyType,OutValType>::addRef(kv);
            database = kv;
            isoutkv = true;
        // output to disk files
        } else {
            delete writer;
            database = NULL;
        }

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_RDC);
        LOG_PRINT(DBG_GEN, "MapReduce: done\n");

        uint64_t total_records = 0;
	PROFILER_RECORD_TIME_START;
        MPI_Allreduce(&output_records, &total_records, 1,
                      MPI_INT64_T, MPI_SUM, mimir_ctx_comm);
        PROFILER_RECORD_TIME_END(TIMER_COMM_RDC);

        LOG_PRINT(DBG_GEN, "MapReduce: reduce done\n");

        return total_records;
    }

    uint64_t output(std::string outfile_format = "binary") {

        typename SafeType<OutKeyType>::type key[keycount];
        typename SafeType<OutValType>::type val[valcount];
        Readable<OutKeyType,OutValType> *output = NULL;

        if (database == NULL)
            LOG_ERROR("No data to output!\n");

        LOG_PRINT(DBG_GEN, "MapReduce: output start\n");

        FileWriter<OutKeyType, OutValType> *writer 
            = FileWriter<OutKeyType, OutValType>::getWriter(mimir_ctx_comm, output_dir.c_str());
        writer->set_file_format(outfile_format.c_str());
        output = (Readable<OutKeyType,OutValType>*)(database);
        output->open();
        writer->open();
        while (output->read(key, val) == 0) {
            writer->write(key, val);
        }
        writer->close();
        output->close();
        uint64_t output_records = writer->get_record_count();
        delete writer;

        BaseDatabase<OutKeyType,OutValType>::subRef((BaseDatabase<OutKeyType,OutValType>*)database);
        database = NULL;

        uint64_t total_records = 0;
	PROFILER_RECORD_TIME_START;
        MPI_Allreduce(&output_records, &total_records, 1,
                      MPI_INT64_T, MPI_SUM, mimir_ctx_comm);
        PROFILER_RECORD_TIME_END(TIMER_COMM_RDC);

        LOG_PRINT(DBG_GEN, "MapReduce: output done\n");

        return total_records;
    }

    uint64_t scan(void (*scan_fn)(KeyType *key, ValType *val, void *ptr),
                  void *ptr = NULL) {

        typename SafeType<KeyType>::type key[keycount];
        typename SafeType<ValType>::type val[valcount];
        Readable<KeyType,ValType> *db = NULL;

        if (database == NULL)
            LOG_ERROR("No data to scan!\n");

        db = (BaseDatabase<KeyType,ValType>*)(database);

        LOG_PRINT(DBG_GEN, "MapReduce: scan start\n");

        db->open();
        while (db->read(key, val) == 0) {
            scan_fn(key, val, ptr);
        }
        db->close();

        LOG_PRINT(DBG_GEN, "MapReduce: scan done\n");

        return 0;
    }

    uint64_t scan_output(void (*scan_fn)(OutKeyType *key, OutValType *val, void *ptr),
                         void *ptr = NULL) {

        typename SafeType<OutKeyType>::type key[keycount];
        typename SafeType<OutValType>::type val[valcount];
        Readable<OutKeyType,OutValType> *db = NULL;

        if (database == NULL)
            LOG_ERROR("No data to scan!\n");

        db = (BaseDatabase<OutKeyType,OutValType>*)(database);

        LOG_PRINT(DBG_GEN, "MapReduce: scan start\n");

        db->open();
        while (db->read(key, val) == 0) {
            scan_fn(key, val, ptr);
        }
        db->close();

        return 0;
    }

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
    void _init(std::vector<std::string> &input_dir,
               std::string &output_dir,
               MPI_Comm ctx_comm,
               void (*combine_fn)(Combinable<KeyType,ValType> *output,
                                  KeyType* key,
                                  ValType* val1, ValType* val2, ValType* val3,
                                  void *ptr),
               int (*partition_fn)(KeyType* key, ValType* val, int npartition),
               int (*padding_fn)(const char* buf, int buflen, bool islast),
               int keycount, int valcount,
               int inkeycount, int invalcount,
               int outkeycount, int outvalcount) {

        this->keycount = keycount;
        this->valcount = valcount;
        this->inkeycount = inkeycount;
        this->invalcount = invalcount;
        this->outkeycount = outkeycount;
        this->outvalcount = outvalcount;

        MPI_Comm_dup(ctx_comm, &mimir_ctx_comm);
        MPI_Comm_rank(mimir_ctx_comm, &mimir_ctx_rank);
        MPI_Comm_size(mimir_ctx_comm, &mimir_ctx_size);

        this->user_combine = combine_fn;
        this->user_partition = partition_fn;
        if (padding_fn == NULL) padding_fn = text_file_repartition;
        this->user_padding = padding_fn;
        this->input_dir = input_dir;
        this->output_dir = output_dir;

        database = user_database = NULL;
        in_databases.clear();

        input_records = output_records = 0;
        kv_records = kmv_records = 0;

        if (mimir_ctx_count == 0) {
            ::mimir_init();
        }
        mimir_ctx_count +=1;

        // BIN_COUNT may be changed by mimir_init function
        this->bincount = mimir_ctx_size * BIN_COUNT;

        isoutkv = false;
    }

    void _uinit() {
        MPI_Comm_free(&mimir_ctx_comm);
        if (database != NULL) {
            if (!isoutkv) BaseDatabase<KeyType,ValType>::subRef((BaseDatabase<KeyType,ValType>*)database);
            else BaseDatabase<OutKeyType,OutValType>::subRef((BaseDatabase<OutKeyType,OutValType>*)database);
            database = NULL;
        }
	else if (in_databases.size() != 0) {
            for (auto iter : in_databases) {
                BaseDatabase<InKeyType,InValType>::subRef(iter);
            }
            in_databases.clear();
	}
	mimir_ctx_count -= 1;
        if (mimir_ctx_count == 0) {
            ::mimir_finalize();
        }
    }

    // Callbacks
    void (*user_combine)(Combinable<KeyType,ValType> *output,
                         KeyType* key, ValType* val1, ValType* val2, ValType *val3, void *ptr);
    int (*user_partition)(KeyType* key, ValType *val, int npartition);
    int (*user_padding)(const char* buf, int buflen, bool islast);

    // Configurations
    std::vector<std::string> input_dir;    // Input files
    std::string              output_dir;   // Output files

    // Count for <Key,Value>
    int         keycount, valcount;
    int         inkeycount, invalcount;
    int         outkeycount, outvalcount;

    ////////////////////////////////////////////////////////////////////////
    // Internal Structure
    void*         database;
    bool          isoutkv;
    void*         user_database;
    std::vector<BaseDatabase<InKeyType, InValType>*> in_databases;

    uint64_t    input_records;
    uint64_t    kv_records;
    uint64_t    kmv_records;
    uint64_t    output_records;

    MPI_Comm    mimir_ctx_comm;
    int         mimir_ctx_rank;
    int         mimir_ctx_size;

    uint32_t    bincount;
};

}

#endif
