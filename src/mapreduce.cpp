/**
 * @file   mapreduce.cpp
 * @Author Tao Gao (taogao@udel.edu)
 * @date   September 1st, 2016
 * @brief  This file provides interfaces to application programs.
 *
 * This file includes two classes: MapReduce and MultiValueIter.
 *
 * \todo multithreading probelm: delete buffers of processed page before competing all pages's processing
 * \todo input buffer problem: if the string length is larger than the configured value, how to hanlde it?
 */
#include <stdio.h>
#include <stdlib.h>
#include <sched.h>
#include <unistd.h>
#include <sys/wait.h>
#include <math.h>
#include <dirent.h>
#include <sys/stat.h>
#include <mpi.h>
#include <iostream>
#include <sstream>
#include <string>
#include <list>
#include <vector>

#include "mapreduce.h"
#include "keyvalue.h"
#include "alltoall.h"
#include "log.h"
#include "config.h"
#include "const.h"
#include "hash.h"
#include "memory.h"
#include "stat.h"
#include "hashbucket.h"

#include "basefilereader.h"
#include "baserecordformat.h"

using namespace MIMIR_NS;

int MapReduce::ref = 0;

/**
  Create MapReduce Object

  @param[in]     _caller MPI Communicator
  @return return MapReduce Object.
  */
MapReduce::MapReduce(MPI_Comm _caller)
{
    // Get coommunicator information
    comm = _caller;
    MPI_Comm_rank(comm, &me);
    MPI_Comm_size(comm, &nprocs);

    // Initalize stat
    INIT_STAT(me, nprocs, comm);

    // Get default values
    _get_default_values();

    MapReduce::ref++;

    LOG_INIT(me, nprocs);
    LOG_PRINT(DBG_GEN, "%s", "MapReduce: create.\n");
}

/**
  Copy MapReduce Object

  @param[in]     _mr original MapReduce Object
  @return return new MapReduce Object.
  */
MapReduce::MapReduce(const MapReduce &_mr)
{
    comm = _mr.comm;
    me = _mr.me;
    nprocs = _mr.nprocs;

    kv = _mr.kv;
    DataObject::addRef(kv);

    myhash = _mr.myhash;
    mycombiner = _mr.mycombiner;

    phase = NonePhase;
    c = NULL;

    // Get default values
    //_get_default_values();

    INIT_STAT(me, nprocs, comm);

    MapReduce::ref++;

    LOG_INIT(me, nprocs);
    LOG_PRINT(DBG_GEN, "%s", "MapReduce: copy\n");
}

/**
  Destory MapReduce Object
  */
MapReduce::~MapReduce()
{
    DataObject::subRef(kv);
    if (c)
        delete c;

    UNINIT_STAT;

    MapReduce::ref--;

#if 1
    if (MapReduce::ref == 0) {
        //printf("DataObject=%ld, CombinerHashBucket=%ld, ReducerHashBucket=%ld\n", DataObject::mem_bytes, CombinerHashBucket::mem_bytes, ReducerHashBucket::mem_bytes);
        if (DataObject::mem_bytes != 0)
            LOG_ERROR("Error: page buffers memory leak!\n");
        if (CombinerHashBucket::mem_bytes != 0)
            LOG_ERROR("Error: hash bucket buffers memory leak!\n");
        if (ReducerHashBucket::mem_bytes != 0)
            LOG_ERROR("Error: hash bucket buffers memory leak!\n");
    }
#endif

    LOG_PRINT(DBG_GEN, "%s", "MapReduce: destroy.\n");
}

#if 0
uint64_t MapReduce::process_binary_file(const char *filepath, int shared,
                                        int recurse,
                                        ProcessBinaryFile myfunc,
                                        UserSplit mysplit,
                                        void* ptr, 
                                        int repartition)
{
    LOG_PRINT(DBG_GEN, "MapReduce: process_binary_file start. (filepath=%s, shared=%d, recursed=%d, comm=%d)\n", filepath, shared, recurse, repartition);

    TRACKER_RECORD_EVENT(EVENT_COMPUTE_OTHER);

    myptr = ptr;

    DataObject::subRef(kv);

    // Create KV Container
    kv = new KeyValue(me, nprocs, DATA_PAGE_SIZE, MAX_PAGE_COUNT);
    kv->set_kv_size(ksize, vsize);
    kv->set_combiner(this, mycombiner);

    // Create communicator
    if (repartition) {
        c = Communicator::Create(comm, KV_EXCH_COMM);
        c->setup(COMM_BUF_SIZE, kv, this, mycombiner, myhash);
        phase = MapPhase;
    }
    else {
        phase = LocalMapPhase;
    }

    FileReader *in = FileReader::createStream((IOMethod)DISK_IO_TYPE,
                                        FILE_SPLIT_UNIT, filepath, shared, 
                                        recurse, comm, mysplit, ptr);

    in->open_stream();
    // Process file
    myfunc(this, in, ptr);
    in->close_stream();

    FileReader::destroyStream(in);

    // Delete communicator
    if (repartition) {
        c->wait();
        delete c;
        c = NULL;
    }

    // Garbage collection
    kv->gc();

    DataObject::addRef(kv);
    phase = NonePhase;

    TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

    LOG_PRINT(DBG_GEN, "%s", "MapReduce: map_text_file end.\n");

    return _get_kv_count();
}
#endif

uint64_t MapReduce::map_files(BaseFileReader *reader, UserMapRecord mymap, 
                              void *ptr, int repartition)
{

    TRACKER_RECORD_EVENT(EVENT_COMPUTE_OTHER);

    myptr = ptr;

    DataObject::subRef(kv);

    // Create KV Container
    kv = new KeyValue(me, nprocs, DATA_PAGE_SIZE, MAX_PAGE_COUNT);
    kv->set_kv_size(ksize, vsize);
    kv->set_combiner(this, mycombiner);

    if (repartition) {
        c = Communicator::Create(comm, KV_EXCH_COMM);
        c->setup(COMM_BUF_SIZE, kv, this, mycombiner, myhash);
        phase = MapPhase;
        // local map
    }
    else {
        phase = LocalMapPhase;
    }

    BaseRecordFormat *record = NULL;
    reader->open();
    while((record = reader->next()) != NULL){
        mymap(this, record, ptr);
    }
    reader->close();

     // Delete communicator
    if (repartition) {
        c->wait();
        delete c;
        c = NULL;
    }


    // Garbage collection
    kv->gc();

    DataObject::addRef(kv);
    phase = NonePhase;

    TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

    LOG_PRINT(DBG_GEN, "%s", "MapReduce: map_text_file end.\n");

    return _get_kv_count();
}

uint64_t MapReduce::map_text_file(const char *filepath,
                                  int shared,
                                  int recurse,
                                  const char *seperator, 
                                  UserMapFile mymap,
                                  void *ptr, int repartition)
{
    //if (strlen(_seperator) == 0)
    //    LOG_ERROR("Error: the separator should not be empty!\n");

    LOG_PRINT(DBG_GEN, "MapReduce: map_text_file start. (filepath=%s, shared=%d, recursed=%d, comm=%d)\n", filepath, shared, recurse, repartition);

    TRACKER_RECORD_EVENT(EVENT_COMPUTE_OTHER);

    myptr = ptr;

    DataObject::subRef(kv);

    // Create KV Container
    kv = new KeyValue(me, nprocs, DATA_PAGE_SIZE, MAX_PAGE_COUNT);
    kv->set_kv_size(ksize, vsize);
    kv->set_combiner(this, mycombiner);

    if (repartition) {
        c = Communicator::Create(comm, KV_EXCH_COMM);
        c->setup(COMM_BUF_SIZE, kv, this, mycombiner, myhash);
        phase = MapPhase;
        // local map
    }
    else {
        phase = LocalMapPhase;
    }

#if 0
    FileReader *in = FileReader::createStream((IOMethod)DISK_IO_TYPE, 
                                              FILE_SPLIT_UNIT, filepath, shared, 
                                              recurse, comm, wordsplitcb, 
                                              (void*)seperator);

    StringIterator *iter = new StringIterator(in, seperator);
    char *string;
    while((string = iter->getNextRecord()) != NULL){
        mymap(this, string, ptr);
    }
    //std::string whitespaces=seperator;
    //std::string str;
    //char *ch_ptr;

    //in->open_stream();
    //while(in->is_empty() == false){
    //    ch_ptr = in->get_byte();
    //    if(in->is_eof()){
    //        if(str.size() != 0){
    //            mymap(this, str.c_str(), ptr);
    //            str.clear();
    //        }
    //    }else if(whitespaces.find(*ch_ptr) == std::string::npos){
    //        str += *ch_ptr;
    //    }else{
    //        if(str.size() != 0){
    //            mymap(this, str.c_str(), ptr);
    //            str.clear();
    //        }
    //    }
    //    in->next();
    //}
    //if(str.size() != 0){
    //    mymap(this, str.c_str(), ptr);
    //    str.clear();
    //}
    //in->close_stream();
    delete iter;
    FileReader::destroyStream(in);
#endif

    // Delete communicator
    if (repartition) {
        c->wait();
        delete c;
        c = NULL;
    }

    // Garbage collection
    kv->gc();

    DataObject::addRef(kv);
    phase = NonePhase;

    TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

    LOG_PRINT(DBG_GEN, "%s", "MapReduce: map_text_file end.\n");

    return _get_kv_count();
}

/**
  Map function KV input

*/
uint64_t MapReduce::map_key_value(MapReduce *mr,
                                  UserMapKV mymap,
                                  void *ptr,
                                  int repartition)
{
    LOG_PRINT(DBG_GEN, "MapReduce: map start. (KV as input)\n");

    TRACKER_RECORD_EVENT(EVENT_COMPUTE_OTHER);

    DataObject::addRef(mr->kv);
    DataObject::subRef(kv);

    KeyValue *inputkv = mr->kv;

    // create new data object
    //LOG_PRINT(DBG_GEN, "MapReduce: new data KV. (KV as input)\n");

    kv = new KeyValue(me, nprocs, DATA_PAGE_SIZE, MAX_PAGE_COUNT);

    kv->set_kv_size(ksize, vsize);
    kv->set_combiner(this, mycombiner);


    //LOG_PRINT(DBG_GEN, "MapReduce: alloc data KV. (KV as input)\n");

    if (repartition) {
        c = Communicator::Create(comm, KV_EXCH_COMM);
        c->setup(COMM_BUF_SIZE, kv, this, mycombiner, myhash);
        phase = MapPhase;
    }
    else {
        phase = LocalMapPhase;
    }

    char *key, *value;
    int keybytes, valuebytes;

    int i;
    for (i = 0; i < inputkv->get_npages(); i++) {

        //printf("i=%d\n", i);

        int64_t offset = 0;

        inputkv->acquire_page(i);

        offset = inputkv->getNextKV(&key, keybytes, &value, valuebytes);

        while (offset != -1) {

            //printf("map_key_value: v0=%ld\n", *(int64_t*)key);

            mymap(this, key, keybytes, value, valuebytes, ptr);

            offset = inputkv->getNextKV(&key, keybytes, &value, valuebytes);
        }

        inputkv->delete_page(i);
        inputkv->release_page(i);
    }

    DataObject::subRef(inputkv);

    if (repartition) {
        c->wait();
        delete c;
        c = NULL;
    }

    kv->gc();
    DataObject::addRef(kv);
    phase = NonePhase;

    TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

    LOG_PRINT(DBG_GEN, "MapReduce: map end. (KV as input)\n");

    return _get_kv_count();
}


/**
  Map function without input
  */
uint64_t MapReduce::init_key_value(UserInitKV myinit,
                                   void *ptr,
                                   int repartition)
{

    LOG_PRINT(DBG_GEN, "MapReduce: map start. (no input)\n");

    TRACKER_RECORD_EVENT(EVENT_COMPUTE_OTHER);

    DataObject::subRef(kv);
    kv = new KeyValue(me, nprocs, DATA_PAGE_SIZE, MAX_PAGE_COUNT);
    kv->set_kv_size(ksize, vsize);
    kv->set_combiner(this, mycombiner);

    if (repartition) {
        c = Communicator::Create(comm, KV_EXCH_COMM);
        c->setup(COMM_BUF_SIZE, kv, this, mycombiner, myhash);
        phase = MapPhase;
    }
    else {
        phase = LocalMapPhase;
    }

    myinit(this, ptr);

    // wait all processes done
    if (repartition) {
        c->wait();
        delete c;
        c = NULL;
    }

    DataObject::addRef(kv);
    phase = NonePhase;

    TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

    LOG_PRINT(DBG_GEN, "MapReduce: map end. (no input)\n");

    return _get_kv_count();
}

/**
  General internal reduce function.

  @param[in]     kv input KV object
  @param[in]     myreduce user-defined reduce function
  @param[in]     ptr user-defined pointer
  @return return number of output KVs
  */
uint64_t MapReduce::reduce(UserReduce myreduce, void *ptr)
{
    LOG_PRINT(DBG_GEN, "MapReduce: reduce start.\n");
    TRACKER_RECORD_EVENT(EVENT_COMPUTE_OTHER);

    phase = ReducePhase;

    // Create DataObject and HashBucket to hold KMVs
    DataObject *mv = new DataObject(me, nprocs, ByteType,
                                    DATA_PAGE_SIZE, MAX_PAGE_COUNT);
    ReducerHashBucket *u = new ReducerHashBucket(kv);

    // Convert input KVs to KMVs
    _convert(kv, mv, u);
    DataObject::subRef(kv);

    // Apply user-defined reduce
    kv = new KeyValue(me, nprocs, DATA_PAGE_SIZE, MAX_PAGE_COUNT);
    kv->set_kv_size(ksize, vsize);
    _reduce(u, myreduce, ptr);
    delete mv;
    delete u;

    DataObject::addRef(kv);
    phase = NonePhase;

    TRACKER_RECORD_EVENT(EVENT_COMPUTE_RDC);

    LOG_PRINT(DBG_GEN, "MapReduce: reduce end.\n");

    return _get_kv_count();
}


/**
  Add <eky,value>

  @param[in]  _key key pointer
  @param[in]  _keybytes key size
  @param[in]  _value value pointer
  @param[in] _valubytes value size
  @return nothing
  */
void MapReduce::add_key_value(const char *key, int keybytes, const char *value, int valuebytes)
{
    // Map Phase
    if (phase == MapPhase) {
        // Send KV
        c->sendKV(key, keybytes, value, valuebytes);
        return;
        // Local Mode
    }
    else if (phase == LocalMapPhase || phase == ReducePhase) {
        // Add KV
        kv->addKV(key, keybytes, value, valuebytes);
        return;
    }
    else {
        LOG_ERROR("Error: add_key_value function can be invoked in map and \
                  reduce callbacks\n");
    }

    return;
}

void MapReduce::update_key_value(const char *key, int keybytes, const char *value, int valuebytes)
{
    if (phase == MapPhase) {
        // Update the KV
        c->updateKV(key, keybytes, value, valuebytes);
    }
    else if (phase == CombinePhase || phase == LocalMapPhase) {
        // Update the KV
        kv->updateKV(key, keybytes, value, valuebytes);
    }
    else {
        LOG_ERROR("Error: update_key_value function can be invoked in \
                  combiner callbacks\n");
    }
}

/**
  Scan <key,value>

  @param[in]  _myscan user-defined scan function
  @param[in]  _ptr user-defined pointer
  @return nothing
  */
void MapReduce::scan(void (_myscan) (char*, int, char*, int, void*), void *_ptr)
{
    char *key, *value;
    int keybytes, valuebytes;

    LOG_PRINT(DBG_GEN, "MapReduce: scan begin\n");
    TRACKER_RECORD_EVENT(EVENT_COMPUTE_OTHER);
    phase = ScanPhase;
    for (int i = 0; i < kv->get_npages(); i++) {
        int64_t offset = 0;

        kv->acquire_page(i);

        offset = kv->getNextKV(&key, keybytes, &value, valuebytes);

        while (offset != -1) {

            _myscan(key, keybytes, value, valuebytes, _ptr);

            offset = kv->getNextKV(&key, keybytes, &value, valuebytes);
        }

        kv->release_page(i);
    }

    phase = NonePhase;
    TRACKER_RECORD_EVENT(EVENT_COMPUTE_SCAN);
    LOG_PRINT(DBG_GEN, "MapReduce: scan end.\n");
}

uint64_t MapReduce::bcast(int _rank)
{
    LOG_PRINT(DBG_GEN, "MapReduce: bcast begin\n");

    // Bcast meta-information
    int npages = 0;
    if (me == _rank)
        npages = kv->get_npages();
    else {
        DataObject::subRef(kv);
        kv = new KeyValue(me, nprocs, DATA_PAGE_SIZE, MAX_PAGE_COUNT);
    }
    MPI_Bcast(&npages, 1, MPI_INT, _rank, comm);
    MPI_Bcast(&(kv->ksize), 1, MPI_INT, _rank, comm);
    MPI_Bcast(&(kv->vsize), 1, MPI_INT, _rank, comm);

    // Bcast the KVs
    for (int i = 0; i < npages; i++) {
        int newpage = 0;
        int64_t count = 0;
        void *buffer = NULL;
        if (me == _rank) {
            buffer = kv->get_page_buffer(i);
            count = kv->get_page_size(i);
        }
        else {
            newpage = kv->add_page();
            buffer = kv->get_page_buffer(newpage);
        }
        MPI_Bcast(&count, 1, MPI_INT64_T, _rank, comm);
        MPI_Bcast(buffer, (int) count, MPI_BYTE, _rank, comm);
        if (me != _rank)
            kv->set_page_size(newpage, count);
    }

    MPI_Bcast(&(kv->local_kvs_count), 1, MPI_UINT64_T, _rank, comm);

    if (me != _rank) {
        DataObject::addRef(kv);
    }

    LOG_PRINT(DBG_GEN, "MapReduce: bcast end.\n");

    return _get_kv_count();
}

uint64_t MapReduce::collect(int _rank)
{
    LOG_PRINT(DBG_GEN, "MapReduce: collect begin.\n");

    int npages = kv->get_npages();
    int npages_sum = 0;
    MPI_Reduce(&npages, &npages_sum, 1, MPI_INT, MPI_SUM, _rank, comm);

    if (me == _rank) {
        npages = npages_sum;
        int recv_count = kv->get_npages();
        void *recv_buf = mem_aligned_malloc(MEMPAGE_SIZE, DATA_PAGE_SIZE);
        int count = 0;
        while (recv_count < npages) {
            MPI_Status st;
            MPI_Recv(recv_buf, (int) DATA_PAGE_SIZE, MPI_BYTE, MPI_ANY_SOURCE, 0x0, comm, &st);
            MPI_Get_count(&st, MPI_BYTE, &count);
            int src_off = 0;
            char *src_buf = (char*) recv_buf;
            while (src_off < count) {
                char *key = NULL, *value = NULL;
                int keybytes = 0, valuebytes = 0, kvsize = 0;
                GET_KV_VARS(kv->ksize, kv->vsize, src_buf, key, keybytes, value, valuebytes,
                            kvsize);
                src_buf += kvsize;
                kv->addKV(key, keybytes, value, valuebytes);
                src_off += kvsize;
            }
            recv_count++;
        }
        mem_aligned_free(recv_buf);
    }
    else {
        for (int i = 0; i < npages; i++) {
            char *buffer = kv->get_page_buffer(i);
            int count = (int) (kv->get_page_size(i));
            MPI_Send(buffer, count, MPI_BYTE, _rank, 0x0, comm);
        }
        DataObject::subRef(kv);
        kv = new KeyValue(me, nprocs, DATA_PAGE_SIZE, MAX_PAGE_COUNT);
        DataObject::addRef(kv);
    }

    LOG_PRINT(DBG_GEN, "MapReduce: collect end.\n");

    return 0;
}

const void *MapReduce::get_first_value()
{
    iter->Begin();
    if (iter->Done())
        return NULL;
    const void *val = iter->getValue();
    return val;
}

const void *MapReduce::get_next_value()
{
    iter->Next();
    if (iter->Done())
        return NULL;
    const void *val = iter->getValue();
    return val;
}

void MapReduce::_reduce(ReducerHashBucket * h, UserReduce _myreduce, void *ptr)
{
    LOG_PRINT(DBG_GEN, "MapReduce: _reduce start.\n");
    PROFILER_RECORD_COUNT(COUNTER_UNIQUE_KEY, (uint64_t) (h->get_nunique()), OPMAX);

#if 1
    // Apply user-defined reduce one-by-one
    ReducerUnique *u = h->BeginUnique();
    while (u != NULL) {
        //ReducerSet *set = u->firstset;
        iter = new MultiValueIterator(kv, u);
        _myreduce(this, u->key, u->keybytes, ptr);
        delete iter;
        iter = NULL;
        u = h->NextUnique();
    }
#endif

    LOG_PRINT(DBG_GEN, "MapReduce: _reduce end.\n");
}

void MapReduce::_convert(KeyValue *inputkv, DataObject *mv, ReducerHashBucket * h)
{
    char *key, *value;
    int keybytes, valuebytes;
    int i;

    LOG_PRINT(DBG_GEN, "MapReduce: _convert start.\n");
    fflush(stdout);

    // Construct Unique structure
    ReducerUnique u;
    for (i = 0; i < inputkv->get_npages(); i++) {
        int64_t offset = 0;

        inputkv->acquire_page(i);

        offset = inputkv->getNextKV(&key, keybytes, &value, valuebytes);

        while (offset != -1) {

            u.key = key;
            u.keybytes = keybytes;
            u.mvbytes = valuebytes;

            h->insertElem(&u);

            offset = inputkv->getNextKV(&key, keybytes, &value, valuebytes);
        }

        inputkv->release_page(i);
    }

    //printf("%d[%d] construct unique structure end\n", me, nprocs); fflush(stdout);

    // Set pointers to hold MVs
    char *page_buf = NULL;
    int64_t page_off = 0;
    int page_id = 0;
    ReducerSet *pset = h->BeginSet();
    while (pset != NULL) {
        if (page_buf == NULL || page_id != pset->pid) {
            page_id = mv->add_page();
            page_buf = mv->get_page_buffer(page_id);
            page_off = 0;

            //printf("page_buf=%p\n", page_buf);
        }

        if (inputkv->vsize == KVGeneral) {
            pset->soffset = (int*) (page_buf + page_off);
            page_off += sizeof(int) * (pset->nvalue);
        }
        else {
            pset->soffset = NULL;
        }

        pset->voffset = page_buf + page_off;
        pset->curoff = pset->voffset;
        page_off += pset->mvbytes;

        if (page_off > mv->pagesize)
            LOG_ERROR
                ("Error: the pointer of page %d exceeds the range (page_off=%ld, iset=%ld),pset=%p!\n",
                 page_id, page_off, h->iset, pset);

        pset = h->NextSet();
    }

    //printf("%d[%d] set pointer end\n", me, nprocs); fflush(stdout);

    // Modify the pointers
    ReducerUnique *uq = h->BeginUnique();
    while (uq != NULL) {

        uq->lastset = uq->firstset;

        uq = h->NextUnique();
    }

    //printf("%d[%d] modify pointer end\n", me, nprocs); fflush(stdout);

#if 1
    // Get the MVs
    for (i = 0; i < inputkv->get_npages(); i++) {
        int64_t offset = 0;

        inputkv->acquire_page(i);

        offset = inputkv->getNextKV(&key, keybytes, &value, valuebytes);

        while (offset != -1) {
            ReducerUnique *punique = h->findElem(key, keybytes);
            ReducerSet *pset = punique->lastset;

            if (inputkv->vsize == KVGeneral) {
                pset->soffset[pset->ivalue] = valuebytes;
            }

            memcpy(pset->curoff, value, valuebytes);
            pset->curoff += valuebytes;
            pset->ivalue += 1;

            if (pset->ivalue == pset->nvalue) {
                punique->lastset = punique->lastset->next;
            }

            //if (punique->lastset==NULL)
            //    printf("lastset=%p\n", punique->lastset);

            offset = inputkv->getNextKV(&key, keybytes, &value, valuebytes);
        }

        inputkv->delete_page(i);
        inputkv->release_page(i);
    }
#endif

    LOG_PRINT(DBG_GEN, "MapReduce: _convert end.\n");
}

void MapReduce::set_key_length(int _ksize)
{
    if (_ksize == 0 || _ksize < -1)
        LOG_ERROR("Error: key length (%d) should be larger than zero or -1(string)\n", _ksize);
    ksize = _ksize;
}

void MapReduce::set_value_length(int _vsize)
{
    if (_vsize < -1)
        LOG_ERROR("Error: value length (%d) should be equal or larger than zero or -1(string)\n",
                  _vsize);
    vsize = _vsize;
}

void MapReduce::output_stat(const char *filename)
{
    GET_CUR_TIME;
    PROFILER_PRINT(filename);
    //TRACKER_GATHER;
    TRACKER_PRINT(filename);
}


/*
 * Output data in this object
 *  type: 0 for string, 1 for int, 2 for int64_t
 *  fp:     file pointer
 *  format: hasn't been used
 */
void MapReduce::output(FILE *fp, ElemType ktype, ElemType vtype)
{
    if (kv) {
        kv->print(fp, ktype, vtype);
    }
}

// private function
/*****************************************************************************/

// process init
void MapReduce::_get_default_values()
{
    /// Initialize member of MapReduce
    phase = NonePhase;

    kv = NULL;
    c = NULL;

    myhash = NULL;
    mycombiner = NULL;

    //kvtype = GeneralKV;
    ksize = vsize = KVGeneral;

    /// Configure main parameters
    char *env = NULL;
    env = getenv("MIMIR_BUCKET_SIZE");
    if (env) {
        BUCKET_COUNT = atoi(env);
        if (BUCKET_COUNT <= 0)
            LOG_ERROR
                ("Error: set bucket size error, please set MIMIR_BUCKET_SIZE (%s) correctly!\n",
                 env);
    }
    env = getenv("MIMIR_COMM_SIZE");
    if (env) {
        COMM_BUF_SIZE = _convert_to_int64(env);
        if (COMM_BUF_SIZE <= 0)
            LOG_ERROR
                ("Error: set communication buffer size error, please set MIMIR_COMM_SIZE (%s) correctly!\n",
                 env);
    }
    env = getenv("MIMIR_PAGE_SIZE");
    if (env) {
        DATA_PAGE_SIZE = _convert_to_int64(env);
        if (DATA_PAGE_SIZE <= 0)
            LOG_ERROR("Error: set page size error, please set DATA_PAGE_SIZE (%s) correctly!\n",
                      env);
    }
    env = getenv("MIMIR_IBUF_SIZE");
    if (env) {
        INPUT_BUF_SIZE = _convert_to_int64(env);
        if (INPUT_BUF_SIZE <= 0)
            LOG_ERROR
                ("Error: set input buffer size error, please set INPUT_BUF_SIZE (%s) correctly!\n",
                 env);
    }

    /// Configure unit size for communication buffer
    env = NULL;
    env = getenv("MIMIR_COMM_UNIT_SIZE");
    if (env) {
        COMM_UNIT_SIZE = (int) _convert_to_int64(env);
        if (COMM_UNIT_SIZE <= 0 || COMM_UNIT_SIZE > 1024 * 1024 * 1024)
            LOG_ERROR("Error: COMM_UNIT_SIZE (%d) should be > 0 and <1G!\n", COMM_UNIT_SIZE);
    }

    // Configure debug level
    env = getenv("MIMIR_DBG_ALL");
    if (env) {
        int flag = atoi(env);
        if (flag != 0) {
            DBG_LEVEL |= (DBG_GEN | DBG_DATA | DBG_COMM | DBG_IO | DBG_MEM);
        }
    }
    env = getenv("MIMIR_DBG_GEN");
    if (env) {
        int flag = atoi(env);
        if (flag != 0) {
            DBG_LEVEL |= (DBG_GEN);
        }
    }
    env = getenv("MIMIR_DBG_DATA");
    if (env) {
        int flag = atoi(env);
        if (flag != 0) {
            DBG_LEVEL |= (DBG_DATA);
        }
    }
    env = getenv("MIMIR_DBG_COMM");
    if (env) {
        int flag = atoi(env);
        if (flag != 0) {
            DBG_LEVEL |= (DBG_COMM);
        }
    }
    env = getenv("MIMIR_DBG_IO");
    if (env) {
        int flag = atoi(env);
        if (flag != 0) {
            DBG_LEVEL |= (DBG_IO);
        }
    }
    env = getenv("MIMIR_DBG_MEM");
    if (env) {
        int flag = atoi(env);
        if (flag != 0) {
            DBG_LEVEL |= (DBG_MEM);
        }
    }
    env = getenv("MIMIR_DBG_VERBOSE");
    if (env) {
        int flag = atoi(env);
        if (flag != 0) {
            DBG_LEVEL |= (DBG_VERBOSE);
        }
    }

    env = getenv("MIMIR_RECORD_PEAKMEM");
    if (env) {
        int flag = atoi(env);
        if (flag == 0) {
            RECORD_PEAKMEM = 0;
        }
    }

    if (me == 0) {
        fprintf(stdout, "bucket size(2^x)=%d, comm_buf_size=%ld, \
                data_page_size=%ld, input_buf_size=%ld, DBG_LEVEL=%x\n", BUCKET_COUNT, COMM_BUF_SIZE, DATA_PAGE_SIZE, INPUT_BUF_SIZE, DBG_LEVEL);
    }
    fflush(stdout);

    PROFILER_RECORD_COUNT(COUNTER_BUCKET_SIZE, (uint64_t) BUCKET_COUNT, OPMAX);
    PROFILER_RECORD_COUNT(COUNTER_INBUF_SIZE, (uint64_t) INPUT_BUF_SIZE, OPMAX);
    PROFILER_RECORD_COUNT(COUNTER_COMM_SIZE, (uint64_t) COMM_BUF_SIZE, OPMAX);
    PROFILER_RECORD_COUNT(COUNTER_PAGE_SIZE, (uint64_t) DATA_PAGE_SIZE, OPMAX);
}

int64_t MapReduce::_convert_to_int64(const char *_str)
{
    std::string str = _str;
    int64_t num = 0;
    if (str[str.size() - 1] == 'b' || str[str.size() - 1] == 'B') {
        str = str.substr(0, str.size() - 1);
        num = atoi(str.c_str());
        num *= 1;
    }
    else if (str[str.size() - 1] == 'k' || str[str.size() - 1] == 'K' ||
             (str[str.size() - 1] == 'b' && str[str.size() - 2] == 'k') ||
             (str[str.size() - 1] == 'B' && str[str.size() - 2] == 'K')) {
        if (str[str.size() - 1] == 'b' || str[str.size() - 1] == 'B') {
            str = str.substr(0, str.size() - 2);
        }
        else {
            str = str.substr(0, str.size() - 1);
        }
        num = atoi(str.c_str());
        num *= 1024;
    }
    else if (str[str.size() - 1] == 'm' || str[str.size() - 1] == 'M' ||
             (str[str.size() - 1] == 'b' && str[str.size() - 2] == 'm') ||
             (str[str.size() - 1] == 'B' && str[str.size() - 2] == 'M')) {
        if (str[str.size() - 1] == 'b' || str[str.size() - 1] == 'B') {
            str = str.substr(0, str.size() - 2);
        }
        else {
            str = str.substr(0, str.size() - 1);
        }
        num = atoi(str.c_str());
        num *= 1024 * 1024;
    }
    else if (str[str.size() - 1] == 'g' || str[str.size() - 1] == 'G' ||
             (str[str.size() - 1] == 'b' && str[str.size() - 2] == 'g') ||
             (str[str.size() - 1] == 'B' && str[str.size() - 2] == 'G')) {
        if (str[str.size() - 1] == 'b' || str[str.size() - 1] == 'B') {
            str = str.substr(0, str.size() - 2);
        }
        else {
            str = str.substr(0, str.size() - 1);
        }
        num = atoi(str.c_str());
        num *= 1024 * 1024 * 1024;
    }
    else {
        LOG_ERROR("Error: set buffer size %s error! \
                  The buffer size should end with b,B,k,K,kb,KB,m,M,mb,MB,g,G,gb,GB", _str);
    }
    if (num == 0) {
        LOG_ERROR("Error: buffer size %s should not be zero!", _str);
    }

    return num;
}


// distribute input file list
void MapReduce::_dist_input_files(const char *filepath, int sharedflag, int recurse)
{

    _get_input_files(filepath, sharedflag, recurse);

    if (sharedflag) {
        int fcount = (int) ifiles.size();
        int div = fcount / nprocs;
        int rem = fcount % nprocs;
        int *send_count = new int[nprocs];
        int total_count = 0;

        if (me == 0) {
            int j = 0, end = 0;
            for (int i = 0; i < nprocs; i++) {
                send_count[i] = 0;
                end += div;
                if (i < rem)
                    end++;
                while (j < end) {
                    send_count[i] += (int) strlen(ifiles[j].first.c_str()) + 1;
                    j++;
                }
                total_count += send_count[i];
            }
        }

        int recv_count;
        MPI_Scatter(send_count, 1, MPI_INT, &recv_count, 1, MPI_INT, 0, comm);

        int *send_displs = new int[nprocs];
        if (me == 0) {
            send_displs[0] = 0;
            for (int i = 1; i < nprocs; i++) {
                send_displs[i] = send_displs[i - 1] + send_count[i - 1];
            }
        }

        char *send_buf = new char[total_count];
        char *recv_buf = new char[recv_count];

        if (me == 0) {
            int offset = 0;
            for (int i = 0; i < fcount; i++) {
                memcpy(send_buf + offset, ifiles[i].first.c_str(),
                       strlen(ifiles[i].first.c_str()) + 1);
                offset += (int) strlen(ifiles[i].first.c_str()) + 1;
            }
        }

        MPI_Scatterv(send_buf, send_count, send_displs, MPI_BYTE,
                     recv_buf, recv_count, MPI_BYTE, 0, comm);

        ifiles.clear();
        int off = 0;
        while (off < recv_count) {
            char *str = recv_buf + off;
            struct stat file_stat;
            int err = stat(str, &file_stat);
            if (err)
                LOG_ERROR("Error in get input files, err=%d\n", err);
            int64_t fsize = file_stat.st_size;
            ifiles.push_back(std::make_pair(std::string(str), fsize));
            off += (int) strlen(str) + 1;
        }

        delete[] send_count;
        delete[] send_displs;
        delete[] send_buf;
        delete[] recv_buf;
    }
}

// get input file list
void MapReduce::_get_input_files(const char *filepath, int sharedflag, int recurse)
{
    // if shared, only process 0 read file names
    if (!sharedflag || (sharedflag && me == 0)) {

        struct stat inpath_stat;
        int err = stat(filepath, &inpath_stat);
        if (err)
            LOG_ERROR("Error in get input files, err=%d\n", err);

        // regular file
        if (S_ISREG(inpath_stat.st_mode)) {
            int64_t fsize = inpath_stat.st_size;
            ifiles.push_back(std::make_pair(std::string(filepath), fsize));
            // dir
        }
        else if (S_ISDIR(inpath_stat.st_mode)) {

            struct dirent *ep;
            DIR *dp = opendir(filepath);
            if (!dp)
                LOG_ERROR("Error in get input files\n");

            while ((ep = readdir(dp)) != NULL) {

#ifdef BGQ
                if (ep->d_name[1] == '.')
                    continue;
#else
                if (ep->d_name[0] == '.')
                    continue;
#endif

                char newstr[MAXLINE];
#ifdef BGQ
                sprintf(newstr, "%s/%s", filepath, &(ep->d_name[1]));
#else
                sprintf(newstr, "%s/%s", filepath, ep->d_name);
#endif
                err = stat(newstr, &inpath_stat);
                if (err)
                    LOG_ERROR("Error in get input files, err=%d\n", err);

                // regular file
                if (S_ISREG(inpath_stat.st_mode)) {
                    int64_t fsize = inpath_stat.st_size;
                    ifiles.push_back(std::make_pair(std::string(newstr), fsize));
                    // dir
                }
                else if (S_ISDIR(inpath_stat.st_mode) && recurse) {
                    _get_input_files(newstr, sharedflag, recurse);
                }
            }
        }
    }
}

uint64_t MapReduce::_get_kv_count()
{

    uint64_t local_count = 0, global_count = 0;

    local_count = kv->get_local_count();

    PROFILER_RECORD_TIME_START;

    MPI_Allreduce(&local_count, &global_count, 1, MPI_UINT64_T, MPI_SUM, comm);

    //printf("local_count=%ld, global_count=%ld\n", local_count, global_count);

    PROFILER_RECORD_TIME_END(TIMER_COMM_RDC);

    kv->set_global_count(global_count);

    TRACKER_RECORD_EVENT(EVENT_COMM_ALLREDUCE);

    return global_count;
}


MultiValueIterator::MultiValueIterator(KeyValue *_kv, ReducerUnique *_ukey)
{

    kv = _kv;
    ukey = _ukey;

    nvalue = ukey->nvalue;
}

void MultiValueIterator::Begin()
{
    ivalue = 0;
    value_start = 0;
    isdone = 0;
    if (ivalue >= nvalue)
        isdone = 1;
    else {
        pset = ukey->firstset;
        valuebytes = pset->soffset;
        values = pset->voffset;
        value_end = pset->nvalue;
    }

    value = values;

    if (kv->vsize == KVGeneral)
        valuesize = valuebytes[ivalue - value_start];
    else if (kv->vsize == KVString)
        valuesize = (int) strlen(value) + 1;
    else
        valuesize = kv->vsize;
}

void MultiValueIterator::Next()
{
    ivalue++;
    if (ivalue >= nvalue) {
        isdone = 1;
    }
    else {
        if (ivalue >= value_end) {
            value_start += pset->nvalue;
            pset = pset->next;

            valuebytes = pset->soffset;
            values = pset->voffset;

            value = values;
            value_end += pset->nvalue;
        }
        else {
            value += valuesize;
        }
        if (kv->vsize == KVGeneral)
            valuesize = valuebytes[ivalue - value_start];
        else if (kv->vsize == KVString)
            valuesize = (int) strlen(value) + 1;
        else
            valuesize = kv->vsize;
    }
}
