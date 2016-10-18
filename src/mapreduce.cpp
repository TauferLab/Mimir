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
#include "dataobject.h"
#include "keyvalue.h"
#include "communicator.h"
#include "alltoall.h"
#include "spool.h"
#include "log.h"
#include "config.h"
#include "const.h"
#include "hash.h"
#include "memory.h"
#include "stat.h"

using namespace MAPREDUCE_NS;

int me,nprocs,tnum;

/**
   Create MapReduce Object

   @param[in]     _caller MPI Communicator
   @return return MapReduce Object.
*/
MapReduce::MapReduce(MPI_Comm _caller)
{
  mem_alloc_init();
  comm = _caller;
  MPI_Comm_rank(comm,&me);
  MPI_Comm_size(comm,&nprocs);

  TRACKER_START(1);
  PROFILER_START(1);
  TRACKER_TIMER_INIT(0);

  _get_default_values();
  _bind_threads();

  data = NULL;
  c = NULL;

  mode = NoneMode;

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: create.\n", me, nprocs);
}

/**
   Copy MapReduce Object

   @param[in]     _mr original MapReduce Object
   @return return new MapReduce Object.
*/
MapReduce::MapReduce(const MapReduce &_mr){
  // copy stats
  local_kvs_count=_mr.local_kvs_count;
  global_kvs_count=_mr.global_kvs_count;
  // copy configures
  kvtype=_mr.kvtype;
  ksize=_mr.ksize;
  vsize=_mr.vsize;
  nmaxblock=_mr.nmaxblock;
  maxmemsize=_mr.maxmemsize;
  outofcore=_mr.outofcore;
  commmode=_mr.commmode;
  lbufsize=_mr.lbufsize;
  gbufsize=_mr.gbufsize;
  blocksize=_mr.blocksize;
  tmpfpath=_mr.tmpfpath;
  myhash=_mr.myhash;
  nbucket=_mr.nbucket;
  nset=_mr.nset;
  // copy internal states
  comm=_mr.comm;
  //me=_mr.me;
  //nprocs=_mr.nprocs;
  //tnum=_mr.tnum;
  mode=_mr.mode;
  ukeyoffset=_mr.ukeyoffset;
  // copy data
  data=_mr.data;
  DataObject::addRef(data);

  c=NULL;
  ifiles.clear();

  TRACKER_START(1);
  PROFILER_START(1);
  TRACKER_TIMER_INIT(0);

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: copy\n", me, nprocs);
}

/**
   Destory MapReduce Object
*/
MapReduce::~MapReduce()
{
  DataObject::subRef(data);

  if(c) delete c;

  TRACKER_END;
  PROFILER_END;

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: destroy.\n", me, nprocs);
}

/**
   Map function without input
*/
uint64_t MapReduce::init_key_value(UserInitKV _myinit, \
  void *_ptr, int _comm){

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map start. (no input)\n", me, nprocs);

  TRACKER_RECORD_EVENT(0, EVENT_MR_GENERAL);

  DataObject::subRef(data);
  KeyValue *kv = new KeyValue(kvtype,
                              blocksize,
                              nmaxblock,
                              maxmemsize,
                              outofcore,
                              tmpfpath);
  data=kv;
  DataObject::addRef(data);
  kv->setKVsize(ksize,vsize);

  if(_comm){
    c=Communicator::Create(comm, commmode);
    c->setup(gbufsize, kv);
    mode = CommMode;
  }else{
    mode = LocalMode;
  }

  nitem=0;
  blockid=-1;
  _myinit(this, _ptr);
  // wait all processes done
  if(_comm){
    c->wait();
    delete c;
    c = NULL;
  }

  mode = NoneMode;

  TRACKER_RECORD_EVENT(0, EVENT_MAP_COMPUTING);

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map end. (no input)\n", \
    me, nprocs);

  return _get_kv_count();
}

/**
   Map function KV input

*/
uint64_t MapReduce::map_key_value(MapReduce *_mr,
    UserMapKV _mymap, UserBiReduce _mycompress,
    void *_ptr, int _comm){

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map start. (KV as input)\n", \
    me, nprocs);

  TRACKER_RECORD_EVENT(0, EVENT_MR_GENERAL);

  DataObject::addRef(_mr->data);
  DataObject::subRef(data);

  DataObject *data = _mr->data;

  // create new data object
  KeyValue *kv = new KeyValue(kvtype,
                              blocksize,
                              nmaxblock,
                              maxmemsize,
                              outofcore,
                              tmpfpath);
  kv->setKVsize(ksize, vsize);
  this->data = kv;
  DataObject::addRef(this->data);

  if(_mycompress!=NULL){
    u=new UniqueInfo();
    u->ubucket = new Unique*[nbucket];
    u->unique_pool=new Spool(nbucket*(int)sizeof(Unique));
    u->nunique=0;
    u->ubuf=u->unique_pool->add_block();
    u->ubuf_end=u->ubuf+u->unique_pool->blocksize;
    memset(u->ubucket, 0, nbucket*(int)sizeof(Unique*));
    mycompress=_mycompress;
    ptr=_ptr;
    mode=CompressMode;
  }else if(_comm){
    c=Communicator::Create(comm, commmode);
    c->setup(gbufsize, kv);
    //c->init(kv);
    mode = CommMode;
  }else{
    mode = LocalMode;
  }

  KeyValue *inputkv = (KeyValue*)data;

  nitem=0;
  blockid=-1;

  char *key, *value;
  int keybytes, valuebytes;

  int i;
  for(i = 0; i < inputkv->nblock; i++){
    int64_t offset = 0;

    inputkv->acquire_block(i);

    offset = inputkv->getNextKV(i, offset, &key, keybytes, &value, valuebytes);

    while(offset != -1){

      _mymap(this, key, keybytes, value, valuebytes, _ptr);

      offset = inputkv->getNextKV(i, offset, &key, keybytes, &value, valuebytes);
    }

    inputkv->delete_block(i);
    inputkv->release_block(i);
  }

  //DataObject::subRef(data);
  PROFILER_RECORD_COUNT(0, COUNTER_MAP_OUTPUT_KV, data->gettotalsize());
  PROFILER_RECORD_COUNT(0, COUNTER_MAP_OUTPUT_PAGE, data->nblock);
  DataObject::subRef(data);

  if(_mycompress != NULL){
    if(_comm){
      c=Communicator::Create(comm, commmode);
      c->setup(gbufsize, kv);
      //c->init(kv);
      mode = CommMode;
    }else{
      mode = LocalMode;
    }

    _cps_unique2kv(u);

    delete [] u->ubucket;
    delete u->unique_pool;
    delete u;
  }

  if(_comm){
    c->wait();
    delete c;
    c = NULL;
  }

  mode=NoneMode;

  TRACKER_RECORD_EVENT(0, EVENT_MAP_COMPUTING);

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map end. (KV as input)\n", \
    me, nprocs);

  return _get_kv_count();
}

uint64_t MapReduce::compress(UserCompress _mycompress, void *_ptr, int _comm){

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: compress start.\n", me, nprocs);

  TRACKER_RECORD_EVENT(0, EVENT_MR_GENERAL);
  PROFILER_RECORD_COUNT(0, COUNTER_CPS_INPUT_KV, data->gettotalsize());

  if(estimate){
    int64_t estimate_kv_count = global_kvs_count/nprocs;
    nbucket=1;
    while(nbucket<=pow(2,MAX_BUCKET_SIZE) && \
      factor*nbucket<estimate_kv_count)
      nbucket*=2;
    nset=nbucket;
  }

  KeyValue *kv = (KeyValue*)data;
  int kvtype=kv->getKVtype();
  KeyValue *outkv = new KeyValue(kvtype,
                      blocksize,
                      nmaxblock,
                      maxmemsize,
                      outofcore,
                      tmpfpath);
  outkv->setKVsize(ksize, vsize);
  data=outkv;

  if(_comm){
    c=Communicator::Create(comm, commmode);
    c->setup(gbufsize, kv);
    //c->init(kv);
    mode = LocalMode;
  }else{
    mode = LocalMode;
  }

  _reduce_compress(kv, _mycompress, _ptr);
  PROFILER_RECORD_COUNT(0, COUNTER_CPS_PR_OUTKV, outkv->gettotalsize());

  DataObject::subRef(kv);

  if(_comm){
    c->wait();
    delete c;
    c = NULL;
  }

  DataObject::addRef(data);
  mode = NoneMode;

  TRACKER_RECORD_EVENT(0, EVENT_CPS_COMPUTING);
  PROFILER_RECORD_COUNT(0, COUNTER_CPS_OUTPUT_KV, data->gettotalsize());

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: compress end.\n", me, nprocs);

  return _get_kv_count();
}

uint64_t MapReduce::reducebykey(UserBiReduce _myreduce, void* _ptr){
  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: reducebykey start.\n", me, nprocs);

  TRACKER_RECORD_EVENT(0, EVENT_MR_GENERAL);
  PROFILER_RECORD_COUNT(0, COUNTER_RDC_INPUT_KV, data->gettotalsize());

  KeyValue *kv = (KeyValue*)data;
  int kvtype=kv->getKVtype();

  // init counters
  //int tid=0;
  //_tinit(tid);
  nitem=0;
  blockid=-1;

  mode = MergeMode;

  // init data structure
  UniqueInfo *u=new UniqueInfo();
  u->ubucket = new Unique*[nbucket];
  u->unique_pool=new Spool(nbucket*(int)sizeof(Unique));
  u->nunique=0;
  memset(u->ubucket, 0, nbucket*sizeof(Unique*));

  u->ubuf=u->unique_pool->add_block();
  u->ubuf_end=u->ubuf+u->unique_pool->blocksize;

  char *key=NULL, *value=NULL;
  int keybytes=0, valuebytes=0, kvsize=0;
  char *kvbuf=NULL;
  for(int i=0; i<kv->nblock; i++){
    // build unique structure
    kv->acquire_block(i);

    kvbuf=kv->getblockbuffer(i);
    int64_t datasize=kv->getdatasize(i);
    char *kvbuf_end=kvbuf+datasize;
    while(kvbuf<kvbuf_end){
      GET_KV_VARS(kv->kvtype, kvbuf,key,keybytes,value,valuebytes,kvsize,kv);

      _cps_kv2unique(u, key, keybytes, value, valuebytes, _myreduce, _ptr);

      //kv_count++;

    }//END while
    kv->delete_block(i);
    kv->release_block(i);
  }//END for

  // create new data object
  DataObject::subRef(kv);
  KeyValue *outkv = new KeyValue(kvtype,
                      blocksize,
                      nmaxblock,
                      maxmemsize,
                      outofcore,
                      tmpfpath);
  outkv->setKVsize(ksize, vsize);
  data=outkv;

  mode = LocalMode;

  _cps_unique2kv(u);

  PROFILER_RECORD_COUNT(0, COUNTER_PR_UNIQUE_SIZE, \
    (u->unique_pool->nblock)*(u->unique_pool->blocksize));

  delete [] u->ubucket;
  delete u->unique_pool;
  delete u;

  DataObject::addRef(data);

  mode=NoneMode;

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: reducebykey end.\n", me, nprocs);

  TRACKER_RECORD_EVENT(0, EVENT_RDC_COMPUTING);
  PROFILER_RECORD_COUNT(0, COUNTER_RDC_OUTPUT_KV, data->gettotalsize());

  return _get_kv_count();
}

uint64_t MapReduce::_cps_kv2unique(UniqueInfo *u, char *key, int keybytes, char *value, int valuebytes, UserBiReduce _myreduce, void *_ptr){
  uint32_t hid = hashlittle(key, keybytes, 0);
  int ibucket = hid % nbucket;

  UniqueCPS *ukey;
  Unique *pre;
  ukey = (UniqueCPS*)_findukey(u->ubucket, ibucket, key, keybytes, pre, 1);
  if(ukey){
    cur_ukey=ukey;

    char *unique_key=NULL, *unique_value=NULL;
    int unique_keybytes=0, unique_valuebytes=0, kvsize=0;
    char *kvbuf=(char*)ukey+sizeof(UniqueCPS);
    GET_KV_VARS(kvtype,kvbuf,unique_key,unique_keybytes,\
      unique_value,unique_valuebytes,kvsize,this);

    _myreduce(this, unique_key, unique_keybytes, \
    unique_value, unique_valuebytes, value, valuebytes, _ptr);
  }else{
    // calculate space needed
    int space_bytes;
    if(kvtype==GeneralKV)
      space_bytes=(int)sizeof(UniqueCPS)+twointlen+keybytes+maxvaluebytes;
    else
      space_bytes=(int)sizeof(UniqueCPS)+keybytes+maxvaluebytes;

    // add another block
    if(u->ubuf_end-u->ubuf<space_bytes){
      memset(u->ubuf, 0, u->ubuf_end-u->ubuf);
      u->ubuf=u->unique_pool->add_block();
      u->ubuf_end=u->ubuf+u->unique_pool->blocksize;
    }

    ukey=(UniqueCPS*)(u->ubuf);
    u->ubuf += sizeof(UniqueCPS);

    // add to the list
    ukey->next = NULL;
    if(pre == NULL)
      u->ubucket[ibucket] = (Unique*)ukey;
    else
      ((UniqueCPS*)pre)->next = ukey;

    // copy key
    int kvbytes;
    PUT_KV_VARS(kvtype, u->ubuf, key, keybytes, value, valuebytes, kvbytes);
    u->ubuf += (maxvaluebytes-valuebytes);
    //u->ubuf += maxvaluebytes;

    u->nunique++;
  }//END if
  return 0;
}


uint64_t MapReduce::_cps_unique2kv(UniqueInfo *u){
  int nunique=0;
  Spool *unique_pool=u->unique_pool;
  for(int j=0; j<unique_pool->nblock; j++){
    char *ubuf=unique_pool->blocks[j];
    char *ubuf_end=ubuf+unique_pool->blocksize;

    while(ubuf < ubuf_end){
      //UniqueCPS *ukey = (UniqueCPS*)(ubuf);
      int left_bytes=(int)(ubuf_end-ubuf);
      if((left_bytes <= (int)sizeof(UniqueCPS))) break;
      else if(kvtype==GeneralKV){
        char *kvbuf=ubuf+(int)sizeof(UniqueCPS);
        if(left_bytes<twointlen || *(int*)kvbuf==0) break;
      }else if(kvtype==StringKV || kvtype==StringKFixedV){
        char *kvbuf=ubuf+(int)sizeof(UniqueCPS);
        if(strlen(kvbuf)==0) break;
      }else if(kvtype==FixedKV || kvtype==FixedKStringV){
        char *kvbuf=ubuf+(int)sizeof(UniqueCPS);
        char tmpbuf[ksize];
        memset(tmpbuf, 0, ksize);
        if(left_bytes<ksize || memcmp(kvbuf,tmpbuf,ksize)==0) break;
      }

      nunique++;
      if(nunique>u->nunique) goto out;

      char *unique_key=NULL, *unique_value=NULL;
      int unique_keybytes=0, unique_valuebytes=0, kvsize=0;

      ubuf+=(int)sizeof(UniqueCPS);
      GET_KV_VARS(kvtype,ubuf,unique_key,unique_keybytes,\
        unique_value,unique_valuebytes,kvsize,this);

      //printf("key=%s\n", ukey->key); fflush(stdout);
      add_key_value(unique_key, unique_keybytes, \
        unique_value, unique_valuebytes);

      //ubuf+=sizeof(UniqueCPS);
      //ubuf+=ukey->keybytes;
      ubuf+=(maxvaluebytes-unique_valuebytes);
      //ubuf+=maxvaluebytes;
    }
  }
out:
  return 0;
}

/**
   Map function without input
*/
uint64_t MapReduce::reduce(UserReduce _myreduce, int _compress, void* _ptr){

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: reduce start.\n", me, nprocs);

  TRACKER_RECORD_EVENT(0, EVENT_MR_GENERAL);
  PROFILER_RECORD_COUNT(0, COUNTER_RDC_INPUT_KV, data->gettotalsize());

  if(estimate){
    int64_t estimate_kv_count = global_kvs_count/nprocs;
    nbucket=1;
    while(nbucket<=pow(2,MAX_BUCKET_SIZE) && \
      factor*nbucket<estimate_kv_count)
      nbucket*=2;
    nset=nbucket;
  }

  KeyValue *kv = (KeyValue*)data;
  int kvtype=kv->getKVtype();

  // create new data object
  KeyValue *outkv = new KeyValue(kvtype,
                      blocksize,
                      nmaxblock,
                      maxmemsize,
                      outofcore,
                      tmpfpath);
  outkv->setKVsize(ksize, vsize);
  data=outkv;

  mode = LocalMode;

  // Reduce without compress
  if(!_compress){
    local_kvs_count = _reduce(kv, _myreduce, _ptr);
    DataObject::subRef(kv);
  // Reduce with compress
  }else{
    //record_memory_usage("before compress");

    _reduce_compress(kv, _myreduce, _ptr);
    DataObject::subRef(kv);

    //record_memory_usage("after compress");

    PROFILER_RECORD_COUNT(0, COUNTER_PR_OUTPUT_KV, outkv->gettotalsize());

    KeyValue *tmpkv=outkv;

    outkv = new KeyValue(kvtype,
                  blocksize,
                  nmaxblock,
                  maxmemsize,
                  outofcore,
                  tmpfpath);
    outkv->setKVsize(ksize, vsize);
    data=outkv;

    //printf("tmp block=%d ref=%d\n", tmpkv->nblock, tmpkv->ref);

    local_kvs_count = _reduce(tmpkv, _myreduce, _ptr);
    delete tmpkv;
    //DataObject::subRef(tmpkv);

    //record_memory_usage("after reduce");
  }

  DataObject::addRef(data);
  //printf("reduce result block=%d, ref=%d\n", data->nblock, data->ref);
  mode = NoneMode;

  TRACKER_RECORD_EVENT(0, EVENT_RDC_COMPUTING);
  PROFILER_RECORD_COUNT(0, COUNTER_RDC_OUTPUT_KV, data->gettotalsize());

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: reduce end.\n", me, nprocs);

  return _get_kv_count();
}

/**
   Scan <key,value>

   @param[in]  _myscan user-defined scan function
   @param[in]  _ptr user-defined pointer
   @return nothing
*/
void MapReduce::scan(
  void (_myscan)(char *, int, char *, int ,void *),
  void * _ptr){
  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: scan begin\n", me, nprocs);

  TRACKER_RECORD_EVENT(0, EVENT_MR_GENERAL);

  KeyValue *kv = (KeyValue*)data;

  for(int i = 0; i < kv->nblock; i++){

     char *key=NULL, *value=NULL;
     int keybytes=0, valuebytes=0, kvsize=0;

     kv->acquire_block(i);
     char *kvbuf=kv->getblockbuffer(i);
     int64_t datasize=kv->getdatasize(i);

     int offset=0;
     while(offset < datasize){
       GET_KV_VARS(kv->kvtype,kvbuf,key,keybytes,value,valuebytes,kvsize,kv);

       _myscan(key, keybytes, value, valuebytes, _ptr);

       offset += kvsize;
     }
     kv->release_block(i);
  }

  TRACKER_RECORD_EVENT(0, EVENT_SCAN_COMPUTING);

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: scan end.\n", me, nprocs);
}

/**
   Add <eky,value>

   @param[in]  _key key pointer
   @param[in]  _keybytes key size
   @param[in]  _value value pointer
   @param[in] _valubytes value size
   @return nothing
*/
void MapReduce::add_key_value(char *key, int keybytes, char *value, int valuebytes){
  // Communication Mode
  if(mode == CommMode){
    int target = 0;
    if(myhash != NULL){
      target=myhash(key, keybytes);
    }else{
      uint32_t hid = 0;
      hid = hashlittle(key, keybytes, nprocs);
      target = hid % (uint32_t)nprocs;
    }

    // send KV
    c->sendKV(target, key, keybytes, value, valuebytes);

    nitem++;

    return;
   // Local Mode
   }else if(mode == LocalMode){

    // add KV into data object
    KeyValue *kv = (KeyValue*)data;

    if(blockid == -1){
      blockid = kv->add_block();
    }

    kv->acquire_block(blockid);

    while(kv->addKV(blockid, key, keybytes, value, valuebytes) == -1){
      kv->release_block(blockid);
      blockid = kv->add_block();
      kv->acquire_block(blockid);
    }

    kv->release_block(blockid);
    nitem++;
  // MergeMode
  }else if(mode==MergeMode){
    if(valuebytes > maxvaluebytes){
      LOG_ERROR("The value bytes %d is larger than the max %d\n", \
        valuebytes, maxvaluebytes);
    }
    char *unique_key=NULL, *unique_value=NULL;
    int unique_keybytes=0, unique_valuebytes=0, kvsize=0;
    char *kvbuf=(char*)cur_ukey+sizeof(UniqueCPS);
    GET_KV_VARS(kvtype,kvbuf,unique_key,unique_keybytes,\
      unique_value,unique_valuebytes,kvsize,this);
    memcpy(unique_value, value, valuebytes);
    if(kvtype==GeneralKV){
      char *kvbuf=(char*)cur_ukey+sizeof(UniqueCPS)+oneintlen;
      *(int*)(kvbuf)=valuebytes;
    }
    //cur_ukey->valuebytes=valuebytes;
    //cur_ukey->nvalue++;
  }else if(mode==CompressMode){
    mode=MergeMode;
    _cps_kv2unique(u, key, keybytes, value, valuebytes, mycompress, ptr);
    mode=CompressMode;
  }

  return;
}

uint64_t MapReduce::map_text_file( \
    char *filepath, int sharedflag, int recurse, char *whitespace, \
    UserMapFile mymap, UserBiReduce _mycompress, void *_ptr, int _comm){

    if(strlen(whitespace) == 0)
        LOG_ERROR("%s", "Error: the white space should not be empty string!\n");

    // log record
    LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map text \
      file start\nfilepath=%s,_mycompress=%p\n", \
      me, nprocs, filepath, _mycompress);

    TRACKER_RECORD_EVENT(0, EVENT_MR_GENERAL);

    // delete previous data
    DataObject::subRef(data);

    // distribute files
    ifiles.clear();
    _disinputfiles(filepath, sharedflag, recurse);

    TRACKER_RECORD_EVENT(0, EVENT_MAP_DIS_FILES);

    // create dataobject
    KeyValue *kv = new KeyValue(kvtype,
                              blocksize,
                              nmaxblock,
                              maxmemsize,
                              outofcore,
                              tmpfpath);
    kv->setKVsize(ksize,vsize);
    data=kv;

    // compress mode
    if(_mycompress!=NULL){
        u=new UniqueInfo();
        u->ubucket = new Unique*[nbucket];
        u->unique_pool=new Spool(nbucket*(int)sizeof(Unique));
        u->nunique=0;
        u->ubuf=u->unique_pool->add_block();
        u->ubuf_end=u->ubuf+u->unique_pool->blocksize;
        memset(u->ubucket, 0, nbucket*sizeof(Unique*));
        mycompress=_mycompress;
        ptr=_ptr;
        mode=CompressMode;
    // normal map
    }else if(_comm){
        c=Communicator::Create(comm, commmode);
        c->setup(gbufsize, kv);
        mode = CommMode;
    // local map
    }else{
        mode = LocalMode;
    }

    blockid=-1;
    nitem=0; 

    int fcount = (int)ifiles.size();
    for(int i = 0; i < fcount; i++){
        int64_t input_char_size=0;

        TRACKER_RECORD_EVENT(0, EVENT_MAP_COMPUTING);
        PROFILER_RECORD_COUNT(0, COUNTER_MAP_FILE_COUNT, 1);

        LOG_PRINT(DBG_IO, "%d[%d] open file %s\n", me, nprocs, ifiles[i].c_str());

        FILE *fp = fopen(ifiles[i].c_str(), "r");

        TRACKER_RECORD_EVENT(0, EVENT_PFS_OPEN);

        // Get file size
        struct stat stbuf;
        stat(ifiles[i].c_str(), &stbuf);
        int64_t fsize = stbuf.st_size;

        // Compute input buffer size
        int64_t input_buffer_size=0;
        if(fsize<=inputsize) input_buffer_size=fsize;
        else input_buffer_size=inputsize;

        // Allocate input buffer
        char *text = (char*)mem_aligned_malloc(MEMPAGE_SIZE, input_buffer_size+MAX_STR_SIZE+1);

        PROFILER_RECORD_COUNT(0, COUNTER_MAP_INPUT_BUF, input_buffer_size);

        // Process the file
        int64_t foff = 0, boff = 0;
        int64_t readsize=0;

        do{
            TRACKER_RECORD_EVENT(0, EVENT_MAP_COMPUTING);
            // Read file
            fseek(fp, foff, SEEK_SET);
            readsize = fread(text+boff, 1, input_buffer_size, fp);

            TRACKER_RECORD_EVENT(0, EVENT_PFS_READ);
            PROFILER_RECORD_COUNT(0, COUNTER_MAP_FILE_SIZE, readsize);

            // read a block
            text[boff+readsize] = '\0';
            input_char_size = boff+readsize;

            LOG_PRINT(DBG_IO, "%d[%d] read file %s, %ld->%ld\n", me, nprocs, ifiles[i].c_str(), foff, foff+readsize);

            int64_t tend=input_char_size;
            boff=0;
            if(readsize >= input_buffer_size && foff+readsize<fsize){
              while(strchr(whitespace, text[input_char_size-boff-1])==NULL) boff++;
              tend-=(boff+1);
              text[tend]='\0';
            }

            if(boff > MAX_STR_SIZE) LOG_ERROR("%s", "Error: string length is large than max size!\n");

            LOG_PRINT(DBG_IO, "%d[%d] read file %s, %ld->%ld, suffix=%d\n", me, nprocs, ifiles[i].c_str(), \
              foff, foff+readsize, boff);

            // Pass words one by one to user-defined map function
            char *saveptr = NULL;
            char *word = strtok_r(text, whitespace, &saveptr);
            while(word){
                mymap(this, word, ptr);
                word = strtok_r(NULL,whitespace, &saveptr);
            }
             
            // Prepare for next buffer
            foff += readsize;

            for(int j =0; j < boff; j++) text[j] = text[input_char_size-boff+j];

        }while(foff<fsize);

        TRACKER_RECORD_EVENT(0, EVENT_MAP_COMPUTING);

        fclose(fp);
        mem_aligned_free(text);

        TRACKER_RECORD_EVENT(0, EVENT_PFS_CLOSE);

        LOG_PRINT(DBG_IO, "%d[%d] close file %s\n", me, nprocs, ifiles[i].c_str());
    }

    // If in compress mode
    if(_mycompress != NULL){
        // Create comunicator
        if(_comm){
            c=Communicator::Create(comm, commmode);
            c->setup(gbufsize, kv);
            mode = CommMode;
        }else{
            mode = LocalMode;
        }

        _cps_unique2kv(u);

        delete [] u->ubucket;
        delete u->unique_pool;
        delete u;
    }

    // delete communicator
    if(_comm){
        c->wait();
        delete c;
        c = NULL;
    }

    DataObject::addRef(data);
    mode = NoneMode;

    TRACKER_RECORD_EVENT(0, EVENT_MAP_COMPUTING);
    PROFILER_RECORD_COUNT(0, COUNTER_MAP_FILE_KV, data->gettotalsize());
    PROFILER_RECORD_COUNT(0, COUNTER_MAP_FILE_PAGE, data->nblock);

    LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map text file end.\n", me, nprocs);

    return _get_kv_count();
}

/***** internal functions ******/

// find the key in the unique list
MapReduce::Unique* MapReduce::_findukey(Unique **unique_list, int ibucket, char *key, int keybytes, Unique *&uprev, int cps){
  if(!cps){
  Unique *uptr = unique_list[ibucket];

  if(!uptr){
    uprev = NULL;
    return NULL;
  }

  char *keyunique;
  while(uptr){
    keyunique = ((char*)uptr)+ukeyoffset;
    if(keybytes==uptr->keybytes && memcmp(key,keyunique,keybytes)==0)
      return uptr;
    uprev = uptr;
    uptr = uptr->next;
  }

  }else{
  UniqueCPS *uptr = (UniqueCPS*)unique_list[ibucket];

  if(!uptr){
    uprev = NULL;
    return NULL;
  }

  char *unique_key=NULL, *unique_value=NULL;
  int unique_keybytes=0, unique_valuebytes=0, kvsize=0;
  while(uptr){
    char *kvbuf=(char*)uptr+sizeof(UniqueCPS);
    GET_KV_VARS(kvtype,kvbuf,unique_key,unique_keybytes,\
      unique_value,unique_valuebytes,kvsize,this);

    if(keybytes==unique_keybytes && memcmp(key,unique_key,keybytes)==0)
      return (Unique*)uptr;
    uprev = (Unique*)uptr;
    uptr = uptr->next;
  }

  }

  return NULL;
}


void MapReduce::_unique2set(UniqueInfo *u){

  Set *set=(Set*)u->set_pool->add_block();

  int nunique=0;

  Spool *unique_pool=u->unique_pool;
  for(int i=0; i<unique_pool->nblock; i++){
    char *ubuf=unique_pool->blocks[i];
    char *ubuf_end=ubuf+unique_pool->blocksize;
    while(ubuf < ubuf_end){
      Unique *ukey = (Unique*)(ubuf);

      if(((int)(ubuf_end-ubuf) < (int)sizeof(Unique)) ||
        (ukey->key==NULL))
        break;

      nunique++;
      if(nunique>u->nunique) goto end;

      Set *pset=&set[u->nset%nset];
      pset->myid=u->nset++;
      pset->nvalue=ukey->nvalue;
      pset->mvbytes=ukey->mvbytes;
      pset->pid=0;
      pset->next=NULL;

      ukey->firstset=pset;
      ukey->lastset=pset;

      if(u->nset%nset==0)
        set = (Set*)u->set_pool->add_block();

      ubuf += ukeyoffset;
      ubuf += ukey->keybytes;
      //ubuf = ROUNDUP(ubuf, ualignm);

    }// end while
  }

end:
  return;
}

/**
   Convert KVs to unique struct

   @param[in]     tid thread id
   @param[in]     kv input KV object
   @param[in]     u unqiue information
   @param[in]     mv multiple values
   @param[in]     myreduce user-defined reduce function
   @param[in]     ptr user-defined pointer
   @param[in]     shared if shared by others
   @return return number of output KVs
*/
int  MapReduce::_kv2unique(int tid, KeyValue *kv, UniqueInfo *u, DataObject *mv, UserReduce myreduce, void *ptr, int shared){

  //DEFINE_KV_VARS;
  char *key=NULL, *value=NULL;
  int keybytes=0, valuebytes=0, kvsize=0;
  char *kvbuf;

  int isfirst=1, pid=0;
  int last_blockid=0, last_offset=0, last_set=0;

  int kmvbytes=0, mvbytes=0;

  char *ubuf=u->unique_pool->add_block();
  char *ubuf_end=ubuf+u->unique_pool->blocksize;

  Set *sets=NULL, *pset = NULL;

  /**
    Scan <key,value> one by one.
   */
  for(int i=0; i<kv->nblock; i++){

    kv->acquire_block(i);

    kvbuf=kv->getblockbuffer(i);
    char *kvbuf_end=kvbuf+kv->getdatasize(i);
    int kvbuf_off=0;

    while(kvbuf<kvbuf_end){
      GET_KV_VARS(kv->kvtype, kvbuf,key,keybytes,value,valuebytes,kvsize,kv);

      uint32_t hid = hashlittle(key, keybytes, 0);
      if(shared && (uint32_t)hid%tnum != (uint32_t)tid) {
        kvbuf_off += kvsize;
        continue;
      }

      // Find the key
      int ibucket = hid % nbucket;
      Unique *ukey=NULL, *pre=NULL;
      ukey = _findukey(u->ubucket, ibucket, key, keybytes, pre);

      int key_hit=1;
      if(!ukey) key_hit=0;

      int mv_inc=valuebytes;
      if(kv->kvtype==GeneralKV)  mv_inc+=oneintlen;

      // The First Block
      if(isfirst){
        //printf("kmvbytes=%d, mv_inc=%d\n", kmvbytes, mv_inc);
        kmvbytes+=mv_inc;
        if(!key_hit) kmvbytes+=(keybytes+3*(int)sizeof(int));

        // We need the intermediate convert
        if(kmvbytes>mv->blocksize){
          //printf("nunique=%d\n", u->nunique); fflush(stdout);
          _unique2set(u);
          //printf("nset=%d\n", u->nset); fflush(stdout);
          sets=(Set*)u->set_pool->blocks[u->nset/nset];
          isfirst=0;
        }
      }

      // Add a new partition
      if(mvbytes+mv_inc>mv->blocksize){
        Partition p;
        p.start_blockid=last_blockid;
        p.start_offset=last_offset;
        p.end_blockid=i;
        p.end_offset=kvbuf_off;
        p.start_set=last_set;
        p.end_set=u->nset;

        LOG_PRINT(DBG_CVT, "%d[%d] T%d Partition %d\n", me, nprocs, tid, pid);

        _unique2mv(tid, kv, &p, u, mv);

        last_blockid=p.end_blockid;
        last_offset=p.end_offset;
        last_set=p.end_set;
        pid++;
        mvbytes=0;
      }
      mvbytes += mv_inc;

      if(ukey){
        ukey->nvalue++;
        ukey->mvbytes += valuebytes;
      // add unique key
      }else{

        if(ubuf_end-ubuf<ukeyoffset+keybytes){
          //printf("add a new unique buffer! ubuf_end-ubuf=%ld\n", ubuf_end-ubuf); fflush(stdout);
          memset(ubuf, 0, ubuf_end-ubuf);
          ubuf=u->unique_pool->add_block();
          ubuf_end=ubuf+u->unique_pool->blocksize;
        }


        ukey=(Unique*)(ubuf);
        ubuf += ukeyoffset;

        // add to the list
        ukey->next = NULL;
        if(pre == NULL)
          u->ubucket[ibucket] = ukey;
        else
          pre->next = ukey;

        // copy key
        ukey->key = ubuf;
        memcpy(ubuf, key, keybytes);
        ubuf += keybytes;
        //ubuf = ROUNDUP(ubuf, ualignm);

        ukey->keybytes=keybytes;
        ukey->nvalue=1;
        ukey->mvbytes=valuebytes;
        ukey->flag=0;
        ukey->firstset=ukey->lastset=NULL;

        //printf("add key=%s\n", ukey->key); fflush(stdout);

        u->nunique++;
      }// end else if

      if(!isfirst) {
        // add one new set
        if((key_hit && ukey->lastset->pid != pid) || (!key_hit)){
          // add a block
          pset=&sets[u->nset%nset];

          pset->myid=u->nset;
          pset->nvalue=0;
          pset->mvbytes=0;
          pset->pid=pid;

          pset->next=NULL;
          if(ukey->lastset != NULL)
            ukey->lastset->next=pset;
          ukey->lastset=pset;
          if(ukey->firstset==NULL)
            ukey->firstset=pset;

          u->nset++;
          if(u->nset%nset==0){
            sets=(Set*)u->set_pool->add_block();
          }
        }else{
          pset=ukey->lastset;
        }

        // add key information into block
        pset->nvalue++;
        pset->mvbytes+=valuebytes;
      }

      kvbuf_off += kvsize;

    }// end while

    kv->release_block(i);
  }// end For

  if(!isfirst && kv->nblock>0){
    Partition p;
    p.start_blockid=last_blockid;
    p.start_offset=last_offset;
    p.end_blockid=kv->nblock-1;
    p.end_offset=(int)kv->getdatasize(kv->nblock-1);
    p.start_set=last_set;
    p.end_set=u->nset;

    _unique2mv(tid, kv, &p, u, mv);
  }

  return isfirst;
}

/**
   General internal reduce function.

   @param[in]     kv input KV object
   @param[in]     myreduce user-defined reduce function
   @param[in]     ptr user-defined pointer
   @return return number of output KVs
*/
void MapReduce::_unique2kmv(int tid, KeyValue *kv, UniqueInfo *u,DataObject *mv,UserReduce myreduce, void *ptr, int shared){

  //DEFINE_KV_VARS;
  char *key=NULL, *value=NULL;
  int keybytes=0, valuebytes=0, kvsize=0;

  //char *kvbuf;

  int mv_block_id=mv->add_block();
  mv->acquire_block(mv_block_id);
  char *mv_buf=mv->getblockbuffer(mv_block_id);
  int mv_off=0;

  int nunique=0;

  // Set the offset
  Spool *unique_pool=u->unique_pool;
  for(int i=0; i<unique_pool->nblock; i++){
    char *ubuf=unique_pool->blocks[i];
    char *ubuf_end=ubuf+unique_pool->blocksize;
    while(ubuf < ubuf_end){

      Unique *ukey = (Unique*)(ubuf);
      if((ubuf_end-ubuf < (int)sizeof(Unique)) || (ukey->key==NULL))
        break;

      nunique++;
      if(nunique>u->nunique) goto end;

      *(int*)(mv_buf+mv_off)=ukey->keybytes;
      mv_off+=(int)sizeof(int);
      *(int*)(mv_buf+mv_off)=ukey->mvbytes;
      mv_off+=(int)sizeof(int);
      *(int*)(mv_buf+mv_off)=ukey->nvalue;
      mv_off+=(int)sizeof(int);

      if(kv->kvtype==GeneralKV){
        ukey->soffset=(int*)(mv_buf+mv_off);
        mv_off+=ukey->nvalue*(int)sizeof(int);
      }

      ubuf+=ukeyoffset;
      memcpy(mv_buf+mv_off, ubuf, ukey->keybytes);
      mv_off+=ukey->keybytes;

      ukey->voffset=mv_buf+mv_off;
      mv_off+=ukey->mvbytes;

      ubuf+=ukey->keybytes;
      //ubuf=ROUNDUP(ubuf, ualignm);

      ukey->nvalue=0;
      ukey->mvbytes=0;
    }
  }

end:

#if SAFE_CHECK
  if(mv_off > (blocksize)){
    LOG_ERROR("KMV size %d is larger than a single block size %ld!\n", mv_off, blocksize);
  }
#endif

  mv->setblockdatasize(mv_block_id, mv_off);

  // gain KVS
  for(int i=0; i<kv->nblock; i++){
    kv->acquire_block(i);
    char *kvbuf=kv->getblockbuffer(i);
    int64_t datasize=kv->getdatasize(i);
    char *kvbuf_end=kvbuf+datasize;
    while(kvbuf<kvbuf_end){
      GET_KV_VARS(kv->kvtype,kvbuf,key,keybytes,value,valuebytes,kvsize,kv);

      //printf("key=%s, value=%s\n", key, value); fflush(stdout);

      uint32_t hid = hashlittle(key, keybytes, 0);
      if(shared && (uint32_t)hid % tnum != (uint32_t)tid) continue;

      //printf("tid=%d, key=%s, value=%s\n", tid, key, value);

      // Find the key
      int ibucket = hid % nbucket;
      Unique *ukey, *pre;
      ukey = _findukey(u->ubucket, ibucket, key, keybytes, pre);

      if(kv->kvtype==GeneralKV){
        ukey->soffset[ukey->nvalue]=valuebytes;
      }

      memcpy(ukey->voffset+ukey->mvbytes, value, valuebytes);

      ukey->mvbytes+=valuebytes;
      ukey->nvalue++;
    }
    kv->release_block(i);
  }

  char *values=NULL;
  int nvalue=0, mvbytes=0, kmvsize=0, *valuesizes=0;

  int64_t datasize=mv->getdatasize(mv_block_id);
  int offset=0;

  //printf("offset=%d, datasize=%d\n", offset, datasize);

  while(offset < datasize){

    //printf("offset=%d, datasize=%d\n", offset, datasize); fflush(stdout);

    GET_KMV_VARS(kv->kvtype, mv_buf, key, keybytes, nvalue, values, valuesizes, mvbytes, kmvsize, kv);

    //printf("key=%s, nvalue=%d\n", key, nvalue); fflush(stdout);

    MultiValueIterator *iter = new MultiValueIterator(nvalue,valuesizes,values,kv->kvtype,kv->vsize);
    myreduce(this, key, keybytes, iter, 1, ptr);
    delete iter;


    offset += kmvsize;
  }

  mv->release_block(mv_block_id);
}

/**
   General internal reduce function.

   @param[in]     kv input KV object
   @param[in]     myreduce user-defined reduce function
   @param[in]     ptr user-defined pointer
   @return return number of output KVs
*/
void MapReduce::_unique2mv(int tid, KeyValue *kv, Partition *p, UniqueInfo *u, DataObject *mv, int shared){
  char *key=NULL, *value=NULL;
  int keybytes=0, valuebytes=0, kvsize=0;
  //char *kvbuf;

  //DEFINE_KV_VARS;

  //printf("unique2mv: add_block, [%d,%d]->[%d,%d]  mv=%d\n", p->start_blockid, p->start_offset, p->end_blockid, p->end_offset, mv->nblock);

  int mv_blockid=mv->add_block();
  //printf("mvblockid=%d\n", mv_blockid);
  mv->acquire_block(mv_blockid);

  char *mvbuf = mv->getblockbuffer(mv_blockid);
  int mvbuf_off=0;

  for(int i=p->start_set; i<p->end_set; i++){
    Set *pset=(Set*)u->set_pool->blocks[i/nset]+i%nset;

    if(kv->kvtype==GeneralKV){
      pset->soffset=(int*)(mvbuf+mvbuf_off);
      pset->s_off=mvbuf_off;
      mvbuf_off += pset->nvalue*(int)sizeof(int);
    }
    pset->voffset=mvbuf+mvbuf_off;
    pset->v_off=mvbuf_off;
    mvbuf_off += pset->mvbytes;

    pset->nvalue=0;
    pset->mvbytes=0;
  }

#if SAFE_CHECK
  if(mvbuf_off > mv->blocksize){
    LOG_ERROR("The offset %d of MV is larger than blocksize %ld!\n", mvbuf_off, mv->blocksize);
  }
#endif

  mv->setblockdatasize(mv_blockid, mvbuf_off);

  for(int i=p->start_blockid; i<=p->end_blockid; i++){
    kv->acquire_block(i);
    char *kvbuf=kv->getblockbuffer(i);
    char *kvbuf_end=kvbuf;
    if(i<p->end_blockid)
      kvbuf_end+=kv->getdatasize(i);
    else
      kvbuf_end+=p->end_offset;
    if(i==p->start_blockid) kvbuf += p->start_offset;

    while(kvbuf<kvbuf_end){
      GET_KV_VARS(kv->kvtype, kvbuf,key,keybytes,value,valuebytes,kvsize, kv);

      //printf("second: key=%s, value=%s\n", key, value);

      uint32_t hid = hashlittle(key, keybytes, 0);
      int ibucket = hid % nbucket;

      if(shared && (uint32_t)hid%tnum != (uint32_t)tid) continue;

      Unique *ukey, *pre;
      ukey = _findukey(u->ubucket, ibucket, key, keybytes, pre);

      Set *pset=ukey->lastset;

      //if(!pset || pset->pid != mv_blockid)
      //  LOG_ERROR("Cannot find one set for key %s!\n", ukey->key);

      if(kv->kvtype==GeneralKV){
        pset->soffset[pset->nvalue]=valuebytes;
      }
      memcpy(pset->voffset+pset->mvbytes, value, valuebytes);
      pset->mvbytes+=valuebytes;
      pset->nvalue++;
    }// end while(kvbuf<kvbuf_end)

    kv->release_block(i);
  }

  mv->release_block(mv_blockid);
}

/**
   General internal reduce function.

   @param[in]     kv input KV object
   @param[in]     myreduce user-defined reduce function
   @param[in]     ptr user-defined pointer
   @return return number of output KVs
*/
void MapReduce::_mv2kmv(DataObject *mv,UniqueInfo *u, int kvtype,
  UserReduce myreduce, void* ptr){

  LOG_PRINT(DBG_CVT, "%d[%d] _mv2kmv start (kvtype=%d).\n", me, nprocs, kvtype);

  int nunique=0;
  char *ubuf;// *kmvbuf=NULL;
  //int uoff=0;
  char *ubuf_end;

  for(int i=0; i < u->unique_pool->nblock; i++){
    ubuf = u->unique_pool->blocks[i];
    ubuf_end=ubuf+u->unique_pool->blocksize;

    while(ubuf<ubuf_end){
      Unique *ukey = (Unique*)ubuf;

      if(ubuf_end-ubuf<ukeyoffset || ukey->key==NULL)
        break;

      nunique++;
      if(nunique > u->nunique) goto end;

      MultiValueIterator *iter = new MultiValueIterator(ukey, mv, kvtype);

      myreduce(this, ukey->key, ukey->keybytes, iter, 1, ptr);

      delete iter;

      ubuf += ukeyoffset;
      ubuf += ukey->keybytes;
      //ubuf = ROUNDUP(ubuf, ualignm);
    }
  }// End for

end:
  ;
  //kmv->setblockdatasize(kmv_blockid, kmvoff);
  //kmv->release_block(kmv_blockid);
  LOG_PRINT(DBG_CVT, "%d[%d] _mv2kmv end.\n", me, nprocs);
}

/**
   General internal reduce function.

   @param[in]     kv input KV object
   @param[in]     myreduce user-defined reduce function
   @param[in]     ptr user-defined pointer
   @return return number of output KVs
*/
uint64_t MapReduce::_reduce(KeyValue *kv, UserReduce myreduce, void* ptr){

  LOG_PRINT(DBG_CVT, "%d[%d] _reduce start.\n", me, nprocs);

  TRACKER_RECORD_EVENT(0, EVENT_RDC_COMPUTING);

  //uint64_t tmax_mem_bytes=0;

  nunique=0;
  uint64_t *tunique=new uint64_t[tnum];

#ifdef MTMR_MULTITHREAD
#pragma omp parallel reduction(+:tmax_mem_bytes)
{
  int tid = omp_get_thread_num();
  TRACKER_RECORD_EVENT(tid, EVENT_OMP_IDLE);
#else
  int tid = 0;
#endif

  //_tinit(tid);
  blockid=-1;
  nitem=0;

  // initialize the unique info
  UniqueInfo *u=new UniqueInfo();
  u->ubucket = new Unique*[nbucket];
  u->unique_pool=new Spool(nbucket*(int)sizeof(Unique));
  u->set_pool=new Spool(nset*(int)sizeof(Set));
  u->nunique=0;
  u->nset=0;

  memset(u->ubucket, 0, nbucket*sizeof(Unique*));

  DataObject *mv = NULL;
  mv = new DataObject(ByteType,
             blocksize,
             nmaxblock,
             maxmemsize,
             outofcore,
             tmpfpath.c_str(),
             0);
  mv->setKVsize(kv->ksize, kv->vsize);


  int isfirst = _kv2unique(tid, kv, u, mv, myreduce, ptr, 1);

  PROFILER_RECORD_COUNT(tid, COUNTER_CVT_BUCKET_SIZE, nbucket*sizeof(void*));
  PROFILER_RECORD_COUNT(tid, COUNTER_CVT_UNIQUE_SIZE, \
    (u->unique_pool->blocksize)*(u->unique_pool->nblock));
  PROFILER_RECORD_COUNT(tid, COUNTER_CVT_SET_SIZE, \
    (u->set_pool->blocksize)*(u->set_pool->nblock));

  LOG_PRINT(DBG_CVT, "%d KV2Unique end:first=%d\n", tid, isfirst);

  //if(me==0) printf("kv2unique: thread_info[0]=%d\n", thread_info[0].nitem);

  if(isfirst){
    _unique2kmv(tid, kv, u, mv, myreduce, ptr);
  }else{
    _mv2kmv(mv, u, kv->kvtype, myreduce, ptr);
  }

  PROFILER_RECORD_COUNT(tid, COUNTER_CVT_KMV_SIZE, \
    (mv->blocksize)*(mv->nblock));

  delete mv;

  //if(me==0) printf("before end: thread_info[0]=%d\n", thread_info[0].nitem);

  //printf("T%d: %ld\n", tid, u->nunique);

  tunique[tid] = u->nunique;

  delete [] u->ubucket;
  delete u->unique_pool;
  delete u->set_pool;
  delete u;

  PROFILER_RECORD_COUNT(tid, COUNTER_CVT_NUNIQUE, nitem);
  TRACKER_RECORD_EVENT(tid, EVENT_RDC_COMPUTING);

#ifdef MTMR_MULTITHREAD
}
#endif

  for(int i=0; i<tnum; i++)
    nunique += tunique[i];

  delete [] tunique;

  //printf("nunique=%d, tnum=%d\n", nunique, tnum);

  LOG_PRINT(DBG_CVT, "%d[%d] Convert(small) end.\n", me, nprocs);

  return nunique;
}

/**
   Reduce function with compress.

   @param[in]  kv input KV object
   @param[in]  myreduce user-defined reduce
   @param[in]  ptr user-defined pointer
   @return output <key,value> count
*/
uint64_t MapReduce::_reduce_compress(KeyValue *kv,
  UserReduce myreduce, void* ptr){

  LOG_PRINT(DBG_CVT, "%d[%d] MapReduce: reduce compress begin\n", me, nprocs);

  TRACKER_RECORD_EVENT(0, EVENT_RDC_COMPUTING);

 // _tinit(tid);
  nitem=0;
  blockid=-1;

  TRACKER_RECORD_EVENT(0, EVENT_OMP_IDLE);

  // initialize the unique info
  UniqueInfo *u=new UniqueInfo();
  u->ubucket = new Unique*[nbucket];
  u->unique_pool=new Spool(nbucket*(int)sizeof(Unique));
  u->nunique=0;
  memset(u->ubucket, 0, nbucket*sizeof(Unique*));

  /// \file mapreduce.cpp
  /// \todo Repair when KMV size > KV size
  char *kmv_buf=(char*)mem_aligned_malloc(MEMPAGE_SIZE, 2*blocksize);
  int kmv_off=0;

  PROFILER_RECORD_COUNT(0,COUNTER_PR_BUCKET_SIZE,nbucket*sizeof(void*));
  PROFILER_RECORD_COUNT(0,COUNTER_PR_KMV_SIZE,blocksize);

  char *key=NULL, *value=NULL;
  int keybytes=0, valuebytes=0, kvsize=0;
  char *kvbuf=NULL;
  int unique_pool_max_block=0;

#ifdef MTMR_MULTITHREAD
#pragma omp for
#endif
  for(int i=0; i<kv->nblock; i++){
    u->unique_pool->clear();
    u->nunique=0;
    kmv_off=0;
    memset(u->ubucket, 0, nbucket*sizeof(Unique*));

    // build unique structure
    kv->acquire_block(i);

    char *ubuf=u->unique_pool->add_block();
    char *ubuf_end=ubuf+u->unique_pool->blocksize;

    kvbuf=kv->getblockbuffer(i);
    int64_t datasize=kv->getdatasize(i);
    char *kvbuf_end=kvbuf+datasize;
    //int blocksize=
    //int kvbuf_off=0;

    //printf("block %d start\n", i); fflush(stdout);

    int kv_count=0;
    while(kvbuf<kvbuf_end){
      GET_KV_VARS(kv->kvtype, kvbuf,key,keybytes,value,valuebytes,kvsize,kv);

      kv_count++;

      //printf("key=%s, value=%s\n", key, value);

      uint32_t hid = hashlittle(key, keybytes, 0);
      int ibucket = hid % nbucket;

      Unique *ukey, *pre;
      ukey = _findukey(u->ubucket, ibucket, key, keybytes, pre);
      if(ukey){
        ukey->nvalue++;
        ukey->mvbytes += valuebytes;
      }else{
        if(ubuf_end-ubuf<ukeyoffset+keybytes){
          memset(ubuf, 0, ubuf_end-ubuf);
          ubuf=u->unique_pool->add_block();
          ubuf_end=ubuf+u->unique_pool->blocksize;
        }

        ukey=(Unique*)(ubuf);
        ubuf += ukeyoffset;

        // add to the list
        ukey->next = NULL;
        if(pre == NULL)
          u->ubucket[ibucket] = ukey;
        else
          pre->next = ukey;

        // copy key
        ukey->key = ubuf;
        memcpy(ubuf, key, keybytes);
        ubuf += keybytes;

        ukey->keybytes=keybytes;
        ukey->nvalue=1;
        ukey->mvbytes=valuebytes;

        u->nunique++;
      }// end else if
    }

    int nunique=0;
    Spool *unique_pool=u->unique_pool;
    for(int j=0; j<unique_pool->nblock; j++){
      char *ubuf=unique_pool->blocks[j];
      char *ubuf_end=ubuf+unique_pool->blocksize;

      while(ubuf < ubuf_end){
        Unique *ukey = (Unique*)(ubuf);
        if(((int)(ubuf_end-ubuf) < (int)sizeof(Unique)) || (ukey->key==NULL))
          break;

        nunique++;
        if(nunique>u->nunique) goto out;

        *(int*)(kmv_buf+kmv_off)=ukey->keybytes;
        kmv_off+=(int)sizeof(int);
        *(int*)(kmv_buf+kmv_off)=ukey->mvbytes;
        kmv_off+=(int)sizeof(int);
        *(int*)(kmv_buf+kmv_off)=ukey->nvalue;
        kmv_off+=(int)sizeof(int);

        if(kv->kvtype==GeneralKV){
          ukey->soffset=(int*)(kmv_buf+kmv_off);
          kmv_off+=(ukey->nvalue)*(int)sizeof(int);
        }

        ubuf+=ukeyoffset;
        memcpy(kmv_buf+kmv_off, ubuf, ukey->keybytes);
        kmv_off+=ukey->keybytes;

        ukey->voffset=kmv_buf+kmv_off;
        kmv_off+=ukey->mvbytes;

        ubuf+=ukey->keybytes;

        ukey->nvalue=0;
        ukey->mvbytes=0;
      }
    }

    //printf("kmv offset=%d, blocksize=%d\n", kmv_off, blocksize);

out:
    if(kmv_off > 2*blocksize){
      LOG_ERROR("Error: kmv size %d is larger than the buffer size %ld\n", kmv_off, blocksize);
    }

    kvbuf=kv->getblockbuffer(i);
    kvbuf_end=kvbuf+datasize;
    while(kvbuf<kvbuf_end){
      GET_KV_VARS(kv->kvtype,kvbuf,key,keybytes,value,valuebytes,kvsize,kv);

      // Find the key
      uint32_t hid = hashlittle(key, keybytes, 0);
      int ibucket = hid % nbucket;
      Unique *ukey, *pre;
      ukey = _findukey(u->ubucket, ibucket, key, keybytes, pre);

      if(kv->kvtype==GeneralKV){
        ukey->soffset[ukey->nvalue]=valuebytes;
        ukey->nvalue++;
      }

      memcpy(ukey->voffset+ukey->mvbytes, value, valuebytes);
      ukey->mvbytes+=valuebytes;
    }

    kv->delete_block(i);
    kv->release_block(i);

    if(u->unique_pool->nblock>unique_pool_max_block)
      unique_pool_max_block=u->unique_pool->nblock;

    char *values=NULL;
    int nvalue=0, mvbytes=0, kmvsize=0, *valuesizes=NULL;

    KeyValue *kv = (KeyValue*)data;
    //kv->print();

    char *mv_buf=kmv_buf;
    datasize=kmv_off;
    int offset=0;

    while(offset < datasize){
      GET_KMV_VARS(kv->kvtype, mv_buf, key, keybytes, nvalue, values, valuesizes, mvbytes, kmvsize, kv);

      MultiValueIterator *iter = new MultiValueIterator(nvalue,valuesizes,values,kv->kvtype,kv->vsize);
      myreduce(this, key, keybytes, iter, 0, ptr);
      delete iter;

      offset += kmvsize;
    }

  }// end for

  mem_aligned_free(kmv_buf);

  PROFILER_RECORD_COUNT(0, COUNTER_PR_UNIQUE_SIZE, \
    unique_pool_max_block*(u->unique_pool->blocksize));

  delete [] u->ubucket;
  delete u->unique_pool;
  //delete u->set_pool;
  delete u;

  TRACKER_RECORD_EVENT(0, EVENT_PR_COMPUTING);
#ifdef MTMR_MULTITHREAD
}
#endif

  LOG_PRINT(DBG_CVT, "%d[%d] MapReduce: reduce compress end\n", me, nprocs);

  return 0;
}

void MapReduce::print_memsize(){
  struct mallinfo mi = mallinfo();
  int64_t vmsize = (int64_t)mi.arena + (int64_t)mi.hblkhd+mi.usmblks + (int64_t)mi.uordblks+mi.fsmblks + (int64_t)mi.fordblks;
  printf("memory usage:%ld\n", vmsize);
}

void MapReduce::print_stat(MapReduce *mr, FILE *out){
//#if GATHER_STAT
  //st.print(verb, out);
//#endif

  fprintf(out, "rank:%d", me);
  fprintf(out, ",size:%d", nprocs);
  fprintf(out, ",thread:%d", tnum);

  char hostname[1024+1];
  hostname[1024] = '\0';
  gethostname(hostname, 1024);
  fprintf(out, ",hostname:%s,peakmem:%ld", hostname, peakmem);
  fprintf(out, ",maxpagecount:%d", DataObject::max_page_count);

#ifndef BGQ
  // Get maxmmap
  int64_t maxmmap = get_max_mmap();
  fprintf(out, ",maxmmap:%ld", maxmmap);
#endif

#ifdef ENABLE_PROFILER
  fprintf(out, ",profiler:enable");
#endif
#ifdef ENABLE_TRACKER
  fprintf(out, ",tracker:enable");
#endif
  fprintf(out, "\n");
  for(int i=0;i<tnum; i++){
#ifdef ENABLE_PROFILER
    fprintf(out, "action:profiler_start");
    std::map<std::string,double>::iterator timer_iter;
    for(timer_iter=profiler_event_timer[i].begin(); timer_iter!=profiler_event_timer[i].end(); timer_iter++){
      fprintf(out, ",%s:%g",timer_iter->first.c_str(), timer_iter->second);
    }
    std::map<std::string,uint64_t>::iterator counter_iter;
    for(counter_iter=profiler_event_counter[i].begin(); counter_iter!=profiler_event_counter[i].end(); counter_iter++){
      fprintf(out, ",%s:%ld", counter_iter->first.c_str(), counter_iter->second);
    }
#ifdef ENABLE_TRACKER
    double total_time=0.0, app_time=0.0, lib_time=0.0;
    std::vector<std::pair<std::string,double> >::iterator event_iter1;
    for(event_iter1=tracker_event_timer[i].begin(); event_iter1!=tracker_event_timer[i].end(); event_iter1++){
      if(event_iter1->first=="event_mr_general"){
        app_time+=event_iter1->second;
      }else{
        lib_time+=event_iter1->second;
      }
    }
    total_time=app_time+lib_time;
    fprintf(out, ",total_time:%g,app_time:%g,lib_time:%g", total_time, app_time, lib_time);
#endif
    fprintf(out, ",action:profiler_end\n");
#endif
#ifdef ENABLE_TRACKER
    fprintf(out, "action:tracker_start");
    std::vector<std::pair<std::string,double> >::iterator event_iter;
    for(event_iter=tracker_event_timer[i].begin(); event_iter!=tracker_event_timer[i].end(); event_iter++){
      fprintf(out, ",%s:%g", event_iter->first.c_str(), event_iter->second);
    }
    fprintf(out, ",action:tracker_end\n");
#endif
  }

  //PROFILER_PRINT(out, tnum);
  //TRACKER_PRINT(out, tnum);
}

//void MapReduce::init_stat(){
//#if GATHER_STAT
  //st.clear();
//#endif
//}

/*
 * Output data in this object
 *  type: 0 for string, 1 for int, 2 for int64_t
 *  fp:     file pointer
 *  format: hasn't been used
 */
void MapReduce::output(int type, FILE* fp, int format){
  if(data){
    data->print(type, fp, format);
  }else{
    LOG_ERROR("%s","Error to output empty data object\n");
  }
}

// private function
/*****************************************************************************/

// process init
void MapReduce::_get_default_values(){

  char *env = NULL;

  env = getenv("MIMIR_COMM_UNIT_SIZE");
  if(env){
    COMM_UNIT_SIZE=_stringtoint(env);
    if(COMM_UNIT_SIZE<=0 || COMM_UNIT_SIZE>1024*1024*1024)
      LOG_ERROR("Error: COMM_UNIT_SIZE (%d) should be > 0 and <1G!\n", COMM_UNIT_SIZE);
  }
  env = getenv("MIMIR_DBG_GEN");
  if(env){
    int flag = atoi(env);
    if(flag != 0){
      DBG_LEVEL |= (DBG_GEN); 
    }
  }
  env = getenv("MIMIR_DBG_DATA");
  if(env){
    int flag = atoi(env);
    if(flag != 0){
      DBG_LEVEL |= (DBG_DATA); 
    }
  }
  env = getenv("MIMIR_DBG_COMM");
  if(env){
    int flag = atoi(env);
    if(flag != 0){
      DBG_LEVEL |= (DBG_COMM);
    }
  }
  env = getenv("MIMIR_DBG_IO");
  if(env){
    int flag = atoi(env);
    if(flag != 0){
      DBG_LEVEL |= (DBG_IO);
    }
  }

  inputsize = INPUT_SIZE;
  blocksize = BLOCK_SIZE;
  nmaxblock = MAX_BLOCKS;
  maxmemsize = MAXMEM_SIZE;
  //lbufsize = LOCAL_BUF_SIZE;
  gbufsize = GLOBAL_BUF_SIZE;

  kvtype = GeneralKV;

  outofcore = OUT_OF_CORE;
  //tmpfpath = std::string(TMP_PATH);

  commmode=0;

  myhash = NULL;

  nbucket=pow(2, BUCKET_SIZE);
  nset = nbucket;
  estimate = 0;
  factor = 1;

  ukeyoffset = sizeof(Unique);

  maxkeybytes=MAX_KEY_SIZE;
  maxvaluebytes=MAX_VALUE_SIZE;
}

int64_t MapReduce::_stringtoint(const char *_str){
  std::string str=_str;
  int64_t num=0;
  if(str[str.size()-1]=='b'||str[str.size()-1]=='B'){
    str=str.substr(0, str.size()-1);
    num=atoi(str.c_str());
    num*=1;
  }else if(str[str.size()-1]=='k'||str[str.size()-1]=='K'||\
    (str[str.size()-1]=='b'&&str[str.size()-2]=='k')||\
    (str[str.size()-1]=='B'&&str[str.size()-2]=='K')){
    if(str[str.size()-1]=='b'||str[str.size()-1]=='B'){
      str=str.substr(0, str.size()-2);
    }else{
      str=str.substr(0, str.size()-1);
    }
    num=atoi(str.c_str());
    num*=1024;
  }else if(str[str.size()-1]=='m'||str[str.size()-1]=='M'||\
    (str[str.size()-1]=='b'&&str[str.size()-2]=='m')||\
    (str[str.size()-1]=='B'&&str[str.size()-2]=='M')){
    if(str[str.size()-1]=='b'||str[str.size()-1]=='B'){
      str=str.substr(0, str.size()-2);
    }else{
      str=str.substr(0, str.size()-1);
    }
    num=atoi(str.c_str());
    num*=1024*1024;
  }else if(str[str.size()-1]=='g'||str[str.size()-1]=='G'||\
    (str[str.size()-1]=='b'&&str[str.size()-2]=='g')||\
    (str[str.size()-1]=='B'&&str[str.size()-2]=='G')){
    if(str[str.size()-1]=='b'||str[str.size()-1]=='B'){
      str=str.substr(0, str.size()-2);
    }else{
      str=str.substr(0, str.size()-1);
    }
    num=atoi(str.c_str());
    num*=1024*1024*1024;
  }else{
    LOG_ERROR("Error: set buffer size %s error! \
      The buffer size should end with b,B,k,K,kb,KB,m,M,mb,MB,g,G,gb,GB", _str);
  }
  if(num==0){
    LOG_ERROR("Error: buffer size %s should not be zero!", _str);
  }

  return num;
}


// distribute input file list
void MapReduce::_disinputfiles(const char *filepath, int sharedflag, int recurse){

  TRACKER_RECORD_EVENT(0, EVENT_MAP_DIS_FILES);
  _getinputfiles(filepath, sharedflag, recurse);
  TRACKER_RECORD_EVENT(0, EVENT_MAP_GET_INPUT);

  if(sharedflag){
    int fcount = (int)ifiles.size();
    int div = fcount / nprocs;
    int rem = fcount % nprocs;
    int *send_count = new int[nprocs];
    int total_count = 0;

    if(me == 0){
      int j = 0, end=0;
      for(int i = 0; i < nprocs; i++){
        send_count[i] = 0;
        end += div;
        if(i < rem) end++;
        while(j < end){
          send_count[i] += (int)strlen(ifiles[j].c_str())+1;
          j++;
        }
        total_count += send_count[i];
      }
    }

    int recv_count;
    MPI_Scatter(send_count, 1, MPI_INT, &recv_count, 1, MPI_INT, 0, comm);

    int *send_displs = new int[nprocs];
    if(me == 0){
      send_displs[0] = 0;
      for(int i = 1; i < nprocs; i++){
        send_displs[i] = send_displs[i-1]+send_count[i-1];
      }
    }

    char *send_buf = new char[total_count];
    char *recv_buf = new char[recv_count];

    if(me == 0){
      int offset = 0;
      for(int i = 0; i < fcount; i++){
          memcpy(send_buf+offset, ifiles[i].c_str(), strlen(ifiles[i].c_str())+1);
          offset += (int)strlen(ifiles[i].c_str())+1;
        }
    }

    MPI_Scatterv(send_buf, send_count, send_displs, MPI_BYTE, recv_buf, recv_count, MPI_BYTE, 0, comm);

    ifiles.clear();
    int off=0;
    while(off < recv_count){
      char *str = recv_buf+off;
      ifiles.push_back(std::string(str));
      off += (int)strlen(str)+1;
    }

    delete [] send_count;
    delete [] send_displs;
    delete [] send_buf;
    delete [] recv_buf;
  }
}

// get input file list
void MapReduce::_getinputfiles(const char *filepath, int sharedflag, int recurse){
  // if shared, only process 0 read file names
  if(!sharedflag || (sharedflag && me == 0)){

    struct stat inpath_stat;
    int err = stat(filepath, &inpath_stat);
    if(err) LOG_ERROR("Error in get input files, err=%d\n", err);

    // regular file
    if(S_ISREG(inpath_stat.st_mode)){
      ifiles.push_back(std::string(filepath));
    // dir
    }else if(S_ISDIR(inpath_stat.st_mode)){

      struct dirent *ep;
      DIR *dp = opendir(filepath);
      if(!dp) LOG_ERROR("%s", "Error in get input files\n");

      while(ep = readdir(dp)){

#ifdef BGQ
        if(ep->d_name[1] == '.') continue;
#else
        if(ep->d_name[0] == '.') continue;
#endif

        char newstr[MAXLINE];
#ifdef BGQ
        sprintf(newstr, "%s/%s", filepath, &(ep->d_name[1]));
#else
        sprintf(newstr, "%s/%s", filepath, ep->d_name);
#endif
        err = stat(newstr, &inpath_stat);
        if(err) LOG_ERROR("Error in get input files, err=%d\n", err);

        // regular file
        if(S_ISREG(inpath_stat.st_mode)){
          ifiles.push_back(std::string(newstr));
        // dir
        }else if(S_ISDIR(inpath_stat.st_mode) && recurse){
          _getinputfiles(newstr, sharedflag, recurse);
        }
      }
    }
  }
}

inline uint64_t MapReduce::_get_kv_count(){
    local_kvs_count=0;
    //for(int i=0; i<tnum; i++) local_kvs_count+=thread_info[i].nitem;
    local_kvs_count=nitem;
    MPI_Allreduce(&local_kvs_count, &global_kvs_count, 1, MPI_UINT64_T, MPI_SUM, comm);
    TRACKER_RECORD_EVENT(0, EVENT_COMM_ALLREDUCE);
    return  global_kvs_count;
  }


MultiValueIterator::MultiValueIterator(int _nvalue, int *_valuebytes, char *_values, int _kvtype, int _vsize){
    nvalue=_nvalue;
    valuebytes=_valuebytes;
    values=_values;
    kvtype=_kvtype;
    vsize=_vsize;

    mode=0;

    Begin();
}

MultiValueIterator::MultiValueIterator(MapReduce::Unique *_ukey, DataObject *_mv, int _kvtype){
    mv=_mv;
    ukey=_ukey;

    nvalue=ukey->nvalue;
    kvtype=_kvtype;
    vsize=mv->vsize;
    pset=NULL;

    mode=1;

    //printf("vsize=%d\n", vsize); fflush(stdout);

    Begin();
}

void MultiValueIterator::Begin(){
    ivalue=0;
    value_start=0;
    isdone=0;
    if(ivalue >= nvalue) isdone=1;
    else{
      if(mode==1){
         pset=ukey->firstset;
         mv->acquire_block(pset->pid);
         char *tmpbuf = mv->getblockbuffer(pset->pid);
         pset->soffset = (int*)(tmpbuf + pset->s_off);
         pset->voffset = tmpbuf + pset->v_off;

         valuebytes=pset->soffset;
         values=pset->voffset;

         value_end=pset->nvalue;
      }
      value=values;
      if(kvtype==GeneralKV) valuesize=valuebytes[ivalue-value_start];
      else if(kvtype==StringKV) valuesize=(int)strlen(value)+1;
      else if(kvtype==2 || kvtype==3) valuesize=vsize;
   }

  // printf("ivalue=%d,value=%p,valuesize=%d\n", ivalue, value, valuesize); fflush(stdout);

}

void MultiValueIterator::Next(){
    ivalue++;
    if(ivalue >= nvalue) {
      isdone=1;
      if(mode==1 && pset){
         mv->release_block(pset->pid);
      }
    }else{
      if(mode==1 && ivalue >=value_end){
        value_start += pset->nvalue;
        mv->release_block(pset->pid);
        pset=pset->next;
        mv->acquire_block(pset->pid);
        char *tmpbuf = mv->getblockbuffer(pset->pid);
        pset->soffset = (int*)(tmpbuf + pset->s_off);
        pset->voffset = tmpbuf + pset->v_off;

        valuebytes=pset->soffset;
        values=pset->voffset;

        value=values;
        value_end+=pset->nvalue;
      }else{
        value+=valuesize;
      }
      if(kvtype==GeneralKV) valuesize=valuebytes[ivalue-value_start];
      else if(kvtype==StringKV) valuesize=(int)strlen(value)+1;
      else if(kvtype==FixedKV || kvtype==StringKFixedV) valuesize=vsize;
    }
    //printf("ivalue=%d,value=%p,valuesize=%d\n", ivalue, value, valuesize); fflush(stdout);
}

