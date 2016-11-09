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
//#include "spool.h"
#include "log.h"
#include "config.h"
#include "const.h"
#include "hash.h"
#include "memory.h"
#include "stat.h"
#include "hashbucket.h"

using namespace MIMIR_NS;


/**
   Create MapReduce Object

   @param[in]     _caller MPI Communicator
   @return return MapReduce Object.
*/
MapReduce::MapReduce(MPI_Comm _caller)
{
    // Get coommunicator information
    comm = _caller;
    MPI_Comm_rank(comm,&me);
    MPI_Comm_size(comm,&nprocs);

    // Get default values
    _get_default_values();

    // Initalize stat
    INIT_STAT(me, nprocs); 

    LOG_PRINT(DBG_GEN, me, nprocs, "%s", "MapReduce: create.\n");
}

/**
   Copy MapReduce Object

   @param[in]     _mr original MapReduce Object
   @return return new MapReduce Object.
*/
MapReduce::MapReduce(const MapReduce &_mr){
#if 0
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

  tnum=1;
  TRACKER_START(1);
  PROFILER_START(1);
  TRACKER_TIMER_INIT(0);
#endif

    INIT_STAT(me, nprocs); 

    LOG_PRINT(DBG_GEN, me, nprocs, "%s", "MapReduce: copy\n");
}

/**
   Destory MapReduce Object
*/
MapReduce::~MapReduce()
{
    DataObject::subRef(kv);
    if(c) delete c;

    UNINT_STAT; 

    LOG_PRINT(DBG_GEN, me, nprocs, "%s", "MapReduce: destroy.\n");
}

uint64_t MapReduce::map_text_file( \
    char *_filepath, int _shared, int _recurse, char *_seperator, \
    UserMapFile _mymap, void *_ptr, int _comm){

    if(strlen(_seperator) == 0)
        LOG_ERROR("%s", "Error: the separator should not be empty!\n");

    LOG_PRINT(DBG_GEN, me, nprocs, "MapReduce: map_text_file start. \
(filepath=%s, shared=%d, recursed=%d, seperator=%s)\n", \
        _filepath, _shared, _recurse, _seperator);

    TRACKER_RECORD_EVENT(EVENT_COMPUTE_OTHER);

    myptr = _ptr;

    // Distribute file list
    ifiles.clear();
    _dist_input_files(_filepath, _shared, _recurse);

    TRACKER_RECORD_EVENT(EVENT_INIT_GETFILES);

    DataObject::subRef(kv);

    // Create KV Container
    kv = new KeyValue(me,nprocs,DATA_PAGE_SIZE, MAX_PAGE_COUNT);
    kv->set_combiner(this, mycombiner);

    if(_comm){
        c=Communicator::Create(comm, KV_EXCH_COMM);
        c->setup(COMM_BUF_SIZE, kv, this, mycombiner, myhash);
        phase = MapPhase;
    // local map
    }else{
        phase = LocalMapPhase;
    }

    // Compute input buffer size
    int fcount = ifiles.size();
    int64_t max_fsize=0;
    for(int i=0; i<fcount; i++){
        if(ifiles[i].second>max_fsize)
            max_fsize=ifiles[i].second;
    }
    int64_t input_buffer_size=0;
    if(max_fsize<=INPUT_BUF_SIZE) input_buffer_size=max_fsize;
    else input_buffer_size=INPUT_BUF_SIZE;

    // Allocate input buffer
    char *text = (char*)mem_aligned_malloc(\
        MEMPAGE_SIZE, input_buffer_size+MAX_STR_SIZE+1);

   
    for(int i = 0; i < fcount; i++){
        int64_t input_char_size=0, fsize=0;

        fsize=ifiles[i].second;

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

        FILE *fp = fopen(ifiles[i].first.c_str(), "r");
        if(fp == NULL){
            LOG_ERROR("Error: open file %s error!", ifiles[i].first.c_str());
        }

        LOG_PRINT(DBG_IO, me, nprocs, "open file %s, fsize=%ld\n", \
            ifiles[i].first.c_str(), ifiles[i].second);

        TRACKER_RECORD_EVENT(EVENT_DISK_OPEN);

        // Process the file
        int64_t foff = 0, boff = 0;
        int64_t readsize=0;

        do{
            TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

            // Read file
            fseek(fp, foff, SEEK_SET);

            TRACKER_RECORD_EVENT(EVENT_DISK_SEEK);

            readsize = fread(text+boff, 1, input_buffer_size, fp);

            TRACKER_RECORD_EVENT(EVENT_DISK_READ);

            // read a block
            text[boff+readsize] = '\0';
            input_char_size = boff+readsize;

            int64_t tend=input_char_size;
            boff=0;
            if(readsize >= input_buffer_size && foff+readsize<fsize){
              while(strchr(_seperator, text[input_char_size-boff-1])==NULL) boff++;
              tend-=(boff+1);
              text[tend]='\0';
            }

            if(boff > MAX_STR_SIZE) 
                LOG_ERROR("Error: string length is large than max size (%d)!\n", \
                    MAX_STR_SIZE);

            LOG_PRINT(DBG_IO, me, nprocs, "read file %s, %ld->%ld, suffix=%ld\n", \
                ifiles[i].first.c_str(), foff, foff+readsize, boff);

            // Pass words one by one to user-defined map function
            char *saveptr = NULL;
            char *word = strtok_r(text, _seperator, &saveptr);
            while(word){
                _mymap(this, word, _ptr);
                word = strtok_r(NULL, _seperator, &saveptr);
            }
             
            // Prepare for next buffer
            foff += readsize;

            for(int j =0; j < boff; j++) text[j] = text[input_char_size-boff+j];

        }while(foff<fsize);

        TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

        fclose(fp);

        TRACKER_RECORD_EVENT(EVENT_DISK_CLOSE);

        LOG_PRINT(DBG_IO, me, nprocs, "close file %s\n", ifiles[i].first.c_str());
    }

    // Free input buffer
    mem_aligned_free(text);

    // Delete communicator
    if(_comm){
        c->wait();
        delete c;
        c = NULL;
    }

    // Garbage collection
    kv->gc();

    DataObject::addRef(kv);

    phase = NonePhase;

    TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

    LOG_PRINT(DBG_GEN, me, nprocs, "%s", "MapReduce: map_text_file end.\n");

    return _get_kv_count();
}

/**
   Map function KV input

*/
uint64_t MapReduce::map_key_value(MapReduce *_mr,
    UserMapKV _mymap, void *_ptr, int _comm){

    LOG_PRINT(DBG_GEN, me, nprocs, "%s", "MapReduce: map start. (KV as input)\n");

    //TRACKER_RECORD_EVENT(EVENT_MR_GENERAL);

    DataObject::addRef(_mr->kv);
    DataObject::subRef(kv);

    KeyValue *inputkv = _mr->kv;

    // create new data object
    LOG_PRINT(DBG_GEN, me, nprocs, "%s", "MapReduce: new data KV. (KV as input)\n");
    kv = new KeyValue(me,nprocs,DATA_PAGE_SIZE, MAX_PAGE_COUNT);
    kv->set_combiner(this, mycombiner);

    LOG_PRINT(DBG_GEN, me, nprocs, "%s", "MapReduce: alloc data KV. (KV as input)\n");

    if(_comm){
        c=Communicator::Create(comm, KV_EXCH_COMM);
        c->setup(COMM_BUF_SIZE, kv, this, mycombiner, myhash);
        phase = MapPhase;
    }else{
        phase = LocalMapPhase;
    }

    char *key, *value;
    int keybytes, valuebytes;

    int i;
    for(i = 0; i < inputkv->get_npages(); i++){
        int64_t offset = 0;

        inputkv->acquire_page(i);

        offset = inputkv->getNextKV(&key, keybytes, &value, valuebytes);

        while(offset != -1){

            _mymap(this, key, keybytes, value, valuebytes, _ptr);

            offset = inputkv->getNextKV(&key, keybytes, &value, valuebytes);
        }

        inputkv->delete_page(i);
        inputkv->release_page(i);
    }

    DataObject::subRef(inputkv);

    if(_comm){
        c->wait();
        delete c;
        c = NULL;
    }

    kv->gc();
    DataObject::addRef(kv);
    phase=NonePhase;

    //TRACKER_RECORD_EVENT(EVENT_MAP_COMPUTING);

    LOG_PRINT(DBG_GEN, me, nprocs, "%s", "MapReduce: map end. (KV as input)\n");

    return _get_kv_count();
}


/**
   Map function without input
*/
uint64_t MapReduce::init_key_value(UserInitKV _myinit, \
  void *_ptr, int _comm){

    LOG_PRINT(DBG_GEN, me, nprocs, "%s", "MapReduce: map start. (no input)\n");

    //TRACKER_RECORD_EVENT(EVENT_MR_GENERAL);

    DataObject::subRef(kv);
    kv = new KeyValue(me,nprocs,DATA_PAGE_SIZE, MAX_PAGE_COUNT);
    kv->set_combiner(this, mycombiner);

    if(_comm){
        c=Communicator::Create(comm, KV_EXCH_COMM);
        c->setup(COMM_BUF_SIZE, kv, this, mycombiner, myhash);
        phase = MapPhase;
    }else{
        phase = LocalMapPhase;
    }

    _myinit(this, _ptr);

    // wait all processes done
    if(_comm){
        c->wait();
        delete c;
        c = NULL;
    }

    phase = NonePhase;

    //TRACKER_RECORD_EVENT(EVENT_MAP_COMPUTING);

    LOG_PRINT(DBG_GEN, me, nprocs, "%s", "MapReduce: map end. (no input)\n");

    return _get_kv_count();
}

/**
   General internal reduce function.

   @param[in]     kv input KV object
   @param[in]     myreduce user-defined reduce function
   @param[in]     ptr user-defined pointer
   @return return number of output KVs
*/
uint64_t MapReduce::reduce(UserReduce myreduce, void* ptr){

    LOG_PRINT(DBG_GEN, me, nprocs, "%s", "MapReduce: reduce start.\n");
   
    phase = ReducePhase;

    DataObject *mv = new DataObject(me,nprocs,ByteType,\
        DATA_PAGE_SIZE, MAX_PAGE_COUNT);    
    ReducerHashBucket *u = new ReducerHashBucket(kv);

    _convert(kv, mv, u);
    DataObject::subRef(kv);

    kv = new KeyValue(me,nprocs,DATA_PAGE_SIZE, MAX_PAGE_COUNT); 
    _reduce(u, myreduce, ptr);
    delete mv;
    delete u;

    DataObject::addRef(kv);
    phase = NonePhase;

    LOG_PRINT(DBG_GEN, me, nprocs, "%s", "MapReduce: reduce end.\n");

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
void MapReduce::add_key_value(char *key, int keybytes, char *value, int valuebytes){

    // Map Phase
    if(phase == MapPhase){

        // Send KV
        c->sendKV(key, keybytes, value, valuebytes);

        return;
    // Local Mode
    }else if(phase == LocalMapPhase || phase == ReducePhase){

        // Add KV
        kv->addKV(key, keybytes, value, valuebytes);

        return;
    }else{
        LOG_ERROR("%s", \
          "Error: add_key_value function can be invoked in map and \
reduce callbacks\n");
    }

    return;
}

void MapReduce::update_key_value(
    char *key, 
    int keybytes, 
    char *value, 
    int valuebytes){
    if(phase == MapPhase || phase == LocalMapPhase){
        newkey = key;
        newkeysize = keybytes;
        newval = value;
        newvalsize = valuebytes;
    }else{
        LOG_ERROR("%s", \
          "Error: update_key_value function can be invoked in \
combiner callbacks\n");
    }
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
  LOG_PRINT(DBG_GEN, me, nprocs, "%s", "MapReduce: scan begin\n");

  //TRACKER_RECORD_EVENT(EVENT_MR_GENERAL);

    char *key, *value;
    int keybytes, valuebytes;

    int i;
    for(i = 0; i < kv->get_npages(); i++){
        int64_t offset = 0;

        kv->acquire_page(i);

        offset = kv->getNextKV(&key, keybytes, &value, valuebytes);

        while(offset != -1){

            _myscan(key, keybytes, value, valuebytes, _ptr);

            offset = kv->getNextKV(&key, keybytes, &value, valuebytes);
        }

        kv->release_page(i);
    }

    //TRACKER_RECORD_EVENT(EVENT_SCAN_COMPUTING);

    LOG_PRINT(DBG_GEN, me, nprocs, "%s", "MapReduce: scan end.\n");
}

void MapReduce::_reduce(ReducerHashBucket *h, UserReduce _myreduce, void* ptr){
    ReducerUnique *u = h->BeginUnique();
    while(u!=NULL){
        ReducerSet *set = u->lastset;
        printf("%s\t%d\t%ld\t%ld\n", u->key, u->keybytes, \
            u->nvalue, u->mvbytes);
        printf("\t\t%ld\t%ld\n", set->nvalue, set->mvbytes);
        MultiValueIterator *iter=new MultiValueIterator(u);
        //_myreduce(this, u->key, u->keybytes, iter, ptr);
        delete iter;
        u = h->NextUnique();
    }
}

void MapReduce::_convert(KeyValue *inputkv, \
    DataObject *mv, \
    ReducerHashBucket *h){

    char *key, *value;
    int keybytes, valuebytes;
    int i;

    ReducerUnique u;
    for(i = 0; i < inputkv->get_npages(); i++){
        int64_t offset = 0;

        inputkv->acquire_page(i);

        offset = inputkv->getNextKV(&key, keybytes, &value, valuebytes);

        while(offset != -1){

            //printf("1.key=%s\n", key); fflush(stdout);

            u.key = key;
            u.keybytes = keybytes;
            u.mvbytes = valuebytes;
            h->insertElem(&u);
            //_mymap(this, key, keybytes, value, valuebytes, _ptr);
            
            offset = inputkv->getNextKV(&key, keybytes, &value, valuebytes);
        }

        //inputkv->delete_page(i);
        inputkv->release_page(i);
    }

    char *page_buf=NULL, page_off=0, page_id=0;
    ReducerSet *set = h->BeginSet();
    while(set!=NULL){
        if(page_buf==NULL || page_id!=set->pid){
            page_id=mv->add_page();
            page_buf=mv->get_page_buffer(page_id);
            page_off=0;
        }
        if(inputkv->kvtype==GeneralKV ||\
          inputkv->kvtype==StringKGeneralV || \
          inputkv->kvtype==FixedKGeneralV){
            set->soffset=(int*)(page_buf+page_off);
            page_off+=sizeof(int)*set->nvalue;
        }else{
            set->soffset=NULL;
        }
        set->voffset=page_buf+page_off;
        page_off+=set->mvbytes;
        set = h->NextSet();
    }

    for(i = 0; i < inputkv->get_npages(); i++){
        int64_t offset = 0;

        inputkv->acquire_page(i);

        offset = inputkv->getNextKV(&key,keybytes,&value,valuebytes);

        while(offset != -1){
            ReducerUnique *punique = h->findElem(key, keybytes);
            ReducerSet *pset = punique->firstset;

            if(inputkv->kvtype==GeneralKV ||\
              inputkv->kvtype==StringKGeneralV || \
              inputkv->kvtype==FixedKGeneralV){
                set->soffset[set->ivalue]=valuebytes;
                memcpy(set->voffset, value, valuebytes);
                set->voffset+=valuebytes;
                set->ivalue+=1;
                if(set->ivalue==set->nvalue){
                    punique->firstset=punique->firstset->next;
                }
                page_off+=sizeof(int)*set->nvalue;
            }else{
                set->soffset=NULL;
            }
           
            offset = inputkv->getNextKV(&key,keybytes,&value,valuebytes);
        }

        inputkv->delete_page(i);
        inputkv->release_page(i);
    }
}


void MapReduce::output_stat(const char *filename){
  
    char tmp[1024];

    sprintf(tmp, "%s.profiler.%d.%d", filename, stat_size, stat_rank);
    FILE *fp = fopen(tmp, "w+");
    PROFILER_PRINT(fp);
    fclose(fp);

    sprintf(tmp, "%s.trace.%d.%d", filename, stat_size, stat_rank);
    fp = fopen(tmp, "w+");
    TRACKER_PRINT(fp);
    fclose(fp);
}


/*
 * Output data in this object
 *  type: 0 for string, 1 for int, 2 for int64_t
 *  fp:     file pointer
 *  format: hasn't been used
 */
void MapReduce::output(FILE *fp, ElemType ktype, ElemType vtype){
    if(kv){
        kv->print(fp, ktype, vtype);
    }
  //if(data){
  //  data->print(type, fp, format);
  //}else{
  //  LOG_ERROR("%s","Error to output empty data object\n");
  //}
}

// private function
/*****************************************************************************/

// process init
void MapReduce::_get_default_values(){ 
    /// Initialize member of MapReduce
    myhash = NULL;
    mycombiner = NULL;
    phase = NonePhase;
    kv = NULL;
    c = NULL;

    /// Configure main parameters
    char *env = NULL;
    env = getenv("MIMIR_BUCKET_SIZE");
    if(env){
        BUCKET_COUNT=pow(2,atoi(env));
        if(BUCKET_COUNT<=0)
            LOG_ERROR("Error: set bucket size error, please set MIMIR_BUCKET_SIZE (%s) correctly!\n", env);
    }
    env = getenv("MIMIR_COMM_SIZE");
    if(env){
        COMM_BUF_SIZE=_convert_to_int64(env);
        if(COMM_BUF_SIZE<=0)
            LOG_ERROR("Error: set communication buffer size error, please set MIMIR_COMM_SIZE (%s) correctly!\n", env);
    }
    env = getenv("MIMIR_PAGE_SIZE");
    if(env){
        DATA_PAGE_SIZE=_convert_to_int64(env);
        if(DATA_PAGE_SIZE<=0)
            LOG_ERROR("Error: set page size error, please set DATA_PAGE_SIZE (%s) correctly!\n", env);
    } 
    env = getenv("MIMIR_INBUF_SIZE");
    if(env){
        INPUT_BUF_SIZE=_convert_to_int64(env);
        if(INPUT_BUF_SIZE<=0)
            LOG_ERROR("Error: set input buffer size error, please set INPUT_BUF_SIZE (%s) correctly!\n", env);
    }

    /// Configure unit size for communication buffer 
    env = NULL;
    env = getenv("MIMIR_COMM_UNIT_SIZE");
    if(env){
        COMM_UNIT_SIZE=_convert_to_int64(env);
        if(COMM_UNIT_SIZE<=0 || COMM_UNIT_SIZE>1024*1024*1024)
            LOG_ERROR("Error: COMM_UNIT_SIZE (%d) should be > 0 and <1G!\n", COMM_UNIT_SIZE);
    }

    // Configure debug level
    env = getenv("MIMIR_DBG_ALL");
    if(env){
        int flag = atoi(env);
        if(flag != 0){
            DBG_LEVEL |= (DBG_GEN|DBG_COMM|DBG_IO|DBG_DATA); 
        }
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

    
}

int64_t MapReduce::_convert_to_int64(const char *_str){
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
void MapReduce::_dist_input_files(const char *filepath, int sharedflag, int recurse){

    _get_input_files(filepath, sharedflag, recurse);

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
                    send_count[i]+=(int)strlen(ifiles[j].first.c_str())+1;
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
                memcpy(send_buf+offset,ifiles[i].first.c_str(),\
                    strlen(ifiles[i].first.c_str())+1);
                offset+=(int)strlen(ifiles[i].first.c_str())+1;
            }
        }

        MPI_Scatterv(send_buf, send_count, send_displs, MPI_BYTE, \
            recv_buf, recv_count, MPI_BYTE, 0, comm);

        ifiles.clear();
        int off=0;
        while(off < recv_count){
            char *str = recv_buf+off;
            struct stat file_stat;
            int err = stat(str, &file_stat);
            if(err) LOG_ERROR("Error in get input files, err=%d\n", err);
            int64_t fsize = file_stat.st_size;
            ifiles.push_back(std::make_pair(std::string(str), fsize));
            off += (int)strlen(str)+1;
        }

        delete [] send_count;
        delete [] send_displs;
        delete [] send_buf;
        delete [] recv_buf;
    }
}

// get input file list
void MapReduce::_get_input_files(const char *filepath, int sharedflag, int recurse){
  // if shared, only process 0 read file names
    if(!sharedflag || (sharedflag && me == 0)){

        struct stat inpath_stat;
        int err = stat(filepath, &inpath_stat);
        if(err) LOG_ERROR("Error in get input files, err=%d\n", err);

        // regular file
        if(S_ISREG(inpath_stat.st_mode)){
            int64_t fsize = inpath_stat.st_size;
            ifiles.push_back(\
                std::make_pair(std::string(filepath),fsize));
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
                    int64_t fsize = inpath_stat.st_size;
                    ifiles.push_back(\
                        std::make_pair(std::string(newstr),fsize));
                // dir
                }else if(S_ISDIR(inpath_stat.st_mode) && recurse){
                    _get_input_files(newstr, sharedflag, recurse);
                }
            }
        }
    }
}

#if 1
inline uint64_t MapReduce::_get_kv_count(){
    uint64_t local_count=0,global_count=0;
    local_count=kv->get_local_count();
    MPI_Allreduce(&local_count, &global_count, 1, MPI_UINT64_T, MPI_SUM, comm);
    kv->set_global_count(global_count);
    TRACKER_RECORD_EVENT(EVENT_COMM_ALLREDUCE);
    return  global_count;
  }
#endif


MultiValueIterator::MultiValueIterator(ReducerUnique *_ukey){
    ukey = _ukey;
} 

void MultiValueIterator::Begin(){
    ivalue=0;
    value_start=0;
    isdone=0;
    if(ivalue >= nvalue) isdone=1;
    else{
        pset=ukey->firstset;
        valuebytes=pset->soffset;
        values=pset->voffset;
        value_end=pset->nvalue;
    }
    value=values;
    if(kvtype==GeneralKV || kvtype==StringKGeneralV || \
        kvtype==FixedKGeneralV)
        valuesize=valuebytes[ivalue-value_start];
    else if(kvtype==StringKV || kvtype==FixedKStringV || \
        kvtype==GeneralKStringV)
        valuesize=(int)strlen(value)+1;
    else if(kvtype==FixedKV || kvtype==StringKFixedV || \
        kvtype==GeneralKFixedV)
        valuesize=vsize;
}

void MultiValueIterator::Next(){
    ivalue++;
    if(ivalue >= nvalue) {
      isdone=1;
    }else{
      if(ivalue >=value_end){
        value_start += pset->nvalue;
        pset=pset->next;

        valuebytes=pset->soffset;
        values=pset->voffset;

        value=values;
        value_end+=pset->nvalue;
      }else{
        value+=valuesize;
      }
      if(kvtype==GeneralKV || kvtype==StringKGeneralV || \
        kvtype==FixedKGeneralV)
        valuesize=valuebytes[ivalue-value_start];
      else if(kvtype==StringKV || kvtype==FixedKStringV || \
        kvtype==GeneralKStringV)
        valuesize=(int)strlen(value)+1;
      else if(kvtype==FixedKV || kvtype==StringKFixedV || \
        kvtype==GeneralKFixedV)
        valuesize=vsize;
    }
}

