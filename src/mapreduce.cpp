#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <dirent.h>

#include <iostream>
#include <string>
#include <list>
#include <vector>

#include <sys/stat.h>

#include <mpi.h>
#include <omp.h>

#include "mapreduce.h"
#include "dataobject.h"
#include "log.h"

#include "config.h"

using namespace MAPREDUCE_NS;

// Position in data object
struct Pos{

  Pos(
    int _off, 
    int _size,
    int _bid=0,
    int _nval=0){
    bid = _bid;
    nval = _nval;
    off = _off;
    size = _size;
  }

  int   bid;     // block id
  int   off;     // offset in block   
  int   size;    // size of data

  int   nval;    // value count
};

// used for merge
struct Unique
{
  char *key;             // unique key
  int keybytes;          // key size

  // the information of current search block
  int cur_size;
  int cur_nval;
  std::list<Pos> cur_pos;

  // the information of all blocks
  int size;
  int nval;
  std::list<Pos> pos;
};

MapReduce::MapReduce(MPI_Comm caller)
{
    comm = caller;
    MPI_Comm_rank(comm,&me);
    MPI_Comm_size(comm,&nprocs);

#pragma omp parallel
{
    tnum = omp_get_num_threads();    
}

    blocks = new int[tnum];   
    nitems = new uint64_t[tnum];
    for(int i = 0; i < tnum; i++){
      blocks[i] = -1;
      nitems[i] = 0;
    }

    data = NULL;
    c = NULL;

    mode = NoneMode;
  
    init();

    fprintf(stdout, "Process count=%d, thread count=%d\n", nprocs, tnum);

    LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: create. (thread number=%d)\n", me, nprocs, tnum);
}

MapReduce::~MapReduce()
{
    if(data) delete data;

    if(c) delete c;

    delete [] nitems;
    delete [] blocks;

    LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: destroy.\n", me, nprocs);
}

// configurable functions
/******************************************************/
void MapReduce::setKVtype(int _kvtype, int _ksize, int _vsize){
  kvtype = _kvtype;
  ksize = _ksize;
  vsize = _vsize;
}

void MapReduce::setBlocksize(int _blocksize){
  blocksize = _blocksize;
}

void MapReduce::setMaxblocks(int _nmaxblock){
  nmaxblock = _nmaxblock;
}

void MapReduce::setMaxmem(int _maxmemsize){
  maxmemsize = _maxmemsize;
}

void MapReduce::setTmpfilePath(const char *_fpath){
  tmpfpath = std::string(_fpath);
}

void MapReduce::setOutofcore(int _flag){
  outofcore = _flag;
}

void MapReduce::setLocalbufsize(int _lbufsize){
  lbufsize = _lbufsize;
}

void MapReduce::setGlobalbufsize(int _gbufsize){
  gbufsize = _gbufsize;
}

void MapReduce::sethash(int (*_myhash)(char *, int)){
  myhash = _myhash;
}


/*
 * map: (no input) 
 * arguments:
 *   mymap: user defined map function
 *   ptr:   user defined pointer
 * return:
 *   global KV count
 */
uint64_t MapReduce::map(void (*mymap)(MapReduce *, void *), void *ptr){

  // create data object
  if(data) delete data;
  KeyValue *kv = new KeyValue(kvtype, 
                              blocksize, 
                              nmaxblock, 
                              maxmemsize, 
                              outofcore, 
                              tmpfpath);
  data=kv;
  kv->setKVsize(ksize,vsize);

  // create communicator
  c = new Alltoall(comm, tnum);
  c->setup(lbufsize, gbufsize, kvtype, ksize, vsize);
  c->init(kv);

  mode = MapMode;

  // begin traversal
#pragma omp parallel
{
  int tid = omp_get_thread_num();
  
  tinit(tid);
  
  // FIXME: should I invoke user-defined map in each thread?
  if(tid == 0)
    mymap(this, ptr);
  
  // wait all threads quit
  c->twait(tid);
}
  // wait all processes done
  c->wait();

  mode = NoneMode;

  // destroy communicator
  delete c;
  c = NULL;

  // sum kv count
  uint64_t sum = 0, count = 0; 
  for(int i = 0; i < tnum; i++) count += nitems[i];

  MPI_Allreduce(&count, &sum, 1, MPI_UINT64_T, MPI_SUM, comm);

  return sum;
}

/*
 * map_local: (no input) 
 * arguments:
 *   mymap: user defined map function
 *   ptr:   user defined pointer
 * return:
 *   local KV count
 */
uint64_t MapReduce::map_local(void (*mymap)(MapReduce *, void *), void* ptr){
  // create data object
  if(data) delete data;
  KeyValue *kv = new KeyValue(kvtype, 
                              blocksize, 
                              nmaxblock, 
                              maxmemsize, 
                              outofcore, 
                              tmpfpath);
  data=kv;
  kv->setKVsize(ksize, vsize);

  mode = MapLocalMode;


#pragma omp parallel
{
  int tid = omp_get_thread_num();
  tinit(tid);
  // FIXME: should invoke mymap in each thread?
  if(tid == 0)
    mymap(this, ptr);
}

  mode = NoneMode;

  uint64_t count = 0; 
  for(int i = 0; i < tnum; i++) count += nitems[i];

  return count;
}


/*
 * map: (files as input, main thread reads files)
 * arguments:
 *  filepath:   input file path
 *  sharedflag: 0 for local file system, 1 for global file system
 *  recurse:    1 for recurse
 *  readmode:   0 for by word, 1 for by line
 *  mymap:      user defined map function
 *  ptr:        user-defined pointer
 * return:
 *  global KV count
 */
uint64_t MapReduce::map(char *filepath, int sharedflag, int recurse, 
    int readmode, void (*mymap) (MapReduce *, char *, void *), void *ptr){

  LOG_ERROR("%s", "map (files as input, main thread reads files) has been implemented!\n");
  
  // TODO: Finish it!!!!
  // distribute input files
  disinputfiles(filepath, sharedflag, recurse);

  int fcount = ifiles.size();
  for(int i = 0; i < fcount; i++){
    std::cout << me << ":" << ifiles[i] << std::endl;
  }

  mode = NoneMode;
 
  return 0; 
}

/*
 * map_local: (files as input, main thread reads files)
 */
// TODO: finish it!!!
uint64_t MapReduce::map_local(char *filepath, int sharedflag, int recurse,
    int readmode, void (*mymap)(MapReduce *, char *, void *), void *ptr){
  return 0;
}

/*
 * map: (files as input, user-defined map reads files)
 * argument:
 *  filepath:   input file path
 *  sharedflag: 0 for local file system, 1 for global file system
 *  recurse:    1 for resucse
 *  mymap:      user-defined map
 *  ptr:        user-defined pointer
 * return:
 *  global KV count
 */
uint64_t MapReduce::map(char *filepath, int sharedflag, int recurse, 
  void (*mymap) (MapReduce *, const char *, void *), void *ptr){
  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map start. (File name to mymap)\n", me, nprocs);

  // distribute input fil list
  disinputfiles(filepath, sharedflag, recurse);

  // create new data object
  if(data) delete data;
  KeyValue *kv = new KeyValue(kvtype, 
                              blocksize, 
                              nmaxblock, 
                              maxmemsize, 
                              outofcore, 
                              tmpfpath);
  data = kv;
  kv->setKVsize(ksize, vsize);


  // create communicator
  c = new Alltoall(comm, tnum);
  c->setup(lbufsize, gbufsize, kvtype, ksize, vsize);
  c->init(data);

  mode = MapMode;

#pragma omp parallel
{
  int tid = omp_get_thread_num();
  tinit(tid);
  
  int fcount = ifiles.size();
#pragma omp for nowait
  for(int i = 0; i < fcount; i++){
    mymap(this, ifiles[i].c_str(), ptr);
  }
  c->twait(tid);
}
  c->wait();

  // delete communicator
  delete c;
  c = NULL; 

  mode = NoneMode;

  // sum KV count
  uint64_t sum = 0, count = 0; 
  for(int i = 0; i < tnum; i++) count += nitems[i];

  MPI_Allreduce(&count, &sum, 1, MPI_UINT64_T, MPI_SUM, comm);

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map end (output count: local=%ld, global=%ld).\n", me, nprocs, count, sum);

  return sum;
}

/*
 * map_local: (files as input, user-defined map reads files)
 * argument:
 *  filepath:   input file path
 *  sharedflag: 0 for local file system, 1 for global file system
 *  recurse:    1 for resucse
 *  mymap:      user-defined map
 *  ptr:        user-defined pointer
 * return:
 *  local KV count
 */
uint64_t MapReduce::map_local(char *filepath, int sharedflag, int recurse, 
  void (*mymap) (MapReduce *, const char *, void *), void *ptr){

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map local start. (File name to mymap)\n", me, nprocs);

  // distribute input file list
  disinputfiles(filepath, sharedflag, recurse);

  // create data object
  if(data) delete data;
  KeyValue *kv = new KeyValue(kvtype, 
                              blocksize, 
                              nmaxblock, 
                              maxmemsize, 
                              outofcore, 
                              tmpfpath);
  data = kv;
  kv->setKVsize(ksize, vsize);

  mode = MapLocalMode;

#pragma omp parallel
{
  int tid = omp_get_thread_num();
  tinit(tid);

  int fcount = ifiles.size();
#pragma omp for
  for(int i = 0; i < fcount; i++){
    //std::cout << ifiles[i] << std::endl;
    mymap(this, ifiles[i].c_str(), ptr);
  }
}

  mode = NoneMode;

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map local end.\n", me, nprocs);

  // sum KV count
  uint64_t count = 0; 
  for(int i = 0; i < tnum; i++) count += nitems[i];

  return count;
}

/*
 * map: (KV object as input)
 * argument:
 *  mr:     input mapreduce object
 *  mymap:  user-defined map
 *  ptr:    user-defined pointer
 * return:
 *  global KV count
 */
uint64_t MapReduce::map(MapReduce *mr, 
    void (*mymap)(MapReduce *, char *, int, char *, int, void*), void *ptr){

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map start. (KV as input)\n", me, nprocs);

  // save original data object
  DataObject *data = mr->data;
  if(!data || data->getDatatype() != KVType){
    LOG_ERROR("%s","Error in map_local: input object of map must be KV object\n");
    return -1;
  }

  // create new data object
  KeyValue *kv = new KeyValue(kvtype, 
                              blocksize, 
                              nmaxblock, 
                              maxmemsize, 
                              outofcore, 
                              tmpfpath);
  kv->setKVsize(ksize, vsize);
  this->data = kv;


  // create communicator
  c = new Alltoall(comm, tnum);
  c->setup(lbufsize, gbufsize, kvtype, ksize, vsize);
  c->init(kv);

  mode = MapMode;

  //printf("begin parallel\n");

  KeyValue *inputkv = (KeyValue*)data;
#pragma omp parallel
{
  int tid = omp_get_thread_num();
  tinit(tid);  

  char *key, *value;
  int keybytes, valuebytes;

  int i;
#pragma omp for nowait
  for(i = 0; i < inputkv->nblock; i++){
    int offset = 0;

    inputkv->acquireblock(i);

    offset = inputkv->getNextKV(i, offset, &key, keybytes, &value, valuebytes);
  
    while(offset != -1){

      mymap(this, key, keybytes, value, valuebytes, ptr);

      offset = inputkv->getNextKV(i, offset, &key, keybytes, &value, valuebytes);
    }
   
    inputkv->releaseblock(i);
  }

  c->twait(tid);
}
  //printf("end parallel\n");

  c->wait();


  //printf("begin delete\n");
  // delete data object
  delete inputkv;
  //printf("end delete\n");

  // delete communicator
  delete c;
  c = NULL;
 
  mode= NoneMode;

  // sum KV count
  uint64_t sum = 0, count = 0; 
  for(int i = 0; i < tnum; i++) count += nitems[i];

  MPI_Allreduce(&count, &sum, 1, MPI_UINT64_T, MPI_SUM, comm);

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map end (output count: local=%ld, global=%ld).\n", me, nprocs, count, sum); 

  return sum;
}

/*
 * map_local: (KV object as input)
 * argument:
 *  mr:     input mapreduce object
 *  mymap:  user-defined map
 *  ptr:    user-defined pointer
 * return:
 *  local KV count
 */
uint64_t MapReduce::map_local(MapReduce *mr, 
    void (*mymap)(MapReduce *, char *, int, char *, int, void *), void *ptr){

  LOG_PRINT(DBG_GEN, "%s", "MapReduce: map local start. (KV as input)\n");

  // save original data object
  DataObject *data = mr->data;
  if(!data || data->getDatatype() != KVType){
    LOG_ERROR("%s","Error in map_local: input object of map must be KV object\n");
    return -1;
  }

  // create new data object
  KeyValue *kv = new KeyValue(kvtype, 
                              blocksize, 
                              nmaxblock, 
                              maxmemsize, 
                              outofcore, 
                              tmpfpath);
  this->data = kv;
  kv->setKVsize(ksize, vsize);

  mode = MapLocalMode;

  KeyValue *inputkv = (KeyValue*)data;

#pragma omp parallel
{
  int tid = omp_get_thread_num();
  tinit(tid);
  
  char *key, *value;
  int keybytes, valuebytes;

#pragma omp for nowait
  for(int i = 0; i < inputkv->nblock; i++){
    int offset = 0;

    inputkv->acquireblock(i);

    offset = inputkv->getNextKV(i, offset, &key, keybytes, &value, valuebytes);
  
    while(offset != -1){
      mymap(this, key, keybytes, value, valuebytes, ptr);

      offset = inputkv->getNextKV(i, offset, &key, keybytes, &value, valuebytes);
    }
    
    inputkv->releaseblock(i);
  }
}

  delete inputkv;

  mode = NoneMode;

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map local end.\n", me, nprocs);

  uint64_t count = 0; 
  for(int i = 0; i < tnum; i++) count += nitems[i];

  return count;
}

/*
 * convert: convert KMV to KV
 *   return: local kmvs count
 */
// FIXME: should convert return global count?
uint64_t MapReduce::convert(){
  // check input data
  if(!data || data->getDatatype() != KVType){
    LOG_ERROR("%s", "Error: input of convert must be KV data!\n");
    return -1;
  }

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: convert start.\n", me, nprocs);

  // create KMV object
  KeyMultiValue *kmv = new KeyMultiValue(0, 
                             blocksize, 
                             nmaxblock, 
                             maxmemsize,
                             outofcore, 
                             tmpfpath);

  KeyValue *kv = (KeyValue*)data;

#pragma omp parallel
{

  int tid = omp_get_thread_num();
  tinit(tid);

  std::vector<std::list<Unique>> ht;

  char *key, *value;
  int keybytes, valuebytes, ret;
  int keyoff, valoff;

  int nbucket = pow(2,20);
  ht.reserve(nbucket);
  for(int i = 0; i < nbucket; i++) ht.emplace_back();

  // FIXME: should thread private object use the same configure as others?
  // create thread private data object
  DataObject *tmpdata = new DataObject(ByteType,
                                       blocksize, 
                                       nmaxblock, 
                                       maxmemsize, 
                                       outofcore, 
                                       tmpfpath); 

  // scan all kvs to gain the thread kvs
  for(int i = 0; i < data->nblock; i++){
    int offset = 0;

    kv->acquireblock(i);

    offset = kv->getNextKV(i, offset, &key, keybytes, &value, valuebytes, &keyoff, &valoff);

    while(offset != -1){
      uint32_t hid = hashlittle(key, keybytes, 0);

      // FIXME: is there any more efficient way to process the kvs?
      if(hid % (uint32_t)tnum == (uint32_t)tid){ 

        int ibucket = hid % nbucket;
        std::list<Unique>& ul = ht[ibucket];
        std::list<Unique>::iterator u;
     
        // search to see if the key has been in the list
        for(u = ul.begin(); u != ul.end(); u++){
          // key hit in the list
          if(memcmp(u->key, key, keybytes) == 0){
            u->cur_pos.push_back(Pos(valoff,valuebytes));
            u->cur_size += valuebytes;
            u->cur_nval++;
            break;
          }
        }
        // add the key to the list
        if(u==ul.end()){          
          ul.emplace_back();
          
          ul.back().key = new char[keybytes];
          memcpy(ul.back().key, key, keybytes);
          ul.back().keybytes = keybytes;
          
          ul.back().cur_pos.push_back(Pos(valoff,valuebytes));
          ul.back().cur_size=valuebytes;
          ul.back().cur_nval=1;

          ul.back().nval=0;
          ul.back().size=0;
        }
      }
      
      offset = kv->getNextKV(i, offset, &key, keybytes, &value, valuebytes, &keyoff, &valoff);
    } // send scan kvs

    // merge the results in a single block
    int blockid = blocks[tid];
    if(blockid == -1){
      blockid = tmpdata->addblock();
    }
    tmpdata->acquireblock(blockid);

    std::vector<std::list<Unique>>::iterator ul;
    for(ul = ht.begin(); ul != ht.end(); ++ul){
      std::list<Unique>::iterator u;
      for(u = ul->begin(); u != ul->end(); ++u){

       if(u->cur_nval == 0) continue;      
 
       // merge all values together
       int bytes = sizeof(int)*(u->cur_nval) + (u->cur_size);

       //printf("local merge [%d] bytes=%d, cur_nval=%d, blockid=%d, tail=%d, remain=%d\n", tid, bytes, u->cur_nval, blockid, tmpdata->getblocktail(blockid), tmpdata->getblockspace(blockid));

       // FIXME: how to handle this situtation?
       if(bytes > tmpdata->getblocksize()){
         LOG_ERROR("Error: KVM size (%d) is larger than block size (%d)\n", bytes, tmpdata->getblocksize());
       }

       // if the block is full, add another block 
       if(tmpdata->getblockspace(blockid) < bytes){
          //printf("haha\n");
          tmpdata->releaseblock(blockid);
          blockid = tmpdata->addblock();
          //if(tmpdata->getblockspace(blockid) < bytes){
          //}
          tmpdata->acquireblock(blockid);
        }
        // add sizes
        std::list<Pos>::iterator l;
        int off = tmpdata->getblocktail(blockid);
        for(l = u->cur_pos.begin(); l != u->cur_pos.end(); ++l){
          tmpdata->addbytes(blockid, (char*)&(l->size), (int)sizeof(int));
        }

        // add values
        for(l = u->cur_pos.begin(); l != u-> cur_pos.end(); ++l){
          char *p = NULL;
          kv->getbytes(i, l->off, &p);
          tmpdata->addbytes(blockid, p, l->size); 
        }

        u->nval += u->cur_nval;
        u->size += u->cur_size;
        u->pos.push_back(Pos(off,u->cur_size,blockid,u->cur_nval));

        u->cur_nval = 0;
        u->cur_size = 0;
        u->cur_pos.clear();
      }
    }
    tmpdata->releaseblock(blockid);
    blocks[tid] = blockid;

    kv->releaseblock(i);
  }

  // merge kvs into kmv
  int blockid = -1;
  std::vector<std::list<Unique>>::iterator ul;
  for(ul = ht.begin(); ul != ht.end(); ++ul){
    std::list<Unique>::iterator u;
    for(u = ul->begin(); u != ul->end(); ++u){

      if(blockid == -1){
        blockid = kmv->addblock();
        kmv->acquireblock(blockid);
      }

      int bytes = sizeof(int)+(u->keybytes)+(u->nval+1)*sizeof(int)+(u->size);

      //printf("global merge [%d] bytes=%d, blockid=%d, remain=%d\n", tid,  bytes, blockid, kmv->getblockspace(blockid));

       if(bytes > kmv->getblocksize()){
         LOG_ERROR("Error: KVM size (%d) is larger than block size (%d)\n", bytes, kmv->getblocksize());
       }


      if(kmv->getblockspace(blockid) < bytes){
        kmv->releaseblock(blockid);
        blockid = kmv->addblock();
        kmv->acquireblock(blockid);
      }

      kmv->addbytes(blockid, (char*)&(u->keybytes), (int)sizeof(int));
      kmv->addbytes(blockid, u->key, u->keybytes);
      kmv->addbytes(blockid, (char*)&(u->nval), (int)sizeof(int));

      std::list<Pos>::iterator l;
      for(l = u->pos.begin(); l != u->pos.end(); ++l){
        char *p = NULL;
        tmpdata->getbytes(l->bid, l->off, &p);
        kmv->addbytes(blockid, p, sizeof(int)*(l->nval));
      }
      for(l = u->pos.begin(); l != u->pos.end(); ++l){
        char *p = NULL;
        tmpdata->getbytes(l->bid, l->off+sizeof(int)*(l->nval), &p);
        kmv->addbytes(blockid, p, l->size);
      }

      nitems[tid]++;
 
      delete [] u->key;
    }
  }
  if(blockid != -1) kmv->releaseblock(blockid);

  delete tmpdata;
} 

  // set new data
  delete data;
  data = kmv;

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: convert end.\n", me, nprocs);

  uint64_t count = 0; 
  for(int i = 0; i < tnum; i++) count += nitems[i];

  return count;
}

/*
 * reduce:
 * argument:
 *  myreduce: user-defined reduce function
 *  ptr:      user-defined pointer
 * return:
 *  local KV count
 */
uint64_t MapReduce::reduce(void (myreduce)(MapReduce *, char *, int, int, char *, 
    int *, void*), void* ptr){
  if(!data || data->getDatatype() != KMVType){
    LOG_ERROR("%s", "Error: the input of reduce function must be KMV object!\n");
    return -1;
  }

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: reduce start.\n", me, nprocs);

  KeyMultiValue *kmv = (KeyMultiValue*)data;

  // create new data object
  KeyValue *kv = new KeyValue(kvtype, 
                      blocksize, 
                      nmaxblock, 
                      maxmemsize, 
                      outofcore, 
                      tmpfpath);
  kv->setKVsize(ksize, vsize);
  data = kv;

  mode = ReduceMode;

#pragma omp parallel
{
  int tid = omp_get_thread_num();
  tinit(tid);

  char *key, *values;
  int keybytes, nvalue, *valuebytes;

#pragma omp for nowait
  for(int i = 0; i < kmv->nblock; i++){
     int offset = 0;
     kmv->acquireblock(i);
     offset = kmv->getNextKMV(i, offset, &key, keybytes, nvalue, &values, &valuebytes);
     while(offset != -1){
       myreduce(this, key, keybytes, nvalue, values, valuebytes, ptr);        
       offset = kmv->getNextKMV(i, offset, &key, keybytes, nvalue, &values, &valuebytes);
     }
     kmv->releaseblock(i);
  }
}

  delete kmv;
 
  mode = NoneMode;

  // sum KV count
  uint64_t count = 0; 
  for(int i = 0; i < tnum; i++) count += nitems[i];

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: reduce end.(Output count: local=%ld)\n", me, nprocs, count);

  return count;
}

/*
 * scan: (KMV object as input)
 * argument:
 *  myscan: user-defined scan function
 *  ptr:    user-defined pointer
 * return:
 *  local KMV count
 */
// FIXME: should I provide multi-thread scan function?
uint64_t MapReduce::scan(void (myscan)(char *, int, int, char *, int *,void *), void * ptr){

  if(!data || data->getDatatype() != KMVType){
    LOG_ERROR("%s", "Error: the input of scan (KMV) must be KMV object!\n");
  }

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: scan start.\n", me, nprocs);  

  KeyMultiValue *kmv = (KeyMultiValue*)data;

  char *key, *values;
  int keybytes, nvalue, *valuebytes;

  int tid = omp_get_thread_num();
  tinit(tid);  

  for(int i = 0; i < kmv->nblock; i++){
    int offset = 0;
    
    kmv->acquireblock(i);
    
    offset = kmv->getNextKMV(i, offset, &key, keybytes, nvalue, &values, &valuebytes);

    while(offset != -1){
      myscan(key, keybytes, nvalue, values, valuebytes, ptr);

      nitems[tid]++;

      offset = kmv->getNextKMV(i, offset, &key, keybytes, nvalue, &values, &valuebytes);
    }

    kmv->releaseblock(i);
  }

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: scan end.\n", me, nprocs);

  // sum KV count
  uint64_t count = 0; 
  for(int i = 0; i < tnum; i++) count += nitems[i];

  return count;
}

/*
 * add a KV (invoked in user-defined map or reduce functions) 
 *  argument:
 *   key:        key 
 *   keybytes:   keysize
 *   value:      value
 *   valuebytes: valuesize
 */
void MapReduce::add(char *key, int keybytes, char *value, int valuebytes){
  if(!data || data->getDatatype() != KVType){
    LOG_ERROR("%s", "Error: add function only can be used to generate KV object!\n");
  }

  if(mode == NoneMode){
    LOG_ERROR("%s", "Error: add function only can be invoked in user-defined map and reduce functions\n");
  }

  int tid = omp_get_thread_num();
 
  // invoked in map function
  if(mode == MapMode){

    // get target process
    uint32_t hid = 0;
    int target = 0;
    if(myhash != NULL){
      target=myhash(key, keybytes);
      //if(target == 1) printf("key=%s\n", key);
    }
    else{
      hid = hashlittle(key, keybytes, nprocs);
      target = hid % (uint32_t)nprocs;
    }

    // send KV    
    c->sendKV(tid, target, key, keybytes, value, valuebytes);

    nitems[tid]++;
 
    return;
   }

  // invoked in map_local or reduce function
  if(mode == MapLocalMode || mode == ReduceMode){
    
    // add KV into data object 
    KeyValue *kv = (KeyValue*)data;

    if(blocks[tid] == -1){
      blocks[tid] = kv->addblock();
    }

    kv->acquireblock(blocks[tid]);

    while(kv->addKV(blocks[tid], key, keybytes, value, valuebytes) == -1){
      kv->releaseblock(blocks[tid]);
      blocks[tid] = kv->addblock();
      kv->acquireblock(blocks[tid]);
    }

    kv->releaseblock(blocks[tid]);
    nitems[tid]++;
  }

  return;
}

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
void MapReduce::init(){
  blocksize = BLOCK_SIZE;
  nmaxblock = MAX_BLOCKS;
  maxmemsize = MAXMEM_SIZE;
  lbufsize = LOCAL_BUF_SIZE;
  gbufsize = GLOBAL_BUF_SIZE;

  kvtype = KV_TYPE;

  outofcore = OUT_OF_CORE; 
  tmpfpath = std::string(TMP_PATH);
  
  myhash = NULL;
}

// thread init
void MapReduce::tinit(int tid){
  blocks[tid] = -1;
  nitems[tid] = 0;
}

// distribute input file list
void MapReduce::disinputfiles(const char *filepath, int sharedflag, int recurse){
  getinputfiles(filepath, sharedflag, recurse);

  if(sharedflag){
    int fcount = ifiles.size();
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
          send_count[i] += strlen(ifiles[j].c_str())+1;
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
          offset += strlen(ifiles[i].c_str())+1;
        }
    }

    MPI_Scatterv(send_buf, send_count, send_displs, MPI_BYTE, recv_buf, recv_count, MPI_BYTE, 0, comm);

    ifiles.clear();
    int off=0;
    while(off < recv_count){
      char *str = recv_buf+off;
      ifiles.push_back(std::string(str));
      off += strlen(str)+1;
    }

    delete [] send_count;
    delete [] send_displs;
    delete [] send_buf;
    delete [] recv_buf;
  }
}

// get input file list
void MapReduce::getinputfiles(const char *filepath, int sharedflag, int recurse){
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
        
        if(ep->d_name[0] == '.') continue;
       
        char newstr[MAXLINE]; 
        sprintf(newstr, "%s/%s", filepath, ep->d_name);
        err = stat(newstr, &inpath_stat);
        if(err) LOG_ERROR("Error in get input files, err=%d\n", err);
        
        // regular file
        if(S_ISREG(inpath_stat.st_mode)){
          ifiles.push_back(std::string(newstr));
        // dir
        }else if(S_ISDIR(inpath_stat.st_mode) && recurse){
          getinputfiles(newstr, sharedflag, recurse);
        }
      }
    }
  }  
}

