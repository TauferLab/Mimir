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
#include "alltoall.h"
#include "ptop.h"
#include "spool.h"

#include "log.h"
#include "config.h"

#if GATHER_STAT
#include "stat.h"
#endif

using namespace MAPREDUCE_NS;

extern Stat s;

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

void MapReduce::setCommMode(int _commmode){
  commmode = _commmode;
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
  if(commmode==0)
    c = new Alltoall(comm, tnum);
  else if(commmode==1)
    c = new Ptop(comm, tnum);
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
    char *whitespace, void (*mymap) (MapReduce *, char *, void *), void *ptr){

  //LOG_ERROR("%s", "map (files as input, main thread reads files) has been implemented!\n");
  // create data object
  if(data) delete data;
  ifiles.clear();

  KeyValue *kv = new KeyValue(kvtype, 
                              blocksize, 
                              nmaxblock, 
                              maxmemsize, 
                              outofcore, 
                              tmpfpath);
  data=kv;
  kv->setKVsize(ksize,vsize);

  // create communicator
  //c = new Alltoall(comm, tnum);
  if(commmode==0)
    c = new Alltoall(comm, tnum);
  else if(commmode==1)
    c = new Ptop(comm, tnum);

  c->setup(lbufsize, gbufsize, kvtype, ksize, vsize);
  c->init(kv);

  mode = MapMode;

  if(strlen(whitespace) == 0){
    LOG_ERROR("%s", "Error: the white space should not be empty string!\n");
  }
  
  // TODO: Finish it!!!!
  // distribute input files
  disinputfiles(filepath, sharedflag, recurse);

  struct stat stbuf;

  int input_buffer_size=BLOCK_SIZE*UNIT_SIZE;

  char *text = new char[input_buffer_size+1];

  int fcount = ifiles.size();
  for(int i = 0; i < fcount; i++){
    //std::cout << me << ":" << ifiles[i] << std::endl;
    int flag = stat(ifiles[i].c_str(), &stbuf);
    if( flag < 0){
      LOG_ERROR("Error: could not query file size of %s\n", ifiles[i].c_str());
    }

    FILE *fp = fopen(ifiles[i].c_str(), "r");
    int fsize = stbuf.st_size;
    int foff = 0, boff = 0;
    while(fsize > 0){
      // set file pointer
      fseek(fp, foff, SEEK_SET);
      // read a block
      int readsize = fread(text+boff, 1, input_buffer_size-boff, fp);
      text[boff+readsize+1] = '\0';

      // the last block
      //if(boff+readsize < input_buffer_size) 
      boff = 0;
      while(!strchr(whitespace, text[input_buffer_size-boff])) boff++;

#pragma omp parallel
{
      int tid = omp_get_thread_num();
      tinit(tid);

      int divisor = input_buffer_size / tnum;
      int remain  = input_buffer_size % tnum;

      int tstart = tid * divisor;
      if(tid < remain) tstart += tid;
      else tstart += remain;

      int tend = tstart + divisor;
      if(tid < remain) tend += 1;

      if(tid == tnum-1) tend -= boff;

      // start by the first none whitespace char
      int i;
      for(i = tstart; i < tend; i++){
        if(!strchr(whitespace, text[i])) break;
      }
      tstart = i;
      
      // end by the first whitespace char
      for(i = tend; i < input_buffer_size; i++){
        if(strchr(whitespace, text[i])) break;
      }
      tend = i;
      text[tend] = '\0';

      char *saveptr = NULL;
      char *word = strtok_r(text+tstart, whitespace, &saveptr);
      while(word){
        mymap(this, word, ptr);
        word = strtok_r(NULL,whitespace,&saveptr);
      }
}

      foff += readsize;
      fsize -= readsize;
      
      for(int i =0; i < boff; i++) text[i] = text[input_buffer_size-boff+i];
    }
    
    fclose(fp);
  }

  delete []  text;

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
 * map_local: (files as input, main thread reads files)
 */
// TODO: finish it!!!
uint64_t MapReduce::map_local(char *filepath, int sharedflag, int recurse,
    char *whitespace, void (*mymap)(MapReduce *, char *, void *), void *ptr){

  if(strlen(whitespace) == 0){
    LOG_ERROR("%s", "Error: the white space should not be empty string!\n");
  }

  // create data object
  if(data) delete data;
  ifiles.clear();

  KeyValue *kv = new KeyValue(kvtype, 
                              blocksize, 
                              nmaxblock, 
                              maxmemsize, 
                              outofcore, 
                              tmpfpath);
  data=kv;
  kv->setKVsize(ksize, vsize);
  
  // TODO: Finish it!!!!
  // distribute input files
  disinputfiles(filepath, sharedflag, recurse);

  struct stat stbuf;

  int input_buffer_size=BLOCK_SIZE*UNIT_SIZE;

  char *text = new char[input_buffer_size+1];

  mode = MapLocalMode;

  int fcount = ifiles.size();
  for(int i = 0; i < fcount; i++){
    //std::cout << me << ":" << ifiles[i] << std::endl;
    int flag = stat(ifiles[i].c_str(), &stbuf);
    if( flag < 0){
      LOG_ERROR("Error: could not query file size of %s\n", ifiles[i].c_str());
    }

    FILE *fp = fopen(ifiles[i].c_str(), "r");
    int fsize = stbuf.st_size;
    int foff = 0, boff = 0;
    while(fsize > 0){
      // set file pointer
      fseek(fp, foff, SEEK_SET);
      // read a block
      int readsize = fread(text+boff, 1, input_buffer_size-boff, fp);
      text[boff+readsize+1] = '\0';

      // the last block
      //if(boff+readsize < input_buffer_size) 
      boff = 0;
      while(!strchr(whitespace, text[input_buffer_size-boff])) boff++;

#pragma omp parallel
{
      int tid = omp_get_thread_num();
      tinit(tid);

      int divisor = input_buffer_size / tnum;
      int remain  = input_buffer_size % tnum;

      int tstart = tid * divisor;
      if(tid < remain) tstart += tid;
      else tstart += remain;

      int tend = tstart + divisor;
      if(tid < remain) tend += 1;

      if(tid == tnum-1) tend -= boff;

      // start by the first none whitespace char
      int i;
      for(i = tstart; i < tend; i++){
        if(!strchr(whitespace, text[i])) break;
      }
      tstart = i;
      
      // end by the first whitespace char
      for(i = tend; i < input_buffer_size; i++){
        if(strchr(whitespace, text[i])) break;
      }
      tend = i;
      text[tend] = '\0';

      char *saveptr = NULL;
      char *word = strtok_r(text+tstart, whitespace, &saveptr);
      while(word){
        mymap(this, word, ptr);
        word = strtok_r(NULL,whitespace,&saveptr);
      }
}

      foff += readsize;
      fsize -= readsize;
      
      for(int i =0; i < boff; i++) text[i] = text[input_buffer_size-boff+i];
    }
    
    fclose(fp);
  }

  delete []  text;

  mode = NoneMode;

  uint64_t count = 0; 
  for(int i = 0; i < tnum; i++) count += nitems[i];

  return count;
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
  // create new data object
  if(data) delete data;
  ifiles.clear();
  KeyValue *kv = new KeyValue(kvtype, 
                              blocksize, 
                              nmaxblock, 
                              maxmemsize, 
                              outofcore, 
                              tmpfpath);

  // distribute input fil list
  disinputfiles(filepath, sharedflag, recurse);

  data = kv;
  kv->setKVsize(ksize, vsize);


  // create communicator
  //c = new Alltoall(comm, tnum);
  if(commmode==0)
    c = new Alltoall(comm, tnum);
  else if(commmode==1)
    c = new Ptop(comm, tnum);
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

  // create data object
  if(data) delete data;
  ifiles.clear();

  KeyValue *kv = new KeyValue(kvtype, 
                              blocksize, 
                              nmaxblock, 
                              maxmemsize, 
                              outofcore, 
                              tmpfpath);
  data = kv;
  kv->setKVsize(ksize, vsize);

  // distribute input file list
  disinputfiles(filepath, sharedflag, recurse);

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
  //c = new Alltoall(comm, tnum);
  if(commmode==0)
    c = new Alltoall(comm, tnum);
  else if(commmode==1)
    c = new Ptop(comm, tnum);

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
  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: convert start.\n", me, nprocs);

  // check input data
  if(!data || data->getDatatype() != KVType){
    LOG_ERROR("%s", "Error: input of convert must be KV data!\n");
    return -1;
  }

  // create KMV object
  KeyMultiValue *kmv = new KeyMultiValue(kvtype, 
                             blocksize, 
                             nmaxblock, 
                             maxmemsize,
                             outofcore, 
                             tmpfpath);
  kmv->setKVsize(ksize, vsize);

  // set bucket size
  nbucket = pow(2, BUCKET_SIZE);

  // get output KMV type
  int kmvtype = kmv->getKMVtype();

  KeyValue *kv = (KeyValue*)data;

#pragma omp parallel
{

  // initialize thread
  int tid = omp_get_thread_num();
  tinit(tid);

  // store thread local KVs
  KeyValue *tkv = new KeyValue(kv->getKVtype(),
              blocksize,
              nmaxblock,
              maxmemsize,
              outofcore,
              tmpfpath);
  tkv->setKVsize(ksize, vsize);

  // store tmp KMV data
  DataObject *tmpdata = new KeyValue(ByteType, 
                    blocksize, 
                    nmaxblock, 
                    maxmemsize, 
                    outofcore, 
                    tmpfpath);
  
  // current buffer information
  Spool *pool = new Spool(UNIQUE_POOL_SIZE);
  char *ubuf=pool->addblock();
  int uoff=0;

  // initialie unique list
  Unique **ulist=(Unique**)ubuf;
  uoff += nbucket*sizeof(Unique*);

  if(uoff > UNIQUE_POOL_SIZE*UNIT_SIZE)
    LOG_ERROR("%s", "Error: unique block size is too small!\n");

  for(int i=0; i<nbucket; i++) ulist[i]=NULL;

  // initialize conuters
  uint64_t nunique=0, nvals=0, totalksize=0, totalvsize=0;

  // the block
  int firstblock=1, blockid=-1;

  // tmp variables
  char *key, *value;
  int keybytes, valuebytes, ret;

  // scan all kvs to gain the thread kvs
  for(int i = 0; i < data->nblock; i++){
    int offset = 0;

    kv->acquireblock(i);

    offset = kv->getNextKV(i, offset, &key, keybytes, &value, valuebytes);

    while(offset != -1){
      uint32_t hid = hashlittle(key, keybytes, 0);

      if(hid % (uint32_t)tnum == (uint32_t)tid){ 
        int ibucket = hid % nbucket;

        // find the key
        Unique *ukey, *pre;
        ret = findukey(ulist, ibucket, key, keybytes, &ukey, &pre); 

        // gather information
        if(!ret) {
          nunique++;
          nitems[tid]++;
        }
        nvals++;
        totalksize+=keybytes;
        totalvsize+=valuebytes;

        // determine if we need convert KVs to tmp KMVs
        int kmvsize=0;
        if(kmvtype==0) 
          kmvsize=(nunique+nvals)*sizeof(int)+totalvsize;
        else if(kmvtype==1)
          kmvsize=nunique*sizeof(int)+totalvsize;
        else if(kmvtype==2)
          kmvsize=nunique*sizeof(int)+totalvsize;
        else LOG_ERROR("Error: undefined KV type %d\n", kvtype);

        // convert unique list to tmp kmv
        if(kmvsize > blocksize*UNIT_SIZE){
          // convert tkv to tmp data
          unique2tmp(ulist, tkv, tmpdata, kmvtype);
          blockid=-1;

          // reset counters
          if(!ret) nunique=1;
          else nunique=0;
          nvals=1;
          totalksize=keybytes;
          totalvsize=valuebytes;

          // it's not the first block
          firstblock = 0;
        }

        // key hit
        if(ret){
          ukey->nvalue++;
          ukey->mvbytes += valuebytes;
          // add a new block
          if(ukey->blocks->bid != -1){
            // add a block
            checkbuffer(sizeof(Block), &ubuf, &uoff, pool);
            Block *block = (Block*)(ubuf+uoff);
            uoff += sizeof(Block);
            
            block->next = ukey->blocks;
            ukey->blocks=block;
              
            block->nvalue=0;
            block->mvbytes=0;
            block->soffset=NULL;
            block->voffset=NULL;
            block->bid=-1;

          }

          // add key information into block
          ukey->blocks->nvalue++;
          ukey->blocks->mvbytes += valuebytes;

        // add unique key
        }else{

          checkbuffer(sizeof(Unique)+keybytes, &ubuf, &uoff, pool);
          ukey = (Unique*)(ubuf + uoff);
          uoff += sizeof(Unique);

          // add to the list
          ukey->next = NULL;
          if(pre == NULL)
            ulist[ibucket] = ukey;
          else
            pre->next = ukey;

          // copy key
          ukey->key = ubuf+uoff;
          memcpy(ubuf+uoff, key, keybytes);
          uoff += keybytes;
 
          ukey->keybytes = keybytes;
          ukey->nvalue=1;
          ukey->mvbytes=valuebytes;
          ukey->soffset=NULL;
          ukey->voffset=NULL;
          ukey->blocks=NULL;
          // add one block
          checkbuffer(sizeof(Block), &ubuf, &uoff, pool);
          Block *block = (Block*)(ubuf+uoff);
          uoff += sizeof(Block);
          // add to the list
          block->next = ukey->blocks;
          ukey->blocks=block;
          
          block->nvalue=1;
          block->mvbytes=valuebytes;
          block->soffset=NULL;
          block->voffset=NULL;
          block->bid=-1;
        }

       // add KV into thread local KV object
        if(blockid == -1){    
          blockid = tkv->addblock();
        }
        tkv->acquireblock(blockid);
    
        while(tkv->addKV(blockid, key, keybytes, value, valuebytes) == -1){
          tkv->releaseblock(blockid);
          blockid = tkv->addblock();
          tkv->acquireblock(blockid);
        }

        tkv->releaseblock(blockid);
      }

      offset = kv->getNextKV(i, offset, &key, keybytes, &value, valuebytes);
    } // send scan kvs

    kv->releaseblock(i);
  }
 
  // Get KMV size
  int kmvsize =0;
  if(kmvtype==0) kmvsize=(nunique*2+nvals)*sizeof(int)+totalksize+totalvsize;
  else if(kmvtype==1) kmvsize=nunique*sizeof(int)+totalksize+totalvsize;
  else if(kmvtype==2) kmvsize=nunique*sizeof(int)+totalksize+totalvsize;

  // the first block and KMV can fit in a single block
  if(firstblock && kmvsize < blocksize*UNIT_SIZE){
    unique2kmv(ulist, tkv, kmv);
  // convert unique to tmp firstly, then merge
  }else{
    unique2tmp(ulist, tkv, tmpdata, kmvtype);
    mergeunique(ulist, tmpdata, kmv);
  }

  // delete memory
  delete pool;
  delete tmpdata;
  delete tkv;
} 

  // set new data
  delete data;
  data = kmv;

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: convert end.\n", me, nprocs);

  uint64_t count = 0, sum = 0; 
  for(int i = 0; i < tnum; i++) count += nitems[i];

  MPI_Allreduce(&count, &sum, 1, MPI_UINT64_T, MPI_SUM, comm);

  return sum;
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
       //printf("keybytes=%d, valuebytes=%d\n", keybytes, nvalue);
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

void MapReduce::print_stat(int verb, FILE *out){
#if GATHER_STAT
  st.print(verb, out);
#endif
}

void MapReduce::clear_stat(){
#if GATHER_STAT
  st.clear();
#endif
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

  commmode=0;
  
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

/*
 * Convert Unique to tmp data
 */
void MapReduce::unique2tmp(Unique **ulist, KeyValue *tkv, DataObject *tmpdata, int kmvtype){
  // add block
  int blockid = tmpdata->addblock();
  tmpdata->acquireblock(blockid);

  // get buffer
  char *outbuf = tmpdata->getblockbuffer(blockid);
  int off = 0;

  // search the Unique list
  for(int i=0; i < nbucket; i++){
    
    if(ulist[i] != NULL){
      Unique *ukey = ulist[i];
      do{
        Block *block = ukey->blocks;
        if(block->bid == -1){
          if(kmvtype==0){
            block->soffset=(int*)(outbuf+off);
            block->s_off = off;
            off += (block->nvalue)*sizeof(int);
            block->voffset=outbuf+off;
            block->v_off = off;
            off += block->mvbytes;
          }else if(kmvtype==1){
            block->soffset=NULL;
            block->s_off = -1;
            block->voffset=outbuf+off;
            block->v_off = off;
            off += block->mvbytes;
          }else if(kmvtype==2){
            block->soffset=NULL;
            block->s_off = -1;
            block->voffset=outbuf+off;
            block->v_off = off;
            off += block->mvbytes;
          }else LOG_ERROR("Error: undefined KV type %d!\n", kvtype);
          block->bid=blockid;
          block->nvalue=0;
          block->mvbytes=0;
        }
      }while(ukey=ukey->next);
    }
  }
  if(off > blocksize*UNIT_SIZE)
    LOG_ERROR("Error: offset %d is larger than block size %d\n", off, blocksize);
  tmpdata->setblockdatasize(blockid, off);

  // convert KV to tmp KMV
  char *key, *val;
  int keybytes, valbytes;
  for(int i = 0; i < tkv->nblock; i++){
    tkv->acquireblock(i);
    int offset = 0;
    offset = tkv->getNextKV(i, offset, &key, keybytes, &val, valbytes);
    while(offset != -1){
      uint32_t hid = hashlittle(key, keybytes, 0);
      int ibucket = hid % nbucket;
      Unique *ukey;
      if(findukey(ulist, ibucket, key, keybytes, &ukey)){
        Block *block = ukey->blocks;
        if(kmvtype==0){
          block->soffset[block->nvalue]=valbytes;
          block->nvalue++;
          memcpy(block->voffset+block->mvbytes, val, valbytes);
          block->mvbytes += valbytes;
        }else if(kmvtype==1){
          block->nvalue++;
          memcpy(block->voffset+block->mvbytes, val, valbytes);
          block->mvbytes += valbytes;
        }else if(kmvtype == 2){
          block->nvalue++;
          memcpy(block->voffset+block->mvbytes, val, valbytes);
          block->mvbytes += valbytes;
        }
      }else{
        LOG_ERROR("Error: cannot find key (size=%d)\n", keybytes);
      }
      offset = tkv->getNextKV(i, offset, &key, keybytes, &val, valbytes);
    }
    tkv->releaseblock(i);
  }

  // clear thread KV data
  tkv->clear();

  tmpdata->releaseblock(blockid);
}

/*
 * convert unique to KMV data object
 */
void MapReduce::unique2kmv(Unique **ulist, KeyValue *tkv, KeyMultiValue *kmv){

  int kmvtype=kmv->getKMVtype();  
  int blockid = kmv->addblock();

  char *outbuf = kmv->getblockbuffer(blockid);  
  int off = 0;

  kmv->acquireblock(blockid);

  // search unique list
  for(int i=0; i < nbucket; i++){
    if(ulist[i] != NULL){
      Unique *ukey = ulist[i];
      do{
         if(kmvtype==0){
          memcpy(outbuf+off, &(ukey->keybytes), sizeof(int));
          off += sizeof(int);
          memcpy(outbuf+off, ukey->key, ukey->keybytes);
          off += ukey->keybytes;
          memcpy(outbuf+off, &(ukey->nvalue), sizeof(int));
          off += sizeof(int);
          ukey->soffset=(int*)(outbuf+off);
          off += (ukey->nvalue)*sizeof(int);
          ukey->voffset=outbuf+off;
          off += ukey->mvbytes;
        }else if(kmvtype==1){
          memcpy(outbuf+off, ukey->key, ukey->keybytes);
          off += ukey->keybytes;
          memcpy(outbuf+off, &(ukey->nvalue), sizeof(int));
          off += sizeof(int);
          ukey->soffset=NULL;
          ukey->voffset=outbuf+off;
          off += ukey->mvbytes;
        }else if(kmvtype==2){
          memcpy(outbuf+off, ukey->key, ukey->keybytes);
          off += ukey->keybytes;
          memcpy(outbuf+off, &(ukey->nvalue), sizeof(int));
          off += sizeof(int);
          ukey->soffset=NULL;
          ukey->voffset=outbuf+off;
          off += ukey->mvbytes;
        }else LOG_ERROR("Error: undefined KV type %d!\n", kmvtype);
        ukey->nvalue=0;
        ukey->mvbytes=0;
      }while(ukey=ukey->next);
    }
  }

  if(off > blocksize*UNIT_SIZE)
    LOG_ERROR("Error: offset %d is larger than block size %d\n", off, blocksize);
  kmv->setblockdatasize(blockid, off);

  char *key, *val;
  int keybytes, valbytes;

  for(int i = 0; i < tkv->nblock; i++){

    tkv->acquireblock(i);

    int offset = 0;
    offset = tkv->getNextKV(i, offset, &key, keybytes, &val, valbytes);

    while(offset != -1){

      uint32_t hid = hashlittle(key, keybytes, 0);
      int ibucket = hid % nbucket;

      Unique *ukey;

      if(findukey(ulist, ibucket, key, keybytes, &ukey, NULL)){

        if(kmvtype==0){
          ukey->soffset[ukey->nvalue]=valbytes;
          ukey->nvalue++;
          memcpy(ukey->voffset+ukey->mvbytes, val, valbytes);
          ukey->mvbytes += valbytes;
        }else if(kmvtype==1){
          ukey->nvalue++;
          memcpy(ukey->voffset+ukey->mvbytes, val, valbytes);
          ukey->mvbytes += valbytes;
        }else if(kmvtype == 2){
          ukey->nvalue++;
          memcpy(ukey->voffset+ukey->mvbytes, val, valbytes);
          ukey->mvbytes += valbytes;
        }
      }else{
        LOG_ERROR("Error: cannot find key (size=%d)\n", keybytes);
      }
      offset = tkv->getNextKV(i, offset, &key, keybytes, &val, valbytes);
    }
    tkv->releaseblock(i);
  }

  kmv->releaseblock(blockid);
}

/*
 * merge unique
 */
void MapReduce::mergeunique(Unique **ulist, DataObject *tmpdata, KeyMultiValue *kmv){
  int kmvtype = kmv->getKMVtype();

  int blockid = kmv->addblock();
  kmv->acquireblock(blockid);
  
  char *outbuf = kmv->getblockbuffer(blockid);
  int off = 0;

  // search unique list
  for(int i = 0; i < nbucket; i++){

    Unique *ukey = ulist[i];
    if(ukey==NULL) continue;

    do{

      char *key = ukey->key;
      int keybytes = ukey->keybytes;
      int nvalue = ukey->nvalue;
      int mvbytes = ukey->mvbytes;

      int kmvsize=0;
      if(kmvtype==0) 
        kmvsize = sizeof(int)+keybytes+sizeof(int)*(nvalue+1)+mvbytes;
      else if(kmvtype==1)
        kmvsize = keybytes+sizeof(int)+mvbytes;
      else if(kmvtype==2) 
        kmvsize = keybytes+sizeof(int)+mvbytes;
      else LOG_ERROR("Error: undefined KV type %d\n", kmvtype);

      if(kmvsize > kmv->getblocksize()){
        LOG_ERROR("Error: one KMV size %d is larger than block size!\n", kmvsize);
      }

      // add another block
      if(kmvsize+off > kmv->getblocksize()){
        kmv->setblockdatasize(blockid, off);
        kmv->releaseblock(blockid);
        blockid = kmv->addblock();
        kmv->acquireblock(blockid);
        outbuf = kmv->getblockbuffer(blockid);
        off = 0;
      }

      // copy key data
      if(kmvtype==0){
        memcpy(outbuf+off, &keybytes, sizeof(int));
        off += sizeof(int);
        memcpy(outbuf+off, key, keybytes);
        off += keybytes;
        memcpy(outbuf+off, &nvalue, sizeof(int));
        off += sizeof(int);
      }else if(kmvtype==1){
        memcpy(outbuf+off, key, keybytes);
        off += keybytes;
        memcpy(outbuf+off, &nvalue, sizeof(int));
        off += sizeof(int);
      }else if(kmvtype==2){
        memcpy(outbuf+off, key, keybytes);
        off += keybytes;
        memcpy(outbuf+off, &nvalue, sizeof(int));
        off += sizeof(int);
      }

      // copy block information
      Block *block = ukey->blocks;
      do{
        tmpdata->acquireblock(block->bid);

        char *tmpbuf = tmpdata->getblockbuffer(block->bid);
        block->soffset = (int*)(tmpbuf + block->s_off);
        block->voffset = tmpbuf + block->v_off;

        if(kmvtype==0){
          memcpy(outbuf+off, block->soffset, (block->nvalue)*sizeof(int));
          off += (block->nvalue)*sizeof(int);
          memcpy(outbuf+off, block->voffset, block->mvbytes);
          off += block->mvbytes;
        }else if(kmvtype==1){
          memcpy(outbuf+off, block->voffset, block->mvbytes);
          off += block->mvbytes;
        }else if(kmvtype==2){
          memcpy(outbuf+off, block->voffset, block->mvbytes);
          off += block->mvbytes;
        }

        tmpdata->releaseblock(block->bid);
      }while(block = block->next);

    }while(ukey=ukey->next);// end while

  }// end for

  kmv->setblockdatasize(blockid, off);
  kmv->releaseblock(blockid);
}

// find the key in the unique list
int MapReduce::findukey(Unique **unique_list, int ibucket, char *key, int keybytes, Unique **unique, Unique **pre){
  Unique *list = unique_list[ibucket];
  
  if(list == NULL){
    *unique = NULL;
    if(pre) *pre = NULL;
    return 0;
  }

  int ret = 0;
  if(pre) *pre = NULL;
  do{
    if(list->keybytes == keybytes && 
      memcmp(list->key, key, keybytes) == 0){
      ret = 1;
      break;
    }
    if(pre) *pre = list;
  }while(list = list->next);

  *unique = list;
  return ret;
}

// check if we need add another block
void MapReduce::checkbuffer(int bytes, char **buf, int *off, Spool *pool){
  if(bytes > pool->getblocksize())
    LOG_ERROR("Error: memory requirement %d is larger than block size %d!\n", bytes, pool->getblocksize()); 

  if(*off+bytes > pool->getblocksize()){
    *buf = pool->addblock();
    *off = 0;
  }
}
