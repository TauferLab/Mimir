#include <stdio.h>
#include <stdlib.h>
#include <sched.h>
#include <unistd.h>
#include <sys/wait.h>

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

#include "const.h"

#include "hash.h"

using namespace MAPREDUCE_NS;

#if GATHER_STAT
#include "stat.h"
#endif

MapReduce::MapReduce(MPI_Comm caller)
{
    comm = caller;
    MPI_Comm_rank(comm,&me);
    MPI_Comm_size(comm,&nprocs);

    get_default_values();

    ualign = sizeof(uint64_t);

    //kalignm = kalign-1;
    //valignm = valign-1;
    //talignm = talign-1;
    ualignm = ualign-1;

    nbucket = pow(2, BUCKET_SIZE);
    nset = nbucket;
    ukeyoffset = sizeof(Unique);

    thashmask=tnum-1;
    uhashmask=nbucket-1;

    blocks = new int[tnum];   
    nitems = new uint64_t[tnum];
    for(int i = 0; i < tnum; i++){
      blocks[i] = -1;
      nitems[i] = 0;
    }

    data = NULL;
    c = NULL;

    mode = NoneMode;
  
    bind_threads();

    //fprintf(stdout, "Process count=%d, thread count=%d\n", nprocs, tnum);

    LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: create. (thread number=%d)\n", me, nprocs, tnum);
}

MapReduce::MapReduce(const MapReduce &mr){
  kvtype=mr.kvtype;
  ksize=mr.ksize;
  vsize=mr.vsize;
  blocksize=mr.blocksize;
  nmaxblock=mr.nmaxblock;
  maxmemsize=mr.maxmemsize;
  outofcore=mr.outofcore;
  commmode=mr.commmode;

  lbufsize=mr.lbufsize;
  gbufsize=mr.gbufsize;

  tmpfpath=mr.tmpfpath;
  myhash=mr.myhash;
  
  comm=mr.comm;
  me=mr.me;
  nprocs=mr.nprocs;
  tnum=mr.tnum;

  mode=mr.mode;

  blocks=new int[tnum];
  nitems=new uint64_t[tnum];

  for(int i=0; i<tnum; i++){
    blocks[i]=mr.blocks[i];
    nitems[i]=mr.nitems[i];
  }

  data=mr.data;
  DataObject::addRef(data);

  c=NULL;

  ifiles.clear();
}


MapReduce::~MapReduce()
{
    //if(data) delete data;
    DataObject::subRef(data);

    if(c) delete c;

    delete [] nitems;
    delete [] blocks;

    LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: destroy.\n", me, nprocs);
}

// configurable functions
/******************************************************/

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
  DataObject::subRef(data);
  //if(data) delete data;
  KeyValue *kv = new KeyValue(kvtype, 
                              blocksize, 
                              nmaxblock, 
                              maxmemsize, 
                              outofcore, 
                              tmpfpath);
  data=kv;
  DataObject::addRef(data);
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
  local_kvs_count=c->get_recv_KVs();

  mode = NoneMode;

  if(c->mem_bytes+data->mem_bytes>max_mem_bytes)
    max_mem_bytes=(c->mem_bytes+data->mem_bytes);

  // destroy communicator
  send_bytes+=c->send_bytes;
  recv_bytes+=c->recv_bytes;
  delete c;
  c = NULL;

  // sum kv count
  uint64_t count = 0; 
  for(int i = 0; i < tnum; i++) count += nitems[i];

  MPI_Allreduce(&count, &global_kvs_count, 1, MPI_UINT64_T, MPI_SUM, comm);

  return global_kvs_count;
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
  //if(data) delete data;
  
  DataObject::subRef(data);

  KeyValue *kv = new KeyValue(kvtype, 
                              blocksize, 
                              nmaxblock, 
                              maxmemsize, 
                              outofcore, 
                              tmpfpath);
  data=kv;
  DataObject::addRef(data);
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

  if(data->mem_bytes>max_mem_bytes)
    max_mem_bytes=data->mem_bytes;

  uint64_t count = 0; 
  for(int i = 0; i < tnum; i++) count += nitems[i];

  local_kvs_count=count;
  MPI_Allreduce(&count, &global_kvs_count, 1, MPI_UINT64_T, MPI_SUM, comm);

  return global_kvs_count;
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
  //if(data) delete data;
  DataObject::subRef(data);
  ifiles.clear();

  KeyValue *kv = new KeyValue(kvtype, 
                              blocksize, 
                              nmaxblock, 
                              maxmemsize, 
                              outofcore, 
                              tmpfpath);
  data=kv;
  DataObject::addRef(data);
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

  int input_buffer_size=blocksize*UNIT_SIZE;

  char *text = new char[input_buffer_size+1];

#pragma omp parallel
{
      int tid = omp_get_thread_num();
      tinit(tid);
}

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
      //tinit(tid);

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

#pragma omp parallel
{
  int tid = omp_get_thread_num();
  c->twait(tid) ;     
}
   c->wait();
   local_kvs_count=c->get_recv_KVs();

   mode = NoneMode;

  if(c->mem_bytes+data->mem_bytes+input_buffer_size>max_mem_bytes)
    max_mem_bytes=(c->mem_bytes+data->mem_bytes+input_buffer_size);

  // destroy communicator
  send_bytes+=c->send_bytes;
  recv_bytes+=c->recv_bytes;
  delete c;
  c = NULL;

  // sum kv count
  uint64_t count = 0; 
  for(int i = 0; i < tnum; i++) count += nitems[i];

  MPI_Allreduce(&count, &global_kvs_count, 1, MPI_UINT64_T, MPI_SUM, comm);

  return global_kvs_count;
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
  //if(data) delete data;
  //
  DataObject::subRef(data);

  ifiles.clear();

  KeyValue *kv = new KeyValue(kvtype, 
                              blocksize, 
                              nmaxblock, 
                              maxmemsize, 
                              outofcore, 
                              tmpfpath);
  data=kv;
  DataObject::addRef(data);
  kv->setKVsize(ksize, vsize);
  
  // TODO: Finish it!!!!
  // distribute input files
  disinputfiles(filepath, sharedflag, recurse);

  struct stat stbuf;

  int input_buffer_size=blocksize*UNIT_SIZE;

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

      //printf("input_buffer_size=%d, boff=%d, read size=%d\n", input_buffer_size, boff, readsize);

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

      //printf("tstart=%d\n", tstart);
      char *saveptr = NULL;
      char *word = strtok_r(text+tstart, whitespace, &saveptr);
      while(word){
        //printf("word=%s\n", word);
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

  if(data->mem_bytes+input_buffer_size>max_mem_bytes)
    max_mem_bytes=(data->mem_bytes+input_buffer_size);


  uint64_t count = 0; 
  for(int i = 0; i < tnum; i++) count += nitems[i];

  local_kvs_count=count;
  MPI_Allreduce(&count, &global_kvs_count, 1, MPI_UINT64_T, MPI_SUM, comm);

  return global_kvs_count;
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
  //if(data) delete data;
  DataObject::subRef(data);

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
  DataObject::addRef(data);

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

#if GATHER_STAT
  double t1 = MPI_Wtime();
#endif

  int fp=0;
  int fcount = ifiles.size();

#pragma omp parallel shared(fp) shared(fcount)
{
  int tid = omp_get_thread_num();
  tinit(tid);

#if GATHER_STAT
  double t1 = omp_get_wtime();
#endif

  int i;
  while((i=__sync_fetch_and_add(&fp,1))<fcount){
#if GATHER_STAT
    st.inc_counter(tid, COUNTER_FILE_COUNT, 1);
#endif  
    mymap(this,ifiles[i].c_str(), ptr);
  }

#if GATHER_STAT
  double t2 = omp_get_wtime();
  st.inc_timer(tid, TIMER_MAP_USER, t2-t1);
#endif

  c->twait(tid);

#if GATHER_STAT
  double t3 = omp_get_wtime();
  st.inc_timer(tid, TIMER_MAP_TWAIT, t3-t2);
#endif
}
  
#if GATHER_STAT
  double t2= MPI_Wtime();
  st.inc_timer(0, TIMER_MAP_PARALLEL, t2-t1);
#endif

  c->wait();
  local_kvs_count=c->get_recv_KVs();

#if GATHER_STAT
  double t3= MPI_Wtime();
  st.inc_timer(0, TIMER_MAP_WAIT, t3-t2);
#endif

  if(c->mem_bytes+data->mem_bytes>max_mem_bytes)
    max_mem_bytes=(c->mem_bytes+data->mem_bytes);

  // delete communicator
  send_bytes+=c->send_bytes;
  recv_bytes+=c->recv_bytes;
  delete c;
  c = NULL; 

  mode = NoneMode;

  // sum KV count
  //uint64_t count = 0; 
  local_kvs_count=0;
  for(int i = 0; i < tnum; i++) local_kvs_count += nitems[i];

  MPI_Allreduce(&local_kvs_count, &global_kvs_count, 1, MPI_UINT64_T, MPI_SUM, comm);

  //LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map end (output count: local=%ld, global=%ld).\n", me, nprocs, count, global_kvs_count);

  return global_kvs_count;
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
  //if(data) delete data;

  DataObject::subRef(data);

  ifiles.clear();

  KeyValue *kv = new KeyValue(kvtype, 
                              blocksize, 
                              nmaxblock, 
                              maxmemsize, 
                              outofcore, 
                              tmpfpath);
  data = kv;

  DataObject::addRef(data);

  kv->setKVsize(ksize, vsize);

  // distribute input file list
  disinputfiles(filepath, sharedflag, recurse);

  mode = MapLocalMode;

#if GATHER_STAT
  double t1 = MPI_Wtime();
#endif

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

#if GATHER_STAT
  double t2= MPI_Wtime();
  st.inc_timer(TIMER_MAP_PARALLEL, t2-t1);
#endif

  mode = NoneMode;

  if(data->mem_bytes>max_mem_bytes)
    max_mem_bytes=(data->mem_bytes);

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map local end.\n", me, nprocs);

  // sum KV count
  uint64_t count = 0; 
  for(int i = 0; i < tnum; i++) count += nitems[i];

  local_kvs_count=count;
  MPI_Allreduce(&count, &global_kvs_count, 1, MPI_UINT64_T, MPI_SUM, comm);

  return global_kvs_count;

//  return count;
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

  DataObject::addRef(mr->data);
  DataObject::subRef(data);

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
  DataObject::addRef(this->data);

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

#if GATHER_STAT
  double t1 = MPI_Wtime();
#endif

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

#if GATHER_STAT
  double t1= MPI_Wtime();
#endif

  c->twait(tid);

#if GATHER_STAT
  double t2= MPI_Wtime();
  if(tid==0) st.inc_timer(TIMER_MAP_TWAIT, t2-t1);
#endif

}

#if GATHER_STAT
  double t2= MPI_Wtime();
  st.inc_timer(TIMER_MAP_PARALLEL, t2-t1);
#endif

  //printf("end parallel\n");

  c->wait();
  local_kvs_count=c->get_recv_KVs();

#if  GATHER_STAT
  double t3 = MPI_Wtime();
  st.inc_timer(TIMER_MAP_WAIT, t3-t2);
#endif

  //printf("begin delete\n");
  // delete data object
  //delete inputkv;
  //printf("end delete\n");
  if(c->mem_bytes+data->mem_bytes+this->data->mem_bytes>max_mem_bytes)
    max_mem_bytes=(c->mem_bytes+data->mem_bytes+this->data->mem_bytes);


  DataObject::subRef(data);

  // delete communicator
  send_bytes+=c->send_bytes;
  recv_bytes+=c->recv_bytes;
  delete c;
  c = NULL;
 
  mode= NoneMode;

  // sum KV count
  uint64_t count = 0; 
  for(int i = 0; i < tnum; i++) count += nitems[i];

  MPI_Allreduce(&count, &global_kvs_count, 1, MPI_UINT64_T, MPI_SUM, comm);

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map end (output count: local=%ld, global=%ld).\n", me, nprocs, count, global_kvs_count); 

  return global_kvs_count;
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

  DataObject::addRef(mr->data);
  DataObject::subRef(data);

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

  DataObject::addRef(this->data);

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

  if(data->mem_bytes+this->data->mem_bytes>max_mem_bytes)
    max_mem_bytes=(data->mem_bytes+this->data->mem_bytes);

  //delete inputkv;
  DataObject::subRef(data);

  mode = NoneMode;

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map local end.\n", me, nprocs);

  uint64_t count = 0; 
  for(int i = 0; i < tnum; i++) count += nitems[i];

  local_kvs_count=count;
  MPI_Allreduce(&count, &global_kvs_count, 1, MPI_UINT64_T, MPI_SUM, comm);

  return global_kvs_count;

  //return count;
}

/*
 * convert: convert KMV to KV
 *   return: local kmvs count
 */
uint64_t MapReduce::reduce(void (*myreduce)(MapReduce *, char *, int, MultiValueIterator *iter, void*), int compress, void* ptr){
//uint64_t MapReduce::convert(){

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: convert start.\n", me, nprocs);

#if SAFE_CHECK
  if(!data || data->getDatatype() != KVType){
    LOG_ERROR("%s", "Error: input of convert must be KV data!\n");
    return -1;
  }
#endif

#if GATHER_STAT
  st.inc_counter(0, COUNTER_BLOCK_BYTES, (data->nblock)*(data->blocksize));
#endif
  
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

  mode = ReduceMode;

  if(!compress){
#if GATHER_STAT
    double t1 = MPI_Wtime();
#endif
    local_kvs_count = _convert_small(kv, myreduce, ptr, 0);
#if GATHER_STAT
    double t2 = MPI_Wtime();
    st.inc_timer(0, TIMER_REDUCE_CVT, t2-t1);
#endif
    DataObject::subRef(kv);
  }else{
    _convert_small(kv, myreduce, ptr, 1);
    DataObject::subRef(kv);
    KeyValue *tmpkv=outkv;
    outkv = new KeyValue(kvtype, 
                  blocksize, 
                  nmaxblock, 
                  maxmemsize, 
                  outofcore, 
                  tmpfpath);
    outkv->setKVsize(ksize, vsize);
    data=outkv;
    local_kvs_count = _convert_small(tmpkv, myreduce, ptr, 0);
  }

  //delete kv;
  //data = kmv;

#if GATHER_STAT
  st.inc_counter(0, COUNTER_RESULT_BYTES, (data->nblock)*(data->blocksize));
#endif

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: convert end.\n", me, nprocs);

  DataObject::addRef(data);

  mode = NoneMode;

  return _get_kv_count(); 
}

// find the key in the unique list
MapReduce::Unique* MapReduce::_findukey(Unique **unique_list, int ibucket, char *key, int keybytes, Unique *&uprev){
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

  return NULL;
}

void MapReduce::_unique2set(UniqueInfo *u){

  Set *set=(Set*)u->set_pool->addblock();

  int nunique=0;

  //printf("nunique=%d, nblock=%d\n", u->nunique, u->unique_pool->nblock); fflush(stdout);

  Spool *unique_pool=u->unique_pool;
  for(int i=0; i<unique_pool->nblock; i++){
    char *ubuf=unique_pool->blocks[i];
    char *ubuf_end=ubuf+unique_pool->blocksize;
    while(ubuf < ubuf_end){
      Unique *ukey = (Unique*)(ubuf);

      if((ubuf_end-ubuf < sizeof(Unique)) || 
        (ukey->key==NULL))
        break;

      nunique++;
      if(nunique>u->nunique) goto end;

      //printf("key=%s\n", ukey->key); fflush(stdout);

      Set *pset=&set[u->nset%nset];
      pset->myid=u->nset++;
      pset->nvalue=ukey->nvalue;
      pset->mvbytes=ukey->mvbytes;
      pset->pid=0;
      pset->next=NULL;

      ukey->firstset=pset;
      ukey->lastset=pset;
      
      if(u->nset%nset==0)
        set = (Set*)u->set_pool->addblock();

      ubuf += ukeyoffset;
      ubuf += ukey->keybytes;
      //ubuf = ROUNDUP(ubuf, ualignm);

    }// end while
  }

end:
  return;
}

int  MapReduce::_kv2unique(int tid, KeyValue *kv, UniqueInfo *u, DataObject *mv, 
  void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*), void *ptr,
  int shared, int compress){

  //DEFINE_KV_VARS;
  char *key, *value;
  int keybytes, valuebytes, kvsize;
  char *kvbuf;
  
  int isfirst=1, pid=0;
  int last_blockid=0, last_offset=0, last_set=0;

  int kmvbytes=0, mvbytes=0;

  char *ubuf=u->unique_pool->addblock();
  char *ubuf_end=ubuf+u->unique_pool->blocksize;

  //printf("ubuf_end-ubuf: %ld\n", ubuf_end-ubuf);
  //printf("") 

  Set *sets=NULL, *pset = NULL;

  //printf("nblock=%d\n", kv->nblock);

  // scan all KVs
  for(int i=0; i<kv->nblock; i++){

    //printf("begin acquire block=%d\n", i);
    kv->acquireblock(i);
    //printf("end acquire\n");

    kvbuf=kv->getblockbuffer(i);
    char *kvbuf_end=kvbuf+kv->getblocktail(i);
    int kvbuf_off=0;

    while(kvbuf<kvbuf_end){
      GET_KV_VARS(kv->kvtype, kvbuf,key,keybytes,value,valuebytes,kvsize,kv);

      //printf("key=%s, keybytes=%d, valuebytes=%d, kvsize=%d\n", key, keybytes, valuebytes, kvsize); fflush(stdout);

      uint32_t hid = hashlittle(key, keybytes, 0);
      if(shared && (uint32_t)hid%tnum != (uint32_t)tid) {
        kvbuf_off += kvsize;
        continue;
      }

      // Find the key
      int ibucket = hid % nbucket;
      //if(ibucket<0)  LOG_ERROR("%s", "ibucket is less than 0!\n");

      Unique *ukey, *pre;
      ukey = _findukey(u->ubucket, ibucket, key, keybytes, pre);

      int key_hit=1;
      if(!ukey) key_hit=0;

      int mv_inc=valuebytes;
      if(kv->kvtype==GeneralKV)  mv_inc+=oneintlen;

      // The First Block
      if(isfirst && !compress){
        //printf("kmvbytes=%d, mv_inc=%d\n", kmvbytes, mv_inc);
        kmvbytes+=mv_inc;
        if(!key_hit) kmvbytes+=(keybytes+3*sizeof(int));

        //printf("kmvbytes=%d, mv->blocksize=%d\n", kmvbytes, mv->blocksize); fflush(stdout) ;     
 
        // We need the intermediate convert
        if(kmvbytes>mv->blocksize){
          //printf("nunique=%d\n", u->nunique); fflush(stdout);
          _unique2set(u);
          //printf("nset=%d\n", u->nset); fflush(stdout);
          sets=(Set*)u->set_pool->blocks[u->nset/nset];
          isfirst=0;
        }
      }

      if(compress) mv_inc+=(keybytes+3*sizeof(int));
      
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

        if(!compress){
#if GATHER_STAT
          double t1 = omp_get_wtime();
#endif
          _unique2mv(tid, kv, &p, u, mv);

          //printf("mv nblock=%d\n", mv->nblock);

#if GATHER_STAT
          double t2 = omp_get_wtime();
          st.inc_timer(tid, TIMER_REDUCE_LCVT, t2-t1);
#endif
        }
        else{
          _unique2kmv(tid, kv, &p, u, mv, myreduce, ptr, shared);
          mv->clear();
          u->nunique=0;
          u->nset=0;
          u->unique_pool->clear();
          u->set_pool->clear();
        }


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

         //printf("%ld, %p->%p, ukeyoffset=%d, keybytes=%d\n", ubuf_end-ubuf, ubuf, ubuf_end, ukeyoffset, keybytes); fflush(stdout);

        //printf("ubuf_end-ubuf: %ld\n", ubuf_end-ubuf);

        if(ubuf_end-ubuf<ukeyoffset+keybytes){
          //printf("add a new unique buffer! ubuf_end-ubuf=%ld\n", ubuf_end-ubuf); fflush(stdout);
          memset(ubuf, 0, ubuf_end-ubuf);
          ubuf=u->unique_pool->addblock();
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

      if(!isfirst && !compress) {
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
            sets=(Set*)u->set_pool->addblock();
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

    //printf("i=%d, nset=%d, mv nblock=%d\n", i, u->nset, mv->nblock);

    kv->releaseblock(i);
  }// end For

  if(!isfirst && kv->nblock>0){
    Partition p;
    p.start_blockid=last_blockid;
    p.start_offset=last_offset;
    p.end_blockid=kv->nblock-1;
    p.end_offset=kv->getblocktail(kv->nblock-1);
    p.start_set=last_set;
    p.end_set=u->nset;

    //printf("last partition! mv block=%d\n", mv->nblock); fflush(stdout);

    if(!compress)
      _unique2mv(tid, kv, &p, u, mv);
    else{
      _unique2kmv(tid, kv, &p, u, mv, myreduce, ptr, shared);
      mv->clear();
      u->nunique=0;
      u->nset=0;
      u->unique_pool->clear();
      u->set_pool->clear();
    }
  }

  return isfirst;
}

void MapReduce::_unique2kmv(int tid, KeyValue *kv, UniqueInfo *u,DataObject *mv,  
  void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*), void *ptr, int shared){
  //printf("unique2kmv\n"); fflush(stdout);
  
  //DEFINE_KV_VARS; 
  char *key, *value;
  int keybytes, valuebytes, kvsize;
 
  char *kvbuf;

  int mv_block_id=mv->addblock();
  mv->acquireblock(mv_block_id);
  char *mv_buf=mv->getblockbuffer(mv_block_id);
  int mv_off=0;

  int nunique=0;

  //printf("nunique=%d\n", u->nunique);

  // Set the offset
  Spool *unique_pool=u->unique_pool;
  for(int i=0; i<unique_pool->nblock; i++){
    char *ubuf=unique_pool->blocks[i];
    char *ubuf_end=ubuf+unique_pool->blocksize;
    while(ubuf < ubuf_end){

      Unique *ukey = (Unique*)(ubuf);
      if((ubuf_end-ubuf < sizeof(Unique)) || (ukey->key==NULL))
        break;

      nunique++;
      if(nunique>u->nunique) goto end;

      //printf("%s\n", ukey->key); fflush(stdout);

      *(int*)(mv_buf+mv_off)=ukey->keybytes;
      mv_off+=sizeof(int);
      *(int*)(mv_buf+mv_off)=ukey->mvbytes;
      mv_off+=sizeof(int);
      *(int*)(mv_buf+mv_off)=ukey->nvalue;
      mv_off+=sizeof(int);
     
      if(kv->kvtype==GeneralKV){
        ukey->soffset=(int*)(mv_buf+mv_off);
        mv_off+=ukey->nvalue*sizeof(int);
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

  //printf("kmv offset=%d\n", kmv_off);
#if SAFE_CHECK
  if(mv_off > (blocksize*UNIT_SIZE)){
    LOG_ERROR("KMV size %d is larger than a single block size %d!\n", mv_off, blocksize);
  }
#endif

  //printf("mv_off=%d\n", mv_off);

  mv->setblockdatasize(mv_block_id, mv_off);

  // gain KVS
  for(int i=0; i<kv->nblock; i++){
    kv->acquireblock(i);
    char *kvbuf=kv->getblockbuffer(i);
    int datasize=kv->getblocktail(i);
    char *kvbuf_end=kvbuf+datasize;
    while(kvbuf<kvbuf_end){
      GET_KV_VARS(kv->kvtype,kvbuf,key,keybytes,value,valuebytes,kvsize,kv);

     
      uint32_t hid = hashlittle(key, keybytes, 0);
      if(shared && (uint32_t)hid % tnum != (uint32_t)tid) continue;

      //printf("tid=%d, key=%s, value=%s\n", tid, key, value);

      // Find the key
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
    kv->releaseblock(i);
  }

  //printf("here!\n"); fflush(stdout);

  char *values;
  int nvalue, mvbytes, kmvsize, *valuesizes;

  int datasize=mv->getblocktail(mv_block_id);
  int offset=0;

  //printf("offset=%d, datasize=%d\n", offset, datasize);

  while(offset < datasize){

    //printf("offset=%d, datasize=%d\n", offset, datasize); fflush(stdout);
    
    GET_KMV_VARS(kv->kvtype, mv_buf, key, keybytes, nvalue, values, valuesizes, mvbytes, kmvsize, kv);

    //printf("key=%s, nvalue=%d\n", key, nvalue); fflush(stdout);
#if GATHER_STAT
    double t1 = omp_get_wtime();
#endif   
 
    MultiValueIterator *iter = new MultiValueIterator(nvalue,valuesizes,values,kv->kvtype,kv->vsize);
    myreduce(this, key, keybytes, iter, ptr);
    delete iter;

#if GATHER_STAT
    double t2 = omp_get_wtime();
    st.inc_timer(tid, TIMER_REDUCE_USER, t2-t1);
#endif

    offset += kmvsize;
  }

  mv->releaseblock(mv_block_id);
}

void MapReduce::_unique2mv(int tid, KeyValue *kv, Partition *p, UniqueInfo *u, DataObject *mv, int shared){
  char *key, *value;
  int keybytes, valuebytes, kvsize;
  char *kvbuf;

  //DEFINE_KV_VARS;

  //printf("unique2mv: addblock, [%d,%d]->[%d,%d]  mv=%d\n", p->start_blockid, p->start_offset, p->end_blockid, p->end_offset, mv->nblock);

  int mv_blockid=mv->addblock();
  //printf("mvblockid=%d\n", mv_blockid);
  mv->acquireblock(mv_blockid);

  char *mvbuf = mv->getblockbuffer(mv_blockid);
  int mvbuf_off=0;

  for(int i=p->start_set; i<p->end_set; i++){
    Set *pset=(Set*)u->set_pool->blocks[i/nset]+i%nset;
    
    if(kv->kvtype==GeneralKV){
      pset->soffset=(int*)(mvbuf+mvbuf_off);
      pset->s_off=mvbuf_off;
      mvbuf_off += pset->nvalue*sizeof(int);
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
    kv->acquireblock(i);
    char *kvbuf=kv->getblockbuffer(i);
    char *kvbuf_end=kvbuf;
    if(i<p->end_blockid)
      kvbuf_end+=kv->getblocktail(i);
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
        pset->nvalue++;
      }
      memcpy(pset->voffset+pset->mvbytes, value, valuebytes);
      pset->mvbytes+=valuebytes;

    }// end while(kvbuf<kvbuf_end)

    kv->releaseblock(i);
  }

  mv->releaseblock(mv_blockid);
}

void MapReduce::_unique2kmv(int tid, KeyValue *kv, Partition *p, UniqueInfo *u, DataObject *mv, 
  void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*), void* ptr, int shared){
  
  char *key, *value;
  int keybytes, valuebytes, kvsize;
  char *kvbuf;

  int mv_block_id=mv->addblock();
  mv->acquireblock(mv_block_id);
  char *mv_buf=mv->getblockbuffer(mv_block_id);
  int mv_off=0;

  int nunique=0;

  //printf("nunique=%d\n", u->nunique);

  // Set the offset
  Spool *unique_pool=u->unique_pool;
  for(int i=0; i<unique_pool->nblock; i++){
    char *ubuf=unique_pool->blocks[i];
    char *ubuf_end=ubuf+unique_pool->blocksize;
    while(ubuf < ubuf_end){

      Unique *ukey = (Unique*)(ubuf);
      if((ubuf_end-ubuf < sizeof(Unique)) || (ukey->key==NULL))
        break;

      nunique++;
      if(nunique>u->nunique) goto end;

      //printf("%s\n", ukey->key); fflush(stdout);

      *(int*)(mv_buf+mv_off)=ukey->keybytes;
      mv_off+=sizeof(int);
      *(int*)(mv_buf+mv_off)=ukey->mvbytes;
      mv_off+=sizeof(int);
      *(int*)(mv_buf+mv_off)=ukey->nvalue;
      mv_off+=sizeof(int);
     
      if(kv->kvtype==GeneralKV){
        ukey->soffset=(int*)(mv_buf+mv_off);
        mv_off+=ukey->nvalue*sizeof(int);
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

  //printf("kmv offset=%d\n", kmv_off);
#if SAFE_CHECK
  if(mv_off > (blocksize*UNIT_SIZE)){
    LOG_ERROR("KMV size %d is larger than a single block size %d!\n", mv_off, blocksize);
  }
#endif

  //printf("mv_off=%d\n", mv_off);

  mv->setblockdatasize(mv_block_id, mv_off);

  for(int i=p->start_blockid; i<=p->end_blockid; i++){
    kv->acquireblock(i);
    char *kvbuf=kv->getblockbuffer(i);
    char *kvbuf_end=kvbuf;
    if(i<p->end_blockid)
      kvbuf_end+=kv->getblocktail(i);
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
        pset->nvalue++;
      }
      memcpy(pset->voffset+pset->mvbytes, value, valuebytes);
      pset->mvbytes+=valuebytes;

    }// end while(kvbuf<kvbuf_end)

    kv->releaseblock(i);
  }

  char *values;
  int nvalue, mvbytes, kmvsize, *valuesizes;

  int datasize=mv->getblocktail(mv_block_id);
  int offset=0;

  //printf("offset=%d, datasize=%d\n", offset, datasize);

  while(offset < datasize){

    //printf("offset=%d, datasize=%d\n", offset, datasize); fflush(stdout);
    
    GET_KMV_VARS(kv->kvtype, mv_buf, key, keybytes, nvalue, values, valuesizes, mvbytes, kmvsize, kv);

    //printf("key=%s, nvalue=%d\n", key, nvalue); fflush(stdout);

#if GATHER_STAT
    double t1 = omp_get_wtime();
#endif    

    MultiValueIterator *iter = new MultiValueIterator(nvalue,valuesizes,values,kv->kvtype,kv->vsize);
    myreduce(this, key, keybytes, iter, ptr);
    delete iter;

#if GATHER_STAT
    double t2 = omp_get_wtime();
    st.inc_timer(tid, TIMER_REDUCE_USER, t2-t1);
#endif

    offset += kmvsize;
  }

  mv->releaseblock(mv_block_id);
  mv->clear(); 
}

void MapReduce::_mv2kmv(DataObject *mv,UniqueInfo *u,
  void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*), void* ptr){

  int nunique=0;
  char *ubuf, *kmvbuf=NULL;
  int uoff=0, kmvoff=0;
  char *ubuf_end;
  
  //int kmv_blockid = kmv->addblock();
  //kmv->acquireblock(kmv_blockid);
  
  //kmvbuf = kmv->getblockbuffer(kmv_blockid);
  //kmvoff = 0;

  for(int i=0; i < u->unique_pool->nblock; i++){
    ubuf = u->unique_pool->blocks[i];
    ubuf_end=ubuf+u->unique_pool->blocksize;

    while(ubuf<ubuf_end){
      Unique *ukey = (Unique*)ubuf;    

      if(ubuf_end-ubuf<ukeyoffset || ukey->key==NULL)
        break;

      nunique++;
      if(nunique > u->nunique) goto end;

#if GATHER_STAT
     double t1 = omp_get_wtime();
#endif

      MultiValueIterator *iter = new MultiValueIterator(ukey, mv);     
      myreduce(this, ukey->key, ukey->keybytes, iter, ptr);
      delete iter;

#if GATHER_STAT
      double t2 = omp_get_wtime();
      st.inc_timer(omp_get_thread_num(), TIMER_REDUCE_USER, t2-t1);
#endif

#if SAFE_CHECK
      //if(kmvsize > kmv->getblocksize()){
      //  LOG_ERROR("Error: KMV size (key=%s, nvalue=%d) %d is larger than block size %d\n", ukey->key, ukey->nvalue, kmvsize, kmv->getblocksize());
      //}
#endif

      //if(kmvoff+kmvsize>kmv->blocksize){
      //  kmv->setblockdatasize(kmv_blockid, kmvoff);
      //  kmv->releaseblock(kmv_blockid);
      //  kmv_blockid=kmv->addblock();
      //  kmv->acquireblock(kmv_blockid);
      //  kmvbuf=kmv->getblockbuffer(kmv_blockid);
      //  kmvoff=0;
      //}
 
#if 0
      *(int*)(kmvbuf+kmvoff)=ukey->keybytes;
      kmvoff+=sizeof(int);
      *(int*)(kmvbuf+kmvoff)=ukey->mvbytes;
      kmvoff+=sizeof(int);
      *(int*)(kmvbuf+kmvoff)=ukey->nvalue;
      kmvoff+=sizeof(int);

      if(kmv->kmvtype==GeneralKV){
        ukey->soffset=(int*)(kmvbuf+kmvoff);
        kmvoff+=(ukey->nvalue)*sizeof(int);
      }

      memcpy(kmvbuf+kmvoff, ukey->key, ukey->keybytes);
      kmvoff+=ukey->keybytes;

      ukey->voffset=kmvbuf+kmvoff;
      kmvoff+=(ukey->mvbytes);

      ukey->nvalue=0;
      ukey->mvbytes=0;

      // copy block information
      Set *pset = ukey->firstset;
      while(pset){
        mv->acquireblock(pset->pid);

        char *tmpbuf = mv->getblockbuffer(pset->pid);

        pset->soffset = (int*)(tmpbuf + pset->s_off);
        pset->voffset = tmpbuf + pset->v_off;

        if(kmv->kmvtype==GeneralKV){
          memcpy(ukey->soffset+ukey->nvalue, pset->soffset, (pset->nvalue)*sizeof(int));
          ukey->nvalue+=pset->nvalue;
        }
 
        memcpy(ukey->voffset+ukey->mvbytes, pset->voffset, pset->mvbytes);
        ukey->mvbytes += pset->mvbytes;

        mv->releaseblock(pset->pid);

        pset=pset->next;
      };
#endif

      ubuf += ukeyoffset;
      ubuf += ukey->keybytes;
      //ubuf = ROUNDUP(ubuf, ualignm);
    }
  }// End for

end:
  ;
  //kmv->setblockdatasize(kmv_blockid, kmvoff);
  //kmv->releaseblock(kmv_blockid);
}

uint64_t MapReduce::_convert_small(KeyValue *kv, 
  void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*), void* ptr, int compress){

  LOG_PRINT(DBG_CVT, "%d[%d] Convert(small) start.\n", me, nprocs);

  uint64_t tmax_mem_bytes=0;
#pragma omp parallel reduction(+:tmax_mem_bytes) 
{
  int tid = omp_get_thread_num();
  tinit(tid);
 
  // initialize the unique info
  UniqueInfo *u=new UniqueInfo();
  u->ubucket = new Unique*[nbucket];
  u->unique_pool=new Spool(nbucket*sizeof(Unique));
  u->set_pool=new Spool(nset*sizeof(Set));
  u->nunique=0;
  u->nset=0;

  memset(u->ubucket, 0, nbucket*sizeof(Unique*));

  //printf("nmaxblock=%d\n", nmaxblock);

  DataObject *mv = NULL;
  mv = new DataObject(ByteType, 
             blocksize, 
             nmaxblock, 
             maxmemsize, 
             outofcore, 
             tmpfpath.c_str(),
             0);

#if GATHER_STAT
  double t1 = omp_get_wtime();
#endif

  int isfirst = _kv2unique(tid, kv, u, mv, myreduce, ptr, 1, compress);

#if GATHER_STAT
  double t2 = omp_get_wtime();
  st.inc_timer(tid, TIMER_REDUCE_KV2U, t2-t1);
  st.inc_counter(tid, COUNTER_BUCKET_BYTES, nbucket*sizeof(Unique*));
  st.inc_counter(tid, COUNTER_UNIQUE_BYTES, (u->unique_pool->nblock)*(u->unique_pool->blocksize));
  st.inc_counter(tid, COUNTER_SET_BYTES, (u->set_pool->nblock)*(u->set_pool->blocksize));
  st.inc_counter(tid, COUNTER_MV_BYTES, (mv->nblock)*(mv->blocksize));
#endif

  LOG_PRINT(DBG_CVT, "%d KV2Unique end:first=%d\n", tid, isfirst);

  if(isfirst){
    _unique2kmv(tid, kv, u, mv, myreduce, ptr);
#if GATHER_STAT
    double t3 = omp_get_wtime();
    st.inc_timer(tid, TIMER_REDUCE_U2KMV, t3-t2);
#endif
  }else if(!compress){
    _mv2kmv(mv, u, myreduce, ptr);
#if GATHER_STAT
    double t3 = omp_get_wtime();
    st.inc_timer(tid, TIMER_REDUCE_MERGE, t3-t2);
#endif
  }

  tmax_mem_bytes=mv->mem_bytes+u->unique_pool->mem_bytes+u->set_pool->mem_bytes;

  delete mv;

  //printf("T%d: %ld\n", tid, u->nunique);

  nitems[tid] = u->nunique;

  delete [] u->ubucket;
  delete u->unique_pool;
  delete u->set_pool;
  delete u;
}

  if(kv->mem_bytes+data->mem_bytes+tmax_mem_bytes>max_mem_bytes)
    max_mem_bytes=(kv->mem_bytes+data->mem_bytes+tmax_mem_bytes);

  //delete kv;
  DataObject::subRef(kv);

  uint64_t nunique=0;
  for(int i=0; i<tnum; i++)
    nunique += nitems[i];

  //printf("nunique=%d, tnum=%d\n", nunique, tnum);

  return nunique;

  LOG_PRINT(DBG_CVT, "%d[%d] Convert(small) end.\n", me, nprocs);
}


#if 0
/*
 * reduce:
 * argument:
 *  myreduce: user-defined reduce function
 *  ptr:      user-defined pointer
 * return:
 *  local KV count
 */
uint64_t MapReduce::reduce(void (myreduce)(MapReduce *, char *, int, MultiValueIterator *iter, void*), void* ptr){
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

  DataObject::addRef(data);

  mode = ReduceMode;

#pragma omp parallel
{
  int tid = omp_get_thread_num();
  tinit(tid);

#pragma omp for nowait
  for(int i = 0; i < kmv->nblock; i++){

     char *key, *values;
     int keybytes, nvalue, mvbytes, kmvsize, *valuebytes;
     int offset = 0;

     kmv->acquireblock(i);
     char *kmvbuf=kmv->getblockbuffer(i);
     int datasize=kmv->getblocktail(i);
     
     //offset = kmv->getNextKMV(i, offset, &key, keybytes, nvalue, &values, &valuebytes);
     while(offset < datasize){
       //printf("keybytes=%d, valuebytes=%d\n", keybytes, nvalue);

       GET_KMV_VARS(kmv->kmvtype, kmvbuf, key, keybytes, nvalue, values, valuebytes, mvbytes, kmvsize, data);


       MultiValueIterator *iter = new MultiValueIterator(nvalue,valuebytes,values,kmv->kmvtype,kmv->ksize,kmv->vsize);
       
       myreduce(this, key, keybytes, iter, ptr);

       delete iter;

       //offset = kmv->getNextKMV(i, offset, &key, keybytes, nvalue, &values, &valuebytes);
       offset += kmvsize;
     }
     kmv->releaseblock(i);
  }
}

  if(kv->mem_bytes+kmv->mem_bytes>max_mem_bytes)
    max_mem_bytes=(kv->mem_bytes+kmv->mem_bytes);

  //delete kmv;
  //
  DataObject::subRef(kmv);
 
  mode = NoneMode;

  // sum KV count
  uint64_t count = 0; 
  for(int i = 0; i < tnum; i++) count += nitems[i];

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: reduce end.(Output count: local=%ld)\n", me, nprocs, count);

  return count;
}
#endif

/*
 * scan: (KMV object as input)
 * argument:
 *  myscan: user-defined scan function
 *  ptr:    user-defined pointer
 * return:
 *  local KMV count
 */
// FIXME: should I provide multi-thread scan function?
void MapReduce::scan(void (myscan)(char *, int, char *, int ,void *), void * ptr){

  if(!data || data->getDatatype() != KVType){
    LOG_ERROR("%s", "Error: the input of scan (KMV) must be KMV object!\n");
  }

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: scan start.\n", me, nprocs);  

  KeyValue *kv = (KeyValue*)data;

#pragma omp parallel for
  for(int i = 0; i < kv->nblock; i++){

     char *key, *value;
     int keybytes, valuebytes, kvsize;

     kv->acquireblock(i);
     char *kvbuf=kv->getblockbuffer(i);
     int datasize=kv->getblocktail(i);
     
     //offset = kmv->getNextKMV(i, offset, &key, keybytes, nvalue, &values, &valuebytes);
     int offset=0;
     while(offset < datasize){
       //printf("keybytes=%d, valuebytes=%d\n", keybytes, nvalue);

       GET_KV_VARS(kv->kvtype,kvbuf,key,keybytes,value,valuebytes,kvsize,kv);

       myscan(key, keybytes, value, valuebytes, ptr);
       
       offset += kvsize;
     }
     kv->releaseblock(i);
  }

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: scan end.\n", me, nprocs);
}



  //int tid = omp_get_thread_num();
  //tinit(tid);  

  //for(int i = 0; i < kmv->nblock; i++){
  //  int offset = 0;
    
  //  kmv->acquireblock(i);
    
 //   offset = kmv->getNextKMV(i, offset, &key, keybytes, nvalue, &values, &valuebytes);

 //   while(offset != -1){
 //     myscan(key, keybytes, nvalue, values, valuebytes, ptr);

 //     nitems[tid]++;

 //     offset = kmv->getNextKMV(i, offset, &key, keybytes, nvalue, &values, &valuebytes);
 //   }

 //   kmv->releaseblock(i);
 // }


  // sum KV count
  //uint64_t count = 0; 
  //for(int i = 0; i < tnum; i++) count += nitems[i];

  //return count;
//}

/*
 * add a KV (invoked in user-defined map or reduce functions) 
 *  argument:
 *   key:        key 
 *   keybytes:   keysize
 *   value:      value
 *   valuebytes: valuesize
 */
void MapReduce::add(char *key, int keybytes, char *value, int valuebytes){

  //printf("add: key=%s, value=%s\n", key, value); fflush(stdout);

#if SAFE_CHECK
  if(!data || data->getDatatype() != KVType){
    LOG_ERROR("%s", "Error: add function only can be used to generate KV object!\n");
  }

  if(mode == NoneMode){
    LOG_ERROR("%s", "Error: add function only can be invoked in user-defined map and reduce functions\n");
  }
#endif

  int tid = omp_get_thread_num();
 
  // invoked in map function
  if(mode == MapMode){

#if GATHER_STAT
    double t1 = omp_get_wtime();
#endif

    // get target process
    int target = 0;
    if(myhash != NULL){
      target=myhash(key, keybytes);
    }else{
      uint32_t hid = 0;
      hid = hashlittle(key, keybytes, nprocs);
      target = hid % (uint32_t)nprocs;
    }

    // send KV    
    c->sendKV(tid, target, key, keybytes, value, valuebytes);

#if GATHER_STAT
   double t2 = omp_get_wtime();
   st.inc_timer(tid, TIMER_MAP_SENDKV, t2-t1);
   st.inc_counter(tid, COUNTER_KV_NUMS, 1);
#endif

    nitems[tid]++;
 
    return;
   }else if(mode == MapLocalMode || mode == ReduceMode){

#if GATHER_STAT
    double t1 = omp_get_wtime();
#endif

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

#if GATHER_STAT
    double t2 = omp_get_wtime();
    //if(tid==0) st.inc_timer(TIMER_MAP_ADD, t2-t1);
#endif

  }

  return;
}

//#if 0
void MapReduce::show_stat(int verb, FILE *out){
//  double tpar=st.timers[TIMER_MAP_PARALLEL];
//  double tsendkv=st.timers[TIMER_MAP_SENDKV];
//  double tserial=st.timers[TIMER_MAP_SERIAL];
//  double twait=st.timers[TIMER_MAP_TWAIT];
//  double tkv2u=st.timers[TIMER_REDUCE_KV2U];
//  double lcvt=st.timers[TIMER_REDUCE_LCVT];

//  fprintf(out, "%g,%g,%g,%g,%g,%g,%g,%g,%g,%g,\n", \
    tpar, st.timers[TIMER_MAP_WAIT], tpar-tsendkv-twait, tsendkv-tserial, tserial, twait,
//        st.timers[TIMER_MAP_LOCK],tkv2u-lcvt, lcvt, st.timers[TIMER_REDUCE_MERGE]);
#if GATHER_STAT  
  st.print(verb, out);
#endif
}

void MapReduce::init_stat(){
  //send_bytes = recv_bytes = 0;
#if GATHER_STAT
  st.clear();
#endif
}
//#endif

#if 0
double MapReduce::get_timer(int id){
#if GATHER_STAT
 return st.timers[id]; 
#else 
 return 0;
#endif
}
#endif

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
void MapReduce::get_default_values(){
  bind_thread=0;
  procs_per_node=0;
  thrs_per_proc=0;  
  show_binding=0;

  char *env = getenv(ENV_BIND_THREADS);
  if(env){
    bind_thread=atoi(env);
    if(bind_thread == 1){
      env = getenv(ENV_PROCS_PER_NODE);
      if(env){
        procs_per_node=atoi(env);
      }
      env = getenv(ENV_THRS_PER_PROC);
      if(env){
        thrs_per_proc=atoi(env);
      }
      if(procs_per_node <=0 || thrs_per_proc <=0 )
        bind_thread = 0;
    }else bind_thread = 0;
  }
  env = getenv(ENV_SHOW_BINGDING);
  if(env){
    show_binding = atoi(env);
    if(show_binding != 1) show_binding=0;
  }

  //printf("bind_thread=%d, show_binging=%d, procs_per_node=%d, thrs_per_proc=%d\n", bind_thread, show_binding, procs_per_node, thrs_per_proc); fflush(stdout);


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

  send_bytes=recv_bytes=0;
  max_mem_bytes=0;

#pragma omp parallel
{
  tnum = omp_get_num_threads();
}
}

void MapReduce::bind_threads(){

#pragma omp parallel
{
  int tid = omp_get_thread_num();

  cpu_set_t mask;

  if(bind_thread){
    CPU_ZERO(&mask);

    int lrank=me%procs_per_node;
    int coreid=lrank*thrs_per_proc+tid;

    CPU_SET(coreid, &mask);
    sched_setaffinity(0, sizeof(mask), &mask);
  }

  if(show_binding){
    printf("Process count=%d, thread count=%d\n", nprocs, tnum);   

    CPU_ZERO(&mask);

    sched_getaffinity(0, sizeof(mask), &mask);
    for(int i=0; i<PCS_PER_NODE*THS_PER_PROC; i++){
      if(CPU_ISSET(i, &mask)){
        printf("P%d[T%d] bind to cpu%d\n", me, tid, i);fflush(stdout);
      }
    }
  }
}
 
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

#if 0
void MapReduce::merge(DataObject *tmpdata, KeyMultiValue *kmv, Spool *unique_pool){
  int kmvtype=kmv->getKMVtype();
  int blocksize=unique_pool->getblocksize();

  char *inbuf=NULL, *outbuf=NULL;
  int inoff=0, outoff=0;
  
  int blockid = kmv->addblock();
  kmv->acquireblock(blockid);
  
  outbuf = kmv->getblockbuffer(blockid);
  outoff = 0;

  for(int i=0; i < unique_pool->nblock; i++){
    inbuf = unique_pool->getblockbuffer(i);
    while(1){
      Unique *ukey = (Unique*)(inbuf+inoff);
      if((blocksize-inoff < sizeof(Unique)) || (ukey->key==NULL))
        break;

      int kmvsize=ukey->keybytes+ukey->mvbytes+sizeof(int);
      if(kmvtype==0) kmvsize += (ukey->nvalue+1)*sizeof(int);

#if SAFE_CHECK
      if(kmvsize > kmv->getblocksize()){
        LOG_ERROR("Error: KMV size %d is larger than block size %d\n", kmvsize, kmv->getblocksize());
      }
#endif

      if(outoff+kmvsize>kmv->getblocksize()){

#if SAFE_CHECK
        if(outoff > kmv->getblocksize()){
          LOG_ERROR("Error: offset in KMV %d is larger than block size %d\n", outoff, kmv->getblocksize());
        }
#endif
        kmv->setblockdatasize(blockid, outoff);
        kmv->releaseblock(blockid);
        blockid=kmv->addblock();
        kmv->acquireblock(blockid);
        outbuf=kmv->getblockbuffer(blockid);
        outoff=0;
      }
     
      if(kmvtype==0){
        *(int*)(outbuf+outoff)=ukey->keybytes;
        outoff+=sizeof(int);
      }

      memcpy(outbuf+outoff, ukey->key, ukey->keybytes);
      outoff+=ukey->keybytes;
      *(int*)(outbuf+outoff)=ukey->nvalue;
      outoff+=sizeof(int);

      if(kmvtype==0){
        ukey->soffset=(int*)(outbuf+outoff);
        outoff+=(ukey->nvalue)*sizeof(int);
      }

      ukey->voffset=outbuf+outoff;
      outoff+=(ukey->mvbytes);

      ukey->nvalue=0;
      ukey->mvbytes=0;

      // copy block information
      Block *block = ukey->blocks;
      do{
        tmpdata->acquireblock(block->blockid);

        char *tmpbuf = tmpdata->getblockbuffer(block->blockid);
        block->soffset = (int*)(tmpbuf + block->s_off);
        block->voffset = tmpbuf + block->v_off;
 
        if(kmvtype==0){
          memcpy(ukey->soffset+ukey->nvalue, block->soffset, (block->nvalue)*sizeof(int));
          ukey->nvalue+=block->nvalue;
        }
 
        memcpy(ukey->voffset+ukey->mvbytes, block->voffset, block->mvbytes);
        ukey->mvbytes += block->mvbytes;

        tmpdata->releaseblock(block->blockid);
      }while(block = block->next);     

      inoff+=(ukey->keybytes+sizeof(Unique));

#if SAFE_CHECK
      if(inoff > blocksize)
        LOG_ERROR("Error: offset %d in unique pool is larger than block size %d\n", inoff, blocksize);
#endif

    }
    inoff=0;
  }

#if SAFE_CHECK
  if(outoff > kmv->getblocksize()){
    LOG_ERROR("Error: offset in KMV %d is larger than block size %d\n", outoff, kmv->getblocksize());
  }
#endif

  kmv->setblockdatasize(blockid, outoff);
  kmv->releaseblock(blockid);
}
#endif

#if 0
void MapReduce::kv2tkv(KeyValue *kv, KeyValue *tkv, KV_Block_info *block_info){

#pragma omp parallel
{
  char *key, *value;
  int keybytes, valuebytes, kvsize;

  Spool *hid_pool = new Spool(KEY_COUNT*sizeof(uint32_t));
  int *insert_off = new int[tnum];

// Stage: 1.1
  // Gather KV information in block
#pragma omp for
  for(int i = 0; i < kv->nblock; i++){

    int tkv_blockid=tkv->addblock();
    KV_Block_info *info=&block_info[tkv_blockid];
    Spool *hid_pool = new Spool(KEY_COUNT*sizeof(uint32_t));

    uint32_t *key_hid = (uint32_t*)hid_pool->addblock();
    int  nkey = 0;

    info->tkv_off = new int[tnum];
    info->tkv_size = new int[tnum];
    memset(info->tkv_off, 0, tnum*sizeof(int));
    memset(info->tkv_size, 0, tnum*sizeof(int));

    kv->acquireblock(i);
 
    char *kvbuf = kv->getblockbuffer(i);
    int datasize = kv->getblocktail(i);
    char *kvbuf_end = kvbuf+datasize;

     //int offset=0;
    while(kvbuf < kvbuf_end){
      
        char *kvbuf_start=kvbuf;
        keybytes = *(int*)(kvbuf);
        valuebytes = *(int*)(kvbuf+oneintlen);

        kvbuf += twointlen;
        kvbuf = ROUNDUP(kvbuf, kalignm);
        key = kvbuf;
        kvbuf += keybytes;
        kvbuf = ROUNDUP(kvbuf, valignm);
        value = kvbuf;
        kvbuf += valuebytes;
        kvbuf = ROUNDUP(kvbuf, talignm);
        kvsize=kvbuf-kvbuf_start;


      uint32_t hid = hashlittle(key, keybytes, 0);
      info->tkv_size[hid%tnum]+=kvsize;

      key_hid[nkey%KEY_COUNT]=hid;
      nkey++;
      if(nkey % KEY_COUNT==0){
        key_hid = (uint32_t*)hid_pool->addblock();
      }

      //offset+=kvsize;
    }

    insert_off[0]=info->tkv_off[0]=0;
    for(int j=1; j<tnum; j++){
      info->tkv_off[j]=info->tkv_off[j-1]+info->tkv_size[j-1];
      insert_off[j]=info->tkv_off[j];
    }

    tkv->acquireblock(tkv_blockid);
    char *tkvbuf = tkv->getblockbuffer(tkv_blockid);

    kvbuf = kv->getblockbuffer(i);
    for(int j=0; j<nkey; j++){  

      char *kvbuf_start=kvbuf;
      keybytes = *(int*)(kvbuf);
      valuebytes = *(int*)(kvbuf+oneintlen);

      kvbuf += twointlen;
      kvbuf = ROUNDUP(kvbuf, kalignm);
      key = kvbuf;
      kvbuf += keybytes;
      kvbuf = ROUNDUP(kvbuf, valignm);
      value = kvbuf;
      kvbuf += valuebytes;
      kvbuf = ROUNDUP(kvbuf, talignm);
      kvsize=kvbuf-kvbuf_start;
      uint32_t hid = *((int*)hid_pool->blocks[j/KEY_COUNT]+j%KEY_COUNT);

      int tkvoff = insert_off[hid%tnum];
 
      memcpy(tkvbuf+tkvoff, kvbuf_start, kvsize);
      insert_off[hid%tnum]+=kvsize;
      //offset+=kvsize;
    }
#if GATHER_STAT
    //double tt3=omp_get_wtime();
    //if(tid==TID) st.inc_timer(tstage12, tt3-tt2);
#endif

    hid_pool->clear();

    tkv->setblockdatasize(tkv_blockid, datasize);
    tkv->releaseblock(tkv_blockid);
    kv->releaseblock(i);

  }

  delete insert_off;
  delete hid_pool;
}// end parallel

}

void MapReduce::buildkvinfo(KeyValue *kv, KV_Block_info *block_info)
{
  //kv->print();
#pragma omp parallel
{
  //int tid = omp_get_thread_num();

  char *key, *value;
  int keybytes, valuebytes, kvsize;


#pragma omp for
  for(int i = 0; i < kv->nblock; i++){
    KV_Block_info *info = &block_info[i];
    info->hid_pool=new Spool((KEY_COUNT)*sizeof(int));
    info->off_pool=new Spool((KEY_COUNT)*sizeof(int));
    info->nkey=0;

    uint32_t *hid_buf = (uint32_t*)info->hid_pool->addblock();
    int *off_buf = (int*)info->off_pool->addblock();

    kv->acquireblock(i);
 
    char *kvbuf = kv->getblockbuffer(i);
    int datasize = kv->getblocktail(i);
    char *kvbuf_end = kvbuf+datasize;
    int off = 0;

    while(kvbuf < kvbuf_end){
      
      char *kvbuf_start=kvbuf;
      keybytes = *(int*)(kvbuf);
      valuebytes = *(int*)(kvbuf+oneintlen);

      kvbuf += twointlen;
      kvbuf = ROUNDUP(kvbuf, kalignm);
      key = kvbuf;
      kvbuf += keybytes;
      kvbuf = ROUNDUP(kvbuf, valignm);
      value = kvbuf;
      kvbuf += valuebytes;
      kvbuf = ROUNDUP(kvbuf, talignm);
      kvsize=kvbuf-kvbuf_start;

      uint32_t hid = hashlittle(key, keybytes, 0);

      hid_buf[info->nkey%KEY_COUNT]=hid;
      off_buf[info->nkey%KEY_COUNT]=off;
      info->nkey++;

      if(info->nkey % KEY_COUNT==0){
        hid_buf = (uint32_t*)info->hid_pool->addblock();
        off_buf = (int*)info->off_pool->addblock();
      }

      off += kvsize;
    }
    kv->releaseblock(i);
  } 
} 
}

#endif

#if 0
void MapReduce::unique2tmp_first(int nunique, DataObject *mv, Spool *unique_pool){
  
  int blockid = mv->addblock();
  mv->acquireblock(blockid);

  int  uidx=0;
  char *ubuf=unique_pool->blocks[uidx++];
  for(int i = 0; i < nunique; i++){
    
  } 

  mv->releaseblock(blockid);

}
#endif

#if 0
void MapReduce::unique2tmp_first(){
}
void MapReduce::unique2tmp(){
}


void MapReduce::unique2kmv(UniqueList *u, Partition *p, KeyMultiValue *kmv){
  //printf("unique2kmv start\n") ; 

  int kmv_block_id=kmv->addblock();
  kmv->acquireblock(kmv_block_id);
  
  char *kmv_buf=kmv->getblockbuffer(kmv_block_id);
  int kmv_off=0;

  char *key, *val;
  int keybytes, valbytes, kvsize;

  Spool *unique_pool=u->unique_pool;
  //printf("unique_pool->nblock=%d\n", unique_pool->nblock);

  for(int i=0; i<unique_pool->nblock; i++){
    char *ubuf=unique_pool->blocks[i];
    char *ubuf_end=ubuf+unique_pool->blocksize;
    while(ubuf < ubuf_end){
      Unique *ukey = (Unique*)(ubuf);
      if((ubuf_end-ubuf < sizeof(Unique)) || (ukey->key==NULL))
        break;
      *(int*)(kmv_buf+kmv_off)=ukey->keybytes;
      kmv_off+=sizeof(int);
      memcpy(kmv_buf+kmv_off, ukey->key, ukey->keybytes);
      kmv_off+=ukey->keybytes;
      *(int*)(kmv_buf+kmv_off)=ukey->nvalue;
      kmv_off+=sizeof(int);
      ukey->soffset=(int*)(kmv_buf+kmv_off);
      kmv_off+=ukey->nvalue*sizeof(int);
      ukey->voffset=kmv_buf+kmv_off;
      kmv_off+=ukey->mvbytes;
      ubuf+=ukeyoffset+ukey->keybytes;
      ubuf=ROUNDUP(ubuf, ualignm);
      
      //printf("key=%s, nvalue=%d, mvbytes=%d\n", ukey->key, ukey->nvalue, ukey->mvbytes);
     
      ukey->nvalue=0;
      ukey->mvbytes=0;

      //printf("kmv offset=%d\n", kmv_off);
    }
  }

  //printf("kmv_off=%d\n", kmv_off);
  kmv->setblockdatasize(kmv_block_id, kmv_off);


  Spool *tmp_kv_pool=p->tmp_kv_pool;

  //printf("tmp_kv_pool=%d\n", tmp_kv_pool->nblock);

  //printf("")
  for(int i=0; i<tmp_kv_pool->nblock; i++){
    char *kvbuf=tmp_kv_pool->blocks[i];
    char *kvbuf_end=kvbuf+tmp_kv_pool->blocksize;
    //int kvoff=0;
    while(kvbuf<kvbuf_end){
      //printf("kvbuf=%p, kvbuf_end=%p\n", kvbuf, kvbuf_end) ;     
 
      if(kvbuf_end-kvbuf<twointlen) break;
      char *kvbuf_start=kvbuf;
      keybytes = *(int*)kvbuf;
      valbytes = *(int*)(kvbuf+oneintlen);
      //printf("keybytes=%d, valytes=%d\n", keybytes, valbytes);
      
      if(keybytes+valbytes<=0) break;

      kvbuf += twointlen;
      kvbuf = ROUNDUP(kvbuf, kalignm);
      key = kvbuf;
      kvbuf += keybytes;
      kvbuf = ROUNDUP(kvbuf, valignm);
      val = kvbuf;
      kvbuf += valbytes;
      kvbuf = ROUNDUP(kvbuf, talignm);
      kvsize = kvbuf-kvbuf_start;

      //printf("key=%s, value=%s, kvsize=%d\n", key, val, kvsize);    
 
      uint32_t hid = hashlittle(key, keybytes, 0);
      // Find the key
      int ibucket = hid % nbucket;
      Unique *ukey, *pre;
      ukey = findukey(u->ulist, ibucket, key, keybytes, pre);
      ukey->soffset[ukey->nvalue]=valbytes;
      memcpy(ukey->voffset+ukey->mvbytes, val, valbytes);
      ukey->nvalue++;
      ukey->mvbytes+=valbytes;

      //printf("key=%s\n", ukey->key);
    }
  }

  kmv->releaseblock(kmv_block_id);
}
#endif


#if 0
// check if we need add another block
void MapReduce::checkbuffer(int bytes, char **buf, int *off, Spool *pool){
  int blocksize = pool->getblocksize();
  if(*off+bytes > blocksize){
    memset(*buf+*off, 0, blocksize-*off);
    *buf = pool->addblock();
    *off = 0;
  }
}
#endif

