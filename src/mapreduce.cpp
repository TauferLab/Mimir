#include "mpi.h"
#include "ctype.h"
#include "string.h"
#include "stdio.h"
#include "stdlib.h"
#include "stdint.h"
#include "sys/types.h"
#include "sys/stat.h"
#include "dirent.h"
#include "mapreduce.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <opa_primitives.h>
#include "kmv.h"
#include <ctime>
#include <iomanip>

//#include <map>
//#include <tuple>
#include <math.h>

#include "dataobject.h"
#include "log.h"

using namespace MAPREDUCE_NS;

//MapReduce::MapReduce(MPI_Comm caller){
//  data = NULL;
//}

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

uint64_t MapReduce::map(int nstr, char **strings, int selfflag, int recurse, 
    int readmode, void (*mymap) (MapReduce *, char *, int), 
    int myhash(char *, int), void *ptr){
  return 0;   
}

/* convert KV to KMV */
uint64_t MapReduce::convert(){
  KeyValue *kv = (KeyValue*)data;
  KeyMultiValue *kmv = new KeyMultiValue(0);

  kv->print();

// handled by multi-threads
//#pragma omp parallel
{
  int tid = omp_get_thread_num();
  int num = omp_get_num_threads();

  std::vector<std::list<Unique>> ht;

  DataObject *tmpdata = new DataObject(ByteType); 

  char *key, *value;
  int keybytes, valuebytes, ret;
  int keyoff, valoff;

  int nbucket = pow(2,20);
  ht.reserve(nbucket);
  for(int i = 0; i < nbucket; i++) ht.emplace_back();

  //((KeyValue*)(data))->print();

  // scan all kvs to gain the thread kvs
  for(int i = 0; i < data->nblock; i++){
    int offset = 0;

    kv->acquireblock(i);

    offset = kv->getNextKV(i, offset, &key, keybytes, &value, valuebytes, &keyoff, &valoff);

    while(offset != -1){
      uint32_t hid = hashlittle(key, keybytes, 0);
      if(hid % num == tid){ 
        int ibucket = hid % nbucket;
        std::list<Unique>& ul = ht[ibucket];
        std::list<Unique>::iterator u;
     
        // search to see if the key has been in the list
        for(u = ul.begin(); u != ul.end(); u++){
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
    }

    //((DataObject*)(kv))->print();

    // merge locally
    int blockid = tmpdata->addblock();
    tmpdata->acquireblock(blockid);

    std::vector<std::list<Unique>>::iterator ul;
    for(ul = ht.begin(); ul != ht.end(); ++ul){
      std::list<Unique>::iterator u;
      for(u = ul->begin(); u != ul->end(); ++u){
       // merge all values together
       int bytes = sizeof(int)*(u->cur_nval) + (u->cur_size);

       // if the block is full, add another block 
       if(tmpdata->getblockspace(blockid) < bytes){
          tmpdata->releaseblock(blockid);
          blockid = tmpdata->addblock();
          tmpdata->acquireblock(blockid);
        }
        // add sizes
        std::list<Pos>::iterator l;
        int off = tmpdata->getblocktail(blockid);
        for(l = u->cur_pos.begin(); l != u->cur_pos.end(); ++l){
          tmpdata->addbytes(blockid, (char*)&(l->size), (int)sizeof(int));
        }
        
        char *p;
        tmpdata->getbytes(blockid, off, &p);

        // add values
        for(l = u->cur_pos.begin(); l != u-> cur_pos.end(); ++l){
          char *p = NULL;
          kv->getbytes(i, l->off, &p);
          tmpdata->addbytes(blockid, p, l->size); 
        }
        u->nval += u->cur_nval;
        u->size += u->cur_size;
        u->pos.push_back(Pos(off,u->cur_size,blockid,u->nval));
        

        u->cur_nval = 0;
        u->cur_size = 0;
        u->cur_pos.clear();
      }
    }
    tmpdata->releaseblock(blockid);

    kv->releaseblock(i);
//#pragma omp barrier
  }

  //tmpdata->print();

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
      delete [] u->key;
    }
  }
  if(blockid != -1) kmv->releaseblock(blockid);

  //((DataObject*)(kmv))->print();

  delete tmpdata;
} 

  delete data;
  data = kmv;

  kmv->print();

  return 0;
}

uint64_t MapReduce::reduce(void (myreduce)(MapReduce *, char *, int, int, char *, 
    int *, void*), void* ptr){

  KeyMultiValue *kmv = (KeyMultiValue*)data;
  KeyValue *kv = new KeyValue(1);
  data = kv;

  kmv->print();

//#pragma omp parallel
{
  int tid = omp_get_thread_num();
  int num = omp_get_num_threads();

  char *key, *values;
  int keybytes, nvalue, *valuebytes;
  blocks[tid] = -1;

//#pragma omp for
  for(int i = 0; i < kmv->nblock; i++){
     int offset = 0;
     kmv->acquireblock(i);
     offset = kmv->getNextKMV(i, offset, &key, keybytes, nvalue, &values, &valuebytes);
     while(offset != -1){
       // apply myreudce here
       myreduce(this, key, keybytes, nvalue, values, valuebytes, ptr);        
       offset = kmv->getNextKMV(i, offset, &key, keybytes, nvalue, &values, &valuebytes);
     }
     kmv->releaseblock(i);
  }
}

  delete kmv;

  kv->print();

  return 0;
}

uint64_t MapReduce::scan(void (myscan)(char *, int, int, char *, int *,void *), void * ptr){
  KeyMultiValue *kmv = (KeyMultiValue*)data;

  char *key, *values;
  int keybytes, nvalue, *valuebytes;
  
  for(int i = 0; i < kmv->nblock; i++){
    int offset = 0;
    
    kmv->acquireblock(i);
    
    offset = kmv->getNextKMV(i, offset, &key, keybytes, nvalue, &values, &valuebytes);

    while(offset != -1){
      myscan(key, keybytes, nvalue, values, valuebytes, ptr);

      offset = kmv->getNextKMV(i, offset, &key, keybytes, nvalue, &values, &valuebytes);
    }

    kmv->releaseblock(i);
  }

}

uint64_t MapReduce::add(char *key, int keybytes, char *value, int valuebytes){
  KeyValue *kv = (KeyValue*)data; 
 
  int tid = omp_get_thread_num();
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
     
  return 0;
}

/********************************************************************/

#define MBYTES 2
#define MRMPI_BIGINT MPI_UNSIGNED_LONG_LONG
#define MAXLINE 2048
#define LOCAL_BUFFER_SIZE (1024*1024) //per proc

#ifdef FUSION
    #define N_THREADS 12
#else
    #define N_THREADS 4
#endif

//xxxxa as in argument
MapReduce::MapReduce(MPI_Comm caller)
{
    comm = caller;
    MPI_Comm_rank(comm,&me);
    MPI_Comm_size(comm,&nprocs);

    msize = MBYTES;
    in_buffer_size = MBYTES * 1024 * 1024;
    kv_buffer_size = MBYTES * 1024 * 1024;

    //log_base = std::string(log_basea);
    //logf_map_p = log_base + "/log_map_p" + std::to_string(me);
    //logf_map_p_of.open(logf_map_p,  std::ofstream::out|std::ofstream::app);
    //logf_reduce_p = log_base + "/log_reduce_p" + std::to_string(me);
    //logf_reduce_p_of.open(logf_reduce_p,  std::ofstream::out|std::ofstream::app);

    //spill_base = std::string(spill_basea);
    //out_base = std::string(out_basea);
    log_base = std::string("./");
    spill_base = std::string("./");
    out_base = std::string("./");

    num_kvbuffer_be = 0;
    num_kvbuffer_ae = 0;

    debug_num_input_buffer = 0;
    debug_num_kv_buffer = 0;

    map_exchange_request = new MPI_Request[2];//one for each buffer
    map_exchange_status = new MPI_Status[2];

    map_num_exchange = 0;

    spill_num_kv_after_all2all = 0;
    spill_num_kv_after_map = 0;

    //mpi_ialltoallv and mpi_wait time
    mpi_itime = 0.0;
    mpi_wait_s = 0.0;
    mpi_wait_e = 0.0;
    mpi_iall_s = 0.0;
    mpi_iall_e = 0.0;

    map_num=0;//increment everytime you call map
    reduce_num=0;

    omp_set_num_threads(N_THREADS);

#pragma omp parallel
{
    tnum = omp_get_num_threads();    
}

    printf("tnum=%d\n", tnum);

    blocks = new int[tnum];   
    for(int i = 0; i < tnum; i++) blocks[i] = -1;
}

MapReduce::~MapReduce()
{
    delete [] blocks;

        delete [] map_exchange_request;

        delete [] map_exchange_status;
    logf_map_p_of.close();
    logf_reduce_p_of.close();
}

unsigned long int MapReduce::set_in_buffer_size(unsigned long int size)
{
    in_buffer_size = size ;//when debug, no *1024*1024
    return in_buffer_size;
}

unsigned long int MapReduce::set_kv_buffer_size(unsigned long int size)
{
    kv_buffer_size = size ;
    return kv_buffer_size;
}


/*find the map input files given the input dir "in_file",
store the file names into the string vector "svec_map_in_file" note bad name*/
void MapReduce::findfiles(char *in_file)
{
    int err;
    char newstr[MAXLINE];

    struct stat in_path_stat;
    err = stat(in_file, &in_path_stat);
    if (err){
        char msg[256];
        printf("Could not query status of file %s in map.\n",in_file);
    }else if (S_ISREG(in_path_stat.st_mode)){
            svec_map_in_files.push_back(std::string(in_file));
            //logf_map_p_of<<"Push back 1 "<<std::string(in_file)<<std::endl;

    }else if (S_ISDIR(in_path_stat.st_mode)){
        struct dirent *ep;
        DIR *dp = opendir(in_file);
        if (dp == NULL){
            char msg[256];
            printf("Cannot open dir %s to search for files in map.\n", in_file);
        }
        while (ep = readdir(dp)){
            if (ep->d_name[0] == '.') continue;
            sprintf(newstr,"%s/%s", in_file, ep->d_name);
            err = stat(newstr, &in_path_stat);
            if (S_ISREG(in_path_stat.st_mode)){
                svec_map_in_files.push_back(std::string(newstr));
                //logf_map_p_of<<"Push back 2 "<<std::string(in_file)<<std::endl;
            }else if (S_ISDIR(in_path_stat.st_mode)){
                findfiles(newstr);
            }
        }
        closedir(dp);
    }else{
            char msg[256];
            printf("Invalid filename %s in map.\n",in_file);
    }
}

/*add the files of kv chunks output by reduce svec_reduce_out_kv_files
 to the svec_map_in_files
*/
void MapReduce::add_files_from_reduce()
{
     if (DEBUG)
     {
         logf_map_p_of<<"Before adding files from reduce, the input files to map are: "<<std::endl;
         for (auto it = svec_map_in_files.begin(); it != svec_map_in_files.end(); ++it)
        {
            logf_map_p_of<< *it<<std::endl;
        }
     }

    for (auto it = svec_reduce_out_kv_files.begin(); it != svec_reduce_out_kv_files.end(); ++it)
    {
        svec_map_in_files.push_back(*it);
    }
     if (DEBUG)
     {
         logf_map_p_of<<"After adding files from reduce, the input files to map are: "<<std::endl;
         for (auto it = svec_map_in_files.begin(); it != svec_map_in_files.end(); ++it)
        {
            logf_map_p_of<< *it<<std::endl;
        }
     }

}


/*add the files of kv chunks output by map svec_spillf_kv_after_all2all
 to the svec_map_in_files
*/
void MapReduce::add_files_from_map()
{
     if (DEBUG)
     {
         logf_map_p_of<<"Before adding files from map, the input files to map are: "<<std::endl;
         for (auto it = svec_map_in_files.begin(); it != svec_map_in_files.end(); ++it)
        {
            logf_map_p_of<< *it<<std::endl;
        }
     }

    for (auto it = svec_spillf_kv_after_all2all.begin(); it != svec_spillf_kv_after_all2all.end(); ++it)
    {
        svec_map_in_files.push_back(*it);
    }


     if (DEBUG)
     {
         logf_map_p_of<<"After adding files from map, the input files to map are: "<<std::endl;
         for (auto it = svec_map_in_files.begin(); it != svec_map_in_files.end(); ++it)
        {
            logf_map_p_of<< *it<<std::endl;
        }
        
     }
}

/*
 * map function
 *   in_patha: input file path
 *   read_modea: 1 for word, 2 for line
 *   map_type: only 2 is used
 */
uint64_t MapReduce::map(char *in_patha, int read_modea, int map_type,
        void (*mymap) (MapReduce *, char *, char *, int *, char *, int *, int, void *), void* ptr)
{
  data = new KeyValue(0);
  
  map_num++;

  findfiles(in_patha);

  input_buffer = new char [in_buffer_size];
  int ele_ptr_size = in_buffer_size/2;
  ele_ptr = new char*[ele_ptr_size];

  uint64_t num_ele = 0;
  uint64_t data_end = 0;
  int my_f_flag = 1;//1 means I am not done with reading all my input files
  int all_f_flag = 1; //1 means there are some proc is still working on its files

  //0 means all procs are done with their files
  long int inputfile_offset = 0;

  kv_buffer1 = new char[kv_buffer_size];
  kv_buffer2 = new char[kv_buffer_size];
  kv_buffer = kv_buffer1;//points to one of the kv_buffer1, kv_buffer2
  offset1 = new OPA_int_t[nprocs];
  offset2 = new OPA_int_t[nprocs];
  offset = offset1;

  //for the file metadata, id is the num used in file name
  uint64_t keysize_all = 0;
  uint64_t nkey_all = 0;
  uint64_t kvsize_all = 0;

  double proc_sync_time=0;
  double proc_comm_time=0;

  map_num_exchange = 0;

  bool switch_flag = false;
  for (int i = 0; i<nprocs; i++)
  {
    OPA_store_int(&offset1[i], ((kv_buffer_size/nprocs) * i));
    OPA_store_int(&offset2[i], ((kv_buffer_size/nprocs) * i));
  }

  //send/recv count buffers for the two buffer
  int *p_map_sendcount1 = new int[nprocs], *p_map_recvcount1 = new int[nprocs]; //the send/recv count
  int *p_map_sendcount2 = new int[nprocs], *p_map_recvcount2 = new int[nprocs];
  int *p_map_sdispls1 = new int[nprocs], *p_map_rdispls1 = new int[nprocs];
  int *p_map_sdispls2 = new int[nprocs], *p_map_rdispls2 = new int[nprocs];

  while (1)
  {
    read_to_input_buffer(read_modea, &my_f_flag, &inputfile_offset, &num_ele, &data_end);

    int *thread_nele = new int[N_THREADS]; //number of element processed by each thread
    int *thread_done = new int[N_THREADS]; // done or not for each thread
    int temp = num_ele/N_THREADS;
    int temp_left = num_ele%N_THREADS;//last thread has more work
    int i;
    for ( i = 0; i < N_THREADS-1; i++){
      thread_nele[i] = temp;
      thread_done[i] = 0;
    }

    thread_nele[i] = temp+temp_left;//last thread process more
    thread_done[i] = 0;

    int left = num_ele;//start process this input_buffer to kv_buffer
    int p_mapc_done = 0; //proc is not done with my current map input buffer computation
    while (left >= 0)//left = 0 all the data in the input buffer is processed
    {
      if (left > 0)//process elements in input_buffer, omp parallel session
      {
        left = num_ele;

#pragma omp parallel
{
        int tid = omp_get_thread_num();
        int nthreads = omp_get_num_threads();
        int my_nele = thread_nele[tid];
        int my_done = thread_done[tid];
                    
        //local buffer
        char *local_kv_buffer = new char[nprocs*LOCAL_BUFFER_SIZE];//store kv locally, before copy to global kv buffer
        int *local_offset = new int[nprocs];//local offset to the local kv buffer
        int *global_offset = new int[nprocs];//offset after opa_fetch_and_add

        for (int i = 0; i<nprocs; i++)
        {
          local_offset[i] = (LOCAL_BUFFER_SIZE/nprocs) * i;
          global_offset[i] = 0;
        }

        double opa_time=0, omp_atomic_time=0, omp_sync_time=0;
        double opa_time_s, opa_time_e, omp_atomic_time_s, omp_atomic_time_e, omp_sync_time_s, omp_sync_time_e;

         //these are taken out from within the while loop
         char tmp_key[MAXLINE];
         char tmp_value[MAXLINE];
         char *word = NULL;
         int keysize = 0;
         int valuesize = 0;
         int proc_id = 0;
         int kv_size = 0;
         int offset_kv = 0;

         int proc_mask = nprocs - 1;
         while ((my_done <my_nele) && (!switch_flag))
         {
           word = ele_ptr[my_done+(tid*thread_nele[0])];
           mymap(this, word, tmp_key, &keysize, tmp_value, &valuesize,tid, ptr);//call user mymap function with the word
                        //after mymap, tmp_key and tmp_value contain c str that end with '\0'
           keysize++;
           valuesize++;
           proc_id = hashlittle(tmp_key, keysize,nprocs) & proc_mask;
           kv_size = keysize + valuesize;

           offset_kv = local_offset[proc_id];//the offset in the local buffer for this kv
           //need to copy local buffer to global buffer in 2 cases
           //1. local buffer is full , i am not done
           //2. local buffer is not full, i am done
                               
           if ((offset_kv+kv_size) < ((LOCAL_BUFFER_SIZE/nprocs) * (proc_id+1)))
           {
             //can copy to lcoal buffer, local buffer is not full
             memcpy(&local_kv_buffer[offset_kv], tmp_key, keysize);
             //local_kv_buffer[offset_kv+keysize-1] = ' ';
             local_kv_buffer[offset_kv+keysize-1] = '\0';
             memcpy(&local_kv_buffer[offset_kv+keysize], tmp_value, valuesize);
             //local_kv_buffer[offset_kv+kv_size-1] = ' ';
             local_kv_buffer[offset_kv+kv_size-1] = '\0';
             local_offset[proc_id] += kv_size;
             my_done++;
           }else{
             bool ok_cp_global = 1;//1: ok to copy from local kv buffer
                            //to global kv buffer, if any global_offset overflow, set 
             //this bool to 0, and break with valu = 0
             for (int i = 0; i < nprocs; i++)
             {
               int local_start = (LOCAL_BUFFER_SIZE/nprocs) * i;
               global_offset[i] = OPA_fetch_and_add_int(&offset[i], (local_offset[i]-local_start));//old value
               if ( (global_offset[i] + local_offset[i]) >= (kv_buffer_size/nprocs)*(i+1))
               {
                 ok_cp_global = 0;
                 for (int j = i; j>=0; j--)
                 {
                   OPA_store_int(&offset[j],global_offset[j]);
                 }
                                  
                 break;
               }
             }
                            //if (DEBUG)
                            //{
                            //    logf_map_t_of<<"ok to copy to global flag is: "<<ok_cp_global<<std::endl;
                            //}
                            if (ok_cp_global)
                            {
                                //ok to copy from local buffer to global kv buffer
                                for (int i = 0; i <nprocs; i++)
                                {
                                    int old_val = global_offset[i];
                                    int local_start = (LOCAL_BUFFER_SIZE/nprocs) * i;
                                    memcpy(&kv_buffer[old_val], &local_kv_buffer[local_start],(local_offset[i]-local_start));

                                    //if (DEBUG)
                                    //{
                                    //    logf_map_t_of<<"copy kv from loca buffer to global buffer "
                                    //       <<"local buffer ["<<i<<"], from "<<local_start<<" with size "
                                    //        <<(local_offset[i]-local_start)<<" to global from start "<<old_val<<std::endl;
                                    //}
                                }
                                thread_done[tid] = my_done;
                                //my_keysize_all += tmp_keysize_all;
                                //my_nkey_all += tmp_nkey_all;
                                //my_kvsize_all += tmp_kvsize_all;

                            }else{
                                //not ok to copy from local buffer to global kv buffer
                                switch_flag = true;
                                //if (DEBUG)
                                //{
                                //    logf_map_t_of<<"Set switch flag to true."<<std::endl;
                                //}
                                //#pragma omp flush (switch_flag)
                            }//end of to copy from local buffer to global kv buffer


                            //after copy local buffer to global buffer, need to reset local buffer offsets
                            for (int i = 0; i<nprocs; i++)
                            {
                                local_offset[i] = (LOCAL_BUFFER_SIZE/nprocs) * i;
                                global_offset[i] = 0;
                            }
                            //tmp_keysize_all = 0;
                            //tmp_nkey_all = 0;
                            //tmp_kvsize_all = 0;
                        }//end of local buffer fit

                        
                        if (my_done == my_nele)
                        {
                            //I am done, cp to global buffer
                            //if (DEBUG)
                            //{
                                
                            //    logf_map_t_of <<"If I am done: local kv buffer is full "<<std::endl;
                            //    logf_map_t_of<<"My share is "<<my_nele<<" My done is "
                            //        <<my_done<<" switch flag is "<<switch_flag<<std::endl;

                            //}
                            bool ok_cp_global = 1;//1: ok to copy from local kv buffer
                            //to global kv buffer, if any global_offset overflow, set 
                            //this bool to 0, and break with valu = 0
                            for (int i = 0; i < nprocs; i++)
                            {
                                int local_start = (LOCAL_BUFFER_SIZE/nprocs) * i;
                                opa_time_s = omp_get_wtime();
                                global_offset[i] = OPA_fetch_and_add_int(&offset[i], (local_offset[i]-local_start));//old value
                                opa_time_e = omp_get_wtime();
                                opa_time += (opa_time_e - opa_time_s);
                                //if (DEBUG)
                                //{
                                //    logf_map_t_of<<"after OPA fetch and add, global offset "<<i<<" is "
                                //        <<global_offset[i]<<" local offset "<<i<< " is "<<local_offset[i]<<std::endl;
                                //}
                                if ( (global_offset[i] + local_offset[i]) >= (kv_buffer_size/nprocs)*(i+1))
                                {
                                    ok_cp_global = 0;
                                    for (int j = i; j>=0; j--)
                                    {
                                        OPA_store_int(&offset[j],global_offset[j]);
                                    }
                                    //if (DEBUG)
                                    //{
                                    //    logf_map_t_of<<"Global buffer "<<i<<" overflows when try to copy."<<std::endl;
                                    //}
                                    
                                    break;
                                }
                            }
                            //if (DEBUG)
                            //{
                            //    logf_map_t_of<<"ok to copy to global flag is: "<<ok_cp_global<<std::endl;
                            //}
                            if (ok_cp_global)
                            {
                                //ok to copy from local buffer to global kv buffer
                                for (int i = 0; i <nprocs; i++)
                                {
                                    int old_val = global_offset[i];
                                    int local_start = (LOCAL_BUFFER_SIZE/nprocs) * i;
                                    memcpy(&kv_buffer[old_val], &local_kv_buffer[local_start],(local_offset[i]-local_start));

                                    //if (DEBUG)
                                    //{
                                    //    logf_map_t_of<<"copy kv from loca buffer to global buffer "
                                    //        <<"local buffer ["<<i<<"], from "<<local_start<<" with size "
                                    //        <<(local_offset[i]-local_start)<<" to global from start "<<old_val<<std::endl;
                                    //}
                                }
                                thread_done[tid] = my_done;
                                //my_keysize_all += tmp_keysize_all;
                                //my_nkey_all += tmp_nkey_all;
                                //my_kvsize_all += tmp_kvsize_all;

                            }else{
                                //not ok to copy from local buffer to global kv buffer
                                switch_flag = true;
                                //if (DEBUG)
                                //{
                                //    logf_map_t_of<<"Set switch flag to true."<<std::endl;
                                //}
                                //#pragma omp flush (switch_flag)
                            }//end of to copy from local buffer to global kv buffer


                            //after copy local buffer to global buffer, need to reset local buffer offsets
                            for (int i = 0; i<nprocs; i++)
                            {
                                local_offset[i] = (LOCAL_BUFFER_SIZE/nprocs) * i;
                                global_offset[i] = 0;
                            }
                            //tmp_keysize_all = 0;
                            //tmp_nkey_all = 0;
                            //tmp_kvsize_all = 0;
                        }//end if I am done
                        //#pragma omp flush (switch_flag)
                    }//end while (mydone && !switch flag)

                    //omp_atomic_time_s = omp_get_wtime();
                    #pragma omp atomic//left need to be the same on each 
                    left -= my_done;
                    //omp_atomic_time_e = omp_get_wtime();
                    //omp_atomic_time += (omp_atomic_time_e - omp_atomic_time_s);
                    //omp_sync_time = omp_atomic_time + opa_time;
                    //if (DEBUG)
                    //{
                    //    logf_map_t_of <<"Exit parallel session in map. map number exchange is " 
                    //        <<map_num_exchange<<"my done is "<<my_done<< std::endl;
                    //logf_map_t_of<<"OPA fa: "<<std::setprecision(15)<<opa_time<<std::endl
                    //    <<"omp atomic: "<<omp_atomic_time<<std::endl
                    //    <<"omp sync: "<<omp_sync_time<<std::endl;
                    //    logf_map_t_of.close();
                    //}
                }//end pragma omp parallel

                //double map_thread_e_timer = omp_get_wtime();
                //double map_thread_time = map_thread_e_timer -  map_thread_s_timer;
                //logf_map_p_of<<"Map thread end time: "<<map_thread_e_timer<<std::endl;
                //logf_map_p_of<<"Map thread time: "<<map_thread_time<<" seconds"<<std::endl;

                //if (DEBUG)
                //{
                //    logf_map_p_of << "After the omp paralle session."<<std::endl;
                //    logf_map_p_of <<"The offset in buffer is: ";
                //    for (int i=0; i<nprocs; i++){
                //        logf_map_p_of << OPA_load_int(&offset[i]) <<" ";
                //    }
                //    logf_map_p_of << std::endl;
                //    logf_map_p_of<<"Left is "<<left
                //        <<"map number of exchage is "<<map_num_exchange<<std::endl;
                //}

            }//end if left >0, left = 0, all the data in the input buffer is processed

            //2 conditions: left = 0; the input_buffer is processed by the threads,
            //need to receive from other procs
            //left > 0; the input_buffer is not finished, but one of the kv_buffer is 
            //full, need to send kv buffer and switch to the other one

            if (map_type==2){
                /*MPI_Ialltoallv to send/recv buffer*/
                exchange_kv(p_map_sendcount1, p_map_recvcount1, p_map_sdispls1, p_map_rdispls1, p_map_sendcount2, p_map_recvcount2, p_map_sdispls2, p_map_rdispls2);
            }else if (map_type==1){
                /**/
                printf("P %d, map type %d erro.\n", me, map_type);

            }else{
                printf("P %d, map type %d erro.", me, map_type);
            };


            /*determin if all procs are done with map compuataion on this input data buffer*/
            if (left == 0){
                p_mapc_done = 1; //proc is done with my map comp
            }else{
                p_mapc_done = 0;
            }
            int num_mapc_done = 0;
            //double proc_sync_s_timer = omp_get_wtime();
            MPI_Allreduce(&p_mapc_done, &num_mapc_done, 1, MPI_INT, MPI_SUM, comm);


            //to determine if all the procs have done with their input files
            MPI_Allreduce(&my_f_flag, &all_f_flag, 1, MPI_INT, MPI_SUM, comm);
            //double proc_sync_e_timer = omp_get_wtime();
            //proc_sync_time += (proc_sync_e_timer - proc_sync_s_timer);

            //if (DEBUG)
            //{
            //    logf_map_p_of<<"After processing the input_buffer, all_f_flag is "
            //        <<all_f_flag<<std::endl;
            //}

            /* wait for previous ialltoallv,
            if all map computation done, break out of the while(left>=0)loop
            means proceed to test if all input files are done, 
            if yes, wait for last ialltoallv, break; else go to read more input*/

            //if (DEBUG)
            //{
            //    logf_map_p_of<<"Wait for the previous ialltoallv."
            //        <<std::endl;
            //}
            if (map_type ==2){
                wait_previous_ialltoallv_switch_buffer(map_num_exchange, &switch_flag);
            }
            //if (DEBUG)
            //{
            //    logf_map_p_of<<"After wait for previous ialltoallv, and switch buffer."
            //        <<std::endl;
            //}

            if (num_mapc_done == nprocs)
            {//all proc done with map compuation for the current input buffer
                //if (DEBUG)
                //{
                //    logf_map_p_of<<"All proc done with map comp for current input buffer"
                //        <<std::endl;
                //}
                break;
            }


        }//end while left >=0

        std::ostringstream s;
        //s << "all_f_flag=" << all_f_flag;
        //log.output(s.str());
        if (all_f_flag == 0){//all procs are done with their files
            //if (DEBUG)
            //{
            //    logf_map_p_of<<"all proc done with all input files."<<std::endl;
            //}
            if (map_type==2){
                wait_last_ialltoallv(map_num_exchange);
            }

            delete [] input_buffer;
            delete [] ele_ptr;
            delete [] kv_buffer1;
            delete [] kv_buffer2;
            delete [] offset1;
            delete [] offset2;
            delete [] p_map_sendcount1;
            delete [] p_map_recvcount1;
            delete [] p_map_sendcount2;
            delete [] p_map_recvcount2;
            delete [] p_map_sdispls1;
            delete [] p_map_rdispls1;
            delete [] p_map_sdispls2;
            delete [] p_map_rdispls2;
            delete [] thread_nele;
            delete [] thread_done;
            //close log files
            //logf_map_p_of <<"Map: exit map function.\n" <<std::endl;
            break;
        }

    }//end while 1

    //double map_e_timer = omp_get_wtime();
    //double map_time = map_e_timer - map_s_timer;
    //logf_map_p_of<<"Map end time: "<<map_e_timer<<std::endl;
    //logf_map_p_of<<"MPI_Iall and MPI_Wait time: "<<mpi_itime<<std::endl;
    //logf_map_p_of<<"Map time: "<<map_time <<" seconds. "<<std::endl;

    return 0;

}//end of map

/*map_local: map without communication, output local files after applying
a "map" function. Use the kv buffer differently.*/

uint64_t MapReduce::map_local(int input_loca, char *in_patha, int read_modea, int map_type, 
        char *log_patha, char *spill_basea, char *out_patha,
        void (*mymap) (MapReduce *, char *, char *, int *, char *, int *, int))    
{
    
    //log map file for process is open in the contructor
    logf_map_p_of << "Map_local: enter map function."<<std::endl;
    double map_s_timer = omp_get_wtime();
    logf_map_p_of<<"Map_local start time: "<<std::setprecision(15)<<map_s_timer<<std::endl;

    map_num++;
    findfiles(in_patha); //put all input files into the vector


    //svec_map_in_files
    if (DEBUG)
    {
        logf_map_p_of <<"Input files for map_local are: "<<std::endl;
        for (auto it = svec_map_in_files.begin(); it != svec_map_in_files.end(); ++it)
        {
            logf_map_p_of << *it <<std::endl;
        }
    }

    //read input file to a data buffer, later threads will do work sharing on this buffer
    //printf("P%d, in map_local, in_buffer_size : %lu.\n", me, in_buffer_size);
    //exit(0);
    input_buffer = new char [in_buffer_size];
    int ele_ptr_size = in_buffer_size/2;
    ele_ptr = new char*[ele_ptr_size];
    //ele_ptr = std::vector<char *>;
    uint64_t num_ele = 0;
    uint64_t data_end = 0;
    int my_f_flag = 1;//1 means I am not done with reading all my input files
    int all_f_flag = 1; //1 means there are some proc is still working on its files
    //0 means all procs are done with their files
    long int inputfile_offset = 0;

    //TODO: each thread write into its own buffer for map_local
    //kv_buffer1 = new char[kv_buffer_size];
    //kv_buffer2 = new char[kv_buffer_size];
    /*offsets to where to write next on the process kv_buffer for
    each thread*/
    int *kv_buffer_offsets = new int[N_THREADS];
    char ** kv_buffer_ptrs = new char *[N_THREADS];
    /*for spill file name*/
    int *num_spill_map_locals = new int[N_THREADS];

    for (int i =0; i<N_THREADS; i++){
        kv_buffer_offsets[i] = 0;
        kv_buffer_ptrs[i] =new char[kv_buffer_size];
        num_spill_map_locals[i] = 0;
    }


    //kv_buffer_ptrs[0] = kv_buffer1;
    //kv_buffer_ptrs[1] = kv_buffer2;

    //num_spill_map_locals[0] =0;
    //num_spill_map_locals[1] =0;

    //for the file metadata, id is the num used in file name
    uint64_t keysize_all = 0;
    uint64_t nkey_all = 0;
    uint64_t kvsize_all = 0;

    double read_input_time=0;
    double spill_output_time=0;
    double proc_sync_time=0;
    double thread_sync_time=0;

    while (1)
    {
        if (DEBUG)
        {
            logf_map_p_of<<"In while 1, before read to input buffer."<<std::endl;
        }
        double io_s_timer =  omp_get_wtime();
        logf_map_p_of<<"Read from file to input buffer start time: "<<io_s_timer<<std::endl;
        read_to_input_buffer(read_modea, &my_f_flag, &inputfile_offset, &num_ele, &data_end);

        double io_e_timer = omp_get_wtime();
        double io_time = io_e_timer - io_s_timer;
        logf_map_p_of<<"Read end time: "<<io_e_timer<<std::endl;
        logf_map_p_of<<"Read time: "<<io_time<<" seconds"<<std::endl;
        read_input_time += io_time;
        //after this, the input_buffer is populated with words, pass back each word
        //to mymap function
        if (DEBUG)
        {
            logf_map_p_of<<"In while 1, after read to input buffer."<<std::endl;
        }


        int *thread_nele = new int[N_THREADS]; //number of element processed by each thread
        int *thread_done = new int[N_THREADS]; // done or not for each thread
        int temp = num_ele/N_THREADS;
        int temp_left = num_ele%N_THREADS;//last thread has more work
        int i;
        for ( i = 0; i < N_THREADS-1; i++){
            thread_nele[i] = temp;
            thread_done[i] = 0;
        }
        thread_nele[i] = temp+temp_left;//last thread process more
        thread_done[i] = 0;

        int left = num_ele;//start process this input_buffer to kv_buffer
        int p_mapc_done = 0; //proc is not done with my current map input buffer computation

        /*left = 0 all the data in the input buffer is processed*/
        //process elements in input_buffer, omp parallel session
        
        left = num_ele;
        if (DEBUG)
        {
            logf_map_p_of << "Before enter the omp paralle session. "<<std::endl;
            logf_map_p_of <<"The offset in buffer is: ";
            for (int i=0; i<N_THREADS; i++){
                logf_map_p_of << kv_buffer_offsets[i] <<" ";
            }
            logf_map_p_of << std::endl;
        }

        double map_thread_s_timer = omp_get_wtime();
        logf_map_p_of<<"Map thread start time: "<<map_thread_s_timer<<std::endl;

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            int nthreads = omp_get_num_threads();
            int my_nele = thread_nele[tid];
            int my_done = thread_done[tid];

            std::string logf_map_t = log_base + "/log_map_p" + std::to_string(me)
                + "t"+std::to_string(tid);
            std::ofstream logf_map_t_of;    
            logf_map_t_of.open(logf_map_t,  std::ofstream::out|std::ofstream::app);


            double opa_time=0, omp_atomic_time=0, omp_sync_time=0;
            double opa_time_s, opa_time_e, omp_atomic_time_s, omp_atomic_time_e, omp_sync_time_s, omp_sync_time_e;
            double thread_io_time=0;
            double thread_io_s_timer=0;
            double thread_io_e_timer=0;

            //these are taken out from within the while loop
            char tmp_key[MAXLINE];
            char tmp_value[MAXLINE];
            char *word = NULL;
            int keysize = 0;
            int valuesize = 0;
            int proc_id = 0;
            int kv_size = 0;
            int offset_kv = kv_buffer_offsets[tid];
            char *kv_buffer = kv_buffer_ptrs[tid];

            while (my_done <my_nele) 
            {
                /*apply map, write to process kv buffer, spill if needed*/
                if (DEBUG)
                {
                    logf_map_t_of<<"In while mydone < my nele,"
                        <<" local offset is "<<kv_buffer_offsets[tid];
                    logf_map_t_of<<std::endl;
                }
                word = ele_ptr[my_done+(tid*thread_nele[0])];
                if (DEBUG)
                {
                    logf_map_t_of<<"Before calling mymap funciton, word: "
                        <<word<<"."<<std::endl;
                }
                mymap(this, word, tmp_key, &keysize, tmp_value, &valuesize, tid);//call user mymap function with the word
                //after mymap, tmp_key and tmp_value contain c str that end with '\0'
                //keysize = strlen(tmp_key)+1;
                //valuesize = strlen(tmp_value)+1;
                keysize++;
                valuesize++;

                kv_size = keysize + valuesize;

                if (DEBUG)
                {
                    logf_map_t_of<<"After processing word: "<<word<<
                    " the offset kv is: "<<offset_kv<<std::endl;
                    logf_map_t_of<<"the size of key is: "<<keysize<<" the size of value is: "
                        <<valuesize<<std::endl;
                    logf_map_t_of<<"The new key: "<<tmp_key<<", value: "<<tmp_value
                        <<std::endl;
                }

                if ((offset_kv+kv_size) >= kv_buffer_size)
                {
                    /*spill kv to disk
                    reset kv buffer pointer and offset*/
                    if (DEBUG)
                    {
                        
                        logf_map_t_of <<"If kv does not fit in memory:  kv buffer is full "<<std::endl;
                        logf_map_t_of<<"My share is "<<my_nele<<" My done is "
                            <<my_done<<std::endl;

                    }

                    char t_kv_buffer[256];
                    /*
                    sprintf(t_kv_buffer,"%s/%s.%s%d.%s%d.%s%d.%s%d",spill_base.c_str(),"spill_kv_map_local",
                        "p",me,"t",tid,"f", num_spill_map_locals[tid], "map",map_num);*/
                    sprintf(t_kv_buffer,"%s/%s.%s%d.%s%d.%s%d.%s%d",spill_basea,"spill_kv_map_local",
                        "p",me,"t",tid,"f", num_spill_map_locals[tid], "map",map_num);
                    thread_io_s_timer = omp_get_wtime();
                    this->spill(kv_buffer_ptrs[tid],t_kv_buffer,offset_kv);
                    thread_io_e_timer = omp_get_wtime();
                    thread_io_time += (thread_io_e_timer -  thread_io_s_timer); 
                    logf_map_t_of<<"Spill start time: "<<thread_io_s_timer<<std::endl;
                    logf_map_t_of<<"Spill end time: "<<thread_io_e_timer<<std::endl;
                    logf_map_t_of<<"Spill time: "<<thread_io_time<<" seconds."<<std::endl;
                    num_spill_map_locals[tid]++;
                    svec_spillf_kv_after_map_local_mutex.lock();
                    ivec_spillf_kv_after_map_local_mutex.lock();
                    svec_spillf_kv_after_map_local.push_back(std::string(t_kv_buffer));
                    ivec_spillf_kv_after_map_local.push_back(offset_kv);
                    svec_spillf_kv_after_map_local_mutex.unlock();
                    ivec_spillf_kv_after_map_local_mutex.unlock();
    
                    kv_buffer = kv_buffer_ptrs[tid];
                    kv_buffer_offsets[tid]=0;
                    offset_kv=0;
                }
                /*thread's kv buffer can hold this additonal
                key, value pair, memcpy to kv_buffer*/
                tmp_key[keysize-1]=' ';
                memcpy(kv_buffer, tmp_key, keysize);
                kv_buffer += keysize;
                if (DEBUG){
                    logf_map_t_of<<"memcpy key: |"<<tmp_key<<"|, keysize is: "<<keysize<<std::endl;
                }
                //local_kv_buffer[offset_kv+keysize-1] = '\0';
                if (valuesize>0)
                    memcpy(kv_buffer, tmp_value, valuesize);
                kv_buffer += valuesize;
                my_done++;
                offset_kv += kv_size;//next kv where to store in the process offset


                if (DEBUG)
                {
                    logf_map_t_of<<"Copy kv "
                        <<tmp_key<<" "<<tmp_value<<" to local buffer. ";
                    logf_map_t_of<<"My share is "<<my_nele<<" My done is "
                        <<my_done<<std::endl;
                }

                
            }//end while (my_done <my_nele)

            /*spill the current kv buffer to file*/
            char t_kv_buffer[256];
            /*sprintf(t_kv_buffer,"%s/%s.%s%d.%s%d.%s%d.%s%d",spill_base.c_str(),"spill_kv_map_local",
                "p",me,"t",tid,"f", num_spill_map_locals[tid], "map",map_num);*/
            sprintf(t_kv_buffer,"%s/%s.%s%d.%s%d.%s%d.%s%d",spill_basea,"spill_kv_map_local",
                "p",me,"t",tid,"f", num_spill_map_locals[tid], "map",map_num);

            thread_io_s_timer = omp_get_wtime();
            this->spill(kv_buffer_ptrs[tid],t_kv_buffer,offset_kv);
            thread_io_e_timer = omp_get_wtime();
            thread_io_time += (thread_io_e_timer - thread_io_s_timer);
            logf_map_t_of<<"Spill start time: "<<thread_io_s_timer<<std::endl;
            logf_map_t_of<<"Spill end time: "<<thread_io_e_timer<<std::endl;
            logf_map_t_of<<"Spill time: "<<thread_io_time<<" seconds."<<std::endl;

            num_spill_map_locals[tid]++;
            svec_spillf_kv_after_map_local_mutex.lock();
            ivec_spillf_kv_after_map_local_mutex.lock();
            svec_spillf_kv_after_map_local.push_back(std::string(t_kv_buffer));
            ivec_spillf_kv_after_map_local.push_back(offset_kv);
            svec_spillf_kv_after_map_local_mutex.unlock();
            ivec_spillf_kv_after_map_local_mutex.unlock();

            kv_buffer = kv_buffer_ptrs[tid];
            kv_buffer_offsets[tid]=0;
            offset_kv=0;

            omp_atomic_time_s = omp_get_wtime();
            #pragma omp barrier//to get wait time for threads 
            omp_atomic_time_e = omp_get_wtime();
            omp_atomic_time += (omp_atomic_time_e - omp_atomic_time_s);
            omp_sync_time += omp_atomic_time;

            logf_map_t_of<<"omp barrier time: "<<std::setprecision(15)<<omp_atomic_time<<std::endl;
            logf_map_t_of.close();
        }//end pragma omp parallel

        double map_thread_e_timer = omp_get_wtime();
        double map_thread_time = map_thread_e_timer -  map_thread_s_timer;
        logf_map_p_of<<"Map thread end time: "<<map_thread_e_timer<<std::endl;
        logf_map_p_of<<"Map thread time: "<<map_thread_time<<" seconds"<<std::endl;

        if (DEBUG)
        {
            logf_map_p_of << "After the omp paralle session."<<std::endl;
            logf_map_p_of <<"The offset in buffer is: "<<kv_buffer_offsets;                    
            logf_map_p_of << std::endl;
            logf_map_p_of<<"Left is "<<left<<std::endl;
        }
        
        if (my_f_flag == 0){//I am done with their files
            if (DEBUG)
            {
                logf_map_p_of<<"Proc "<<me<<" done with all input files."<<std::endl;
            }

            delete [] input_buffer;
            delete [] ele_ptr;
            //delete [] kv_buffer1;
            //delete [] kv_buffer2;
            delete [] kv_buffer_offsets;
            delete [] num_spill_map_locals;
            delete [] thread_nele;
            delete [] thread_done;
            for (int i =0; i<N_THREADS; i++){
                delete [] kv_buffer_ptrs[i];
                
            }
            delete [] kv_buffer_ptrs;
            //close log files
            logf_map_p_of <<"Map: exit map function.\n" <<std::endl;
            break;
        }
    }//end while 1

    double map_e_timer = omp_get_wtime();
    double map_time = map_e_timer - map_s_timer;
    logf_map_p_of<<"Map_local end time: "<<map_e_timer<<std::endl;
    logf_map_p_of<<"Map_local time: "<<map_time <<" seconds. "<<std::endl;

    return 0;

}//end of map


void MapReduce::wait_previous_ialltoallv_switch_buffer(uint32_t map_num_exchange, bool *switch_flag)
{

    //if (DEBUG)
    //{
    //    logf_map_p_of<<"Enter wait previous ialltoallv and switch buffer."
    //        <<"The map num exchange flag is: "<<map_num_exchange
    //        <<std::endl;
    //}


    if (offset == offset1){
    //currently using kv buffer1, this is either the first exchange, or 
    //previously already exchanged kv buffer2, now using kvbuffer 1, need to switch to kvbuffer2
        if (map_num_exchange>1){
            //this is not first exchage, wait, flip
            //if (DEBUG)
            //{
            //    logf_map_p_of<<"map_fist_exchange = 0, i am using kv buffer 1, "
            //        <<"wait for kv buffer 2."<<std::endl;
            //}
            //mpi_wait_s = omp_get_wtime();
            MPI_Wait(&map_exchange_request[1], &map_exchange_status[1]);//for buffer2
            //mpi_wait_e = omp_get_wtime();
            //mpi_itime += (mpi_wait_e - mpi_wait_s);
            //logf_map_p_of<<"MPI_wait start: "<<mpi_wait_s<<std::endl
            //    <<"MPI_wait end: "<<mpi_wait_e<<std::endl;
            offset = offset2;
            kv_buffer = kv_buffer2;
        

            char p_kv_before_convert[256];
            sprintf(p_kv_before_convert,"%s/%s.%d.%d.%d",spill_base.c_str(),"spill_kv_after_all2all_kvb2", me, spill_num_kv_after_all2all, map_num);
            data->addblock(recvbuf2, recvbuf_size2);
            //this->spill(recvbuf2,p_kv_before_convert,recvbuf_size2);
            spill_num_kv_after_all2all++;
            svec_spillf_kv_after_all2all.push_back(std::string(p_kv_before_convert));
            ivec_spillf_kv_after_all2all.push_back(recvbuf_size2);

            //if (recvbuf2)
            //{
                delete [] recvbuf2;
             //   recvbuf2 = NULL;
            //}
            
            

            int i =0;
            for (i = 0; i < nprocs; i++){
                OPA_store_int(&offset2[i], ((kv_buffer_size/nprocs) * i));
            }

            //if (DEBUG)
            //{

            //    char kv_file_name[MAXLINE];
            //    debug_num_kv_buffer++;
            //    sprintf(kv_file_name,"%s/output_map_kv_buffer2_%d_%d", 
            //        out_base.c_str(), me, debug_num_kv_buffer);
            //    write_output_files(kv_file_name, kv_buffer2, kv_buffer_size);

            //    logf_map_p_of<<"Reset offset2 to "<<std::endl;
            //    int tmp;
            //    for (i = 0; i<nprocs; i++)
            //    {
            //        tmp = OPA_load_int(&offset2[i]);
            //        logf_map_p_of<<tmp<<" ";
            //    }
            //    logf_map_p_of<<std::endl;

            //    logf_map_p_of<<"Finished wait for previous ialltoallv, map_first_exchange  = 0."
            //        <<std::endl;
            //}
           // printf("P%d, offset = offset1, printing kv_after_all2all1 and buffer2.\n", me);
        }else{//map_nun_exchang=1, this is the firs exchange
            //this is first exchange, no wait, flip
            offset = offset2;
            kv_buffer = kv_buffer2;
        }

        //this is the first map exchange, no wait, just flip the buffer
        
        *switch_flag = false;

    }else{//offset = offset2
        //curertnely using offset2, not possible this is the first exchange
        if (map_num_exchange>1){
            //this is not the first exchange
            //if (DEBUG)
            //{
            //    logf_map_p_of<<"map_fist_exchange = 0, i am using kv buffer 2, "
            //        <<"wait for kv buffer 1."<<std::endl;
            //}
            //mpi_wait_s = omp_get_wtime();
            MPI_Wait(&map_exchange_request[0], &map_exchange_status[0]);//for buffer1
            //mpi_wait_e = omp_get_wtime();
            //mpi_itime += (mpi_wait_e - mpi_wait_s);
            //logf_map_p_of<<"MPI_Wait start: "<<mpi_wait_s<<std::endl
            //    <<"MPI_wait end: "<<mpi_wait_e<<std::endl;
            offset = offset1;
            kv_buffer = kv_buffer1;  
            *switch_flag = false;      

            char p_kv_before_convert[256];
            sprintf(p_kv_before_convert,"%s/%s.%d.%d.%d",spill_base.c_str(),"spill_kv_after_all2all_kvb1",
                me, spill_num_kv_after_all2all, map_num);
            data->addblock(recvbuf1, recvbuf_size1);
            //this->spill(recvbuf1,p_kv_before_convert,recvbuf_size1);
            spill_num_kv_after_all2all++;
            svec_spillf_kv_after_all2all.push_back(std::string(p_kv_before_convert));
            ivec_spillf_kv_after_all2all.push_back(recvbuf_size1);

            delete [] recvbuf1;


            int i =0;
            for (i = 0; i < nprocs; i++){
                OPA_store_int(&offset1[i], ((kv_buffer_size/nprocs) * i));
            }

            //if (DEBUG)
            //{

            //    char kv_file_name[MAXLINE];
            //    debug_num_kv_buffer++;
            //    sprintf(kv_file_name,"%s/output_map_kv_buffer1_%d_%d", 
            //        out_base.c_str(), me, debug_num_kv_buffer);
            //    write_output_files(kv_file_name, kv_buffer1, kv_buffer_size);

            //    logf_map_p_of<<"Reset offset1 to "<<std::endl;
            //    int tmp;
            //    for (i = 0; i<nprocs; i++)
            //    {
            //        tmp = OPA_load_int(&offset1[i]);
            //        logf_map_p_of<<tmp<<" ";
            //    }
            //    logf_map_p_of<<std::endl;

            //    logf_map_p_of<<"Finished wait for previous ialltoallv, map_first_exchange  = 0."
            //        <<std::endl;
            //}
           // printf("P%d, offset = offset1, printing kv_after_all2all1 and buffer2.\n", me);
        }else{
            //this is first exchange
            printf("P%d, error: using kv buffer 2 is not possible to be the first exchange.\n",me);
            exit(1);
        }

    }


    //if (DEBUG)
    //{
    //    logf_map_p_of<<"Exit wait previous ialltoallv and switch buffer."
    //        <<"Map number of exchange is "<<map_num_exchange
    //        <<std::endl;
    //}

}


void MapReduce::wait_last_ialltoallv(uint32_t map_num_exchange)
{
    //if (DEBUG)
    //{
    //    logf_map_p_of<<"Enter wait for last ialltoallv, with map number of exchange "
    //        <<map_num_exchange<<std::endl;
    //}


    if (offset == offset1){
        //wait for buffer2
        //mpi_wait_s = omp_get_wtime();
        MPI_Wait(&map_exchange_request[1], &map_exchange_status[1]);
        //mpi_wait_e = omp_get_wtime();
        //mpi_itime += (mpi_wait_e - mpi_wait_s);
        //logf_map_p_of<<"MPI wait start: "<<mpi_wait_s<<std::endl
        //    <<"MPI wait end: "<<mpi_wait_e<<std::endl;

        char p_kv_after_all2all[256];
        sprintf(p_kv_after_all2all,"%s/%s.%d.%d.%d",spill_base.c_str(),"spill_kv_after_all2all_kvb2", me, spill_num_kv_after_all2all, map_num);
        data->addblock(recvbuf2, recvbuf_size2);
        //this->spill(recvbuf2,p_kv_after_all2all,recvbuf_size2);
        spill_num_kv_after_all2all++;
        svec_spillf_kv_after_all2all.push_back(std::string(p_kv_after_all2all));
        ivec_spillf_kv_after_all2all.push_back(recvbuf_size2);

        delete [] recvbuf2;

        

       // if (DEBUG)
       // {
       //     char kv_file_name[MAXLINE];
       //     debug_num_kv_buffer++;
       //     sprintf(kv_file_name,"%s/output_map_kv_buffer2_%d_%d", 
       //         out_base.c_str(), me, debug_num_kv_buffer);
       //     write_output_files(kv_file_name, kv_buffer2, kv_buffer_size);

       //    logf_map_p_of<<"Finished wait for last ialltoallv, map number of exchange "
       //         <<map_num_exchange<<std::endl;
        //}


    }else{//if offset == offset1
        //wait for buffer1
        //mpi_wait_s = omp_get_wtime();
        MPI_Wait(&map_exchange_request[0], &map_exchange_status[0]);
        //mpi_wait_e = omp_get_wtime();
        //mpi_itime += (mpi_wait_e - mpi_wait_s);
        //logf_map_p_of<<"MPI wait start: "<<mpi_wait_s<<std::endl
        //    <<"MPI wait end: "<<mpi_wait_e<<std::endl;


        char p_kv_after_all2all[256];
        sprintf(p_kv_after_all2all,"%s/%s.%d.%d.%d",spill_base.c_str(),"spill_kv_after_all2all_kvb1", me, spill_num_kv_after_all2all, map_num);
        data->addblock(recvbuf1, recvbuf_size1);
        //this->spill(recvbuf1,p_kv_after_all2all,recvbuf_size1);
        spill_num_kv_after_all2all++;
        svec_spillf_kv_after_all2all.push_back(std::string(p_kv_after_all2all));
        ivec_spillf_kv_after_all2all.push_back(recvbuf_size1);

        delete [] recvbuf1;

        

        //if (DEBUG)
        //{
        //    char kv_file_name[MAXLINE];
        //    debug_num_kv_buffer++;
        //    sprintf(kv_file_name,"%s/output_map_kv_buffer1_%d_%d", 
        //        out_base.c_str(), me, debug_num_kv_buffer);
        //    write_output_files(kv_file_name, kv_buffer1, kv_buffer_size);

        //    logf_map_p_of<<"Finished wait for last ialltoallv, map number of exchange "
        //        <<map_num_exchange<<std::endl;
        //}
    }//end if offset ==offset1

    //if (DEBUG)
    //{
    //    logf_map_p_of<<"Exit wait for last ialltoallv, with map number of exchange "
    //        <<map_num_exchange<<std::endl;
    //}

}


/*exchang kv among procs
in -- sendcount recvcount, sdispls, rdispls for the 2 buffers
out -- none
do -- populate the sendcount recvcount sdispls rdispls
   -- ialltoallv to send/recv*/

uint64_t MapReduce::exchange_kv(int *p_map_sendcount1, int *p_map_recvcount1, int *p_map_sdispls1, int *p_map_rdispls1, int *p_map_sendcount2, int *p_map_recvcount2, int *p_map_sdispls2, int *p_map_rdispls2)
{
    //if (DEBUG)
    //{
    //    logf_map_p_of<<"Before exchange kv, map number of exchange "<<map_num_exchange<<std::endl;
    //}
    int *p_map_sendcount, *p_map_recvcount, *p_map_sdispls, *p_map_rdispls;
    char *kv_buffer_ptr = kv_buffer;//points to buffer to send in the function
    if (kv_buffer == kv_buffer1){
        //if (DEBUG)
        //{
        //    logf_map_p_of<<"Using buffer 1.map number of exchange "<<map_num_exchange<<std::endl;
        //}
        p_map_sendcount = p_map_sendcount1;
        p_map_recvcount = p_map_recvcount1;
        p_map_sdispls = p_map_sdispls1;
        p_map_rdispls = p_map_rdispls1;
    }else{
        p_map_sendcount = p_map_sendcount2;
        p_map_recvcount = p_map_recvcount2;
        p_map_sdispls = p_map_sdispls2;
        p_map_rdispls = p_map_rdispls2;
        //if (DEBUG)
        //{
        //    logf_map_p_of<<"Using buffer 2.map number of exchange "<<map_num_exchange<<std::endl;
        //}
    }


    
    int i = 0;
    for (i = 0; i < nprocs; i++){
        p_map_recvcount[i] = 0;
    }

    p_map_sendcount[0] = OPA_load_int(&offset[0]);
    for (i = 1; i < nprocs; i++){
        p_map_sendcount[i] = OPA_load_int(&offset[i]) - ((int)(kv_buffer_size/nprocs) * i);
    }
    MPI_Alltoall(p_map_sendcount, 1, MPI_INT, p_map_recvcount, 1, MPI_INT, comm);
    //if (DEBUG)
    //{
    //    logf_map_p_of<<"After sending buffer info using MPI_Alltoallv, sendcout is "
    //        <<p_map_sendcount[0]<<" "<<p_map_sendcount[1]<<" receive count is "
    //        <<p_map_recvcount[0]<<" "<<p_map_recvcount[1]<<std::endl;
    //}

    /*prepare recvbuf*/
    int recvbuf_size = 0;
    for (i = 0; i < nprocs; i++){
        recvbuf_size += p_map_recvcount[i];
        p_map_sdispls[i] = ((kv_buffer_size/nprocs) * i);
    }
    p_map_rdispls[0] = 0;
    for (i = 1; i < nprocs; i++){
        p_map_rdispls[i] = p_map_recvcount[i-1] + p_map_rdispls[i-1];
    }
    char *recvbuf;
    if (kv_buffer == kv_buffer1){
        recvbuf1 = new char[recvbuf_size];//TODO when to delete??
        recvbuf = recvbuf1;
        recvbuf_size1 = recvbuf_size;
    }else{
        recvbuf2 = new char[recvbuf_size];
        recvbuf = recvbuf2;
        recvbuf_size2 = recvbuf_size;
    }

    //MPI_Alltoallv(sendbuf, p_map_sendcount, p_map_sdispls, MPI_BYTE, recvbuf, p_map_recvcount, p_map_rdispls, MPI_BYTE, comm);
    if (kv_buffer == kv_buffer1)
    {
        //mpi_iall_s = omp_get_wtime();
        MPI_Ialltoallv(kv_buffer_ptr, p_map_sendcount, p_map_sdispls, MPI_BYTE, recvbuf, p_map_recvcount, p_map_rdispls, MPI_BYTE, comm, &map_exchange_request[0]); 
        //logf_map_p_of<<"Send count: "<<std::endl;
        //for (int tmp = 0; tmp < nprocs; tmp++){
        //    logf_map_p_of<<p_map_sendcount[tmp]<<" ";
        //} 
        //logf_map_p_of<<std::endl;
        //mpi_iall_e = omp_get_wtime();
        //mpi_itime += (mpi_iall_e - mpi_iall_s);
        //MPI_Wait(&map_exchange_request[0], &map_exchange_status[0]); 
    }else{
        //mpi_iall_s = omp_get_wtime();
        MPI_Ialltoallv(kv_buffer_ptr, p_map_sendcount, p_map_sdispls, MPI_BYTE, 
            recvbuf, p_map_recvcount, p_map_rdispls, MPI_BYTE, comm, &map_exchange_request[1]);
        //        logf_map_p_of<<"Send count: "<<std::endl;
        //for (int tmp = 0; tmp < nprocs; tmp++){
        //    logf_map_p_of<<p_map_sendcount[tmp]<<" ";
        //} 
        //logf_map_p_of<<std::endl;
        //mpi_iall_e = omp_get_wtime();
        //mpi_itime += (mpi_iall_e - mpi_iall_s);
    }
    //logf_map_p_of<<"MPI_Ialltoallv start: "<<mpi_iall_s<<std::endl
    //    <<"MPI_Ialltoallv end: "<<mpi_iall_e<<std::endl;
    
    map_num_exchange++;//one more kv buffer exchange posted


    //if (DEBUG)
    //{
    //    logf_map_p_of<<"After sending actual data, scount[0] is "<<p_map_sendcount[0]
    //       <<" scount[1] is "<<p_map_sendcount[1] <<" sdispls[0] is "<<p_map_sdispls[0]
    //        <<" sdispls[1] is "<<p_map_sdispls[1]<<std::endl;
    //    logf_map_p_of<<"After exchange kv."<<std::endl;
        /*char file_name[MAXLINE];
        debug_num_kv_buffer++;
        sprintf(file_name,"%s/output_map_kv_buffer_%d_%d", 
                out_base.c_str(), me, debug_num_kv_buffer);
        write_output_files(file_name, recvbuf, recvbuf_size);*/
    //}
    return 0;
}   


//kv_add is called in the mymap function in main,
//called omp parallel session, test to see if can 
//add this key value pair to buffer

uint64_t MapReduce::kv_add(char *key, char *value)
{

}


/*
in -- read_mode (word or line); inputfile_offset
out -- inputfile_offset; my_f_flag; num_ele; data_end
make changes -- input_buffer; ele_ptr
*/
uint64_t MapReduce::read_to_input_buffer(int read_modea, int *my_f_flaga, 
        long int *inputfile_offseta, uint64_t *num_elea, uint64_t *data_enda)
{
    if (read_modea == 1){//read word
        read_word_to_input_buffer(read_modea, my_f_flaga, inputfile_offseta,
            num_elea, data_enda);
    }else if (read_modea == 2){//read line
        read_line_to_input_buffer(read_modea, my_f_flaga, inputfile_offseta,
            num_elea, data_enda);

    }else{
        printf("P%d, read to input buffer error, unknown read mdoe.\n", me);
    }

}

uint64_t MapReduce::read_line_to_input_buffer(int read_modea, int *my_f_flaga, 
        long int *inputfile_offseta, uint64_t *num_elea, uint64_t *data_enda)
{


    if (DEBUG){
        logf_map_p_of<<"read_line_to_input_buffer: enter."<<std::endl;
    }

    int my_f_flag = *my_f_flaga;
    uint64_t num_ele = 0;
    uint64_t data_end = 0;
    long int inputfile_offset = *inputfile_offseta;
    std::string inputfile;
    char *cinputfile;

    if (my_f_flag == 1) //I am not done reading all my input
    {
        //open file for read
        char cinputfile[MAXLINE];
        if (svec_map_in_files.size() > 0)
        {
            inputfile = svec_map_in_files.back();
            strcpy(cinputfile, inputfile.c_str());
            cinputfile[inputfile.length()] = '\0';
            if(DEBUG)
            {
                logf_map_p_of<<"my_f_flag = 1, need to read file: "
                    <<inputfile << " from position: "<< inputfile_offset<<std::endl;
            }
        }else{
            printf("P%d: error: no input file to map.\n", me);
        }

        FILE *p_in_file;
        p_in_file = fopen(cinputfile, "rb");
        char tmp_word[MAXLINE];
        int c=0, word_len=0;
        int i = 0;//for update the ele_ptr
        char * line = NULL; //for getline
        size_t len = 0;
        ssize_t read;

        while (data_end < in_buffer_size)
        {
            fseek(p_in_file, inputfile_offset, SEEK_SET);
            //c = fscanf(p_in_file, "%s", tmp_word);
            read = getline(&line, &len, p_in_file);
            //read = length of line, including the "\n", we dont cp the "\n"
            //word_len = strlen(tmp_word);
            //tmp_word[word_len] = '\0';
            if (DEBUG)
            {
                logf_map_p_of<<"After getline, got:"<<line<<". The length is: "
                    <<read<<". The len is: "<<len<<std::endl;
                logf_map_p_of<<"Get |"<<line<<"| from "<<cinputfile
                    <<" at postion "<<inputfile_offset<<std::endl;
            }
            if (read != -1){//-1: end of file EOF
                if ((read+data_end)<in_buffer_size)//fit in the buffer
                {
                    memcpy(&input_buffer[data_end], line, (read-1));
                    if (DEBUG)
                    {
                        logf_map_p_of<<"Adding |"<<line<<"| to input_buffer at position "
                            <<(data_end)<<"."<<std::endl;
                    }
                    ele_ptr[i] = &input_buffer[data_end];
                    data_end += (read-1);
                    input_buffer[data_end]='\0';
                    data_end++;
                    if (DEBUG)
                    {
                        logf_map_p_of<<"data end after is:"<<data_end<<std::endl;
                    }

                    i++;
                    //data_end += (read+1);
                    num_ele++;
                    inputfile_offset = ftell(p_in_file);

                    if (DEBUG)
                    {
                        logf_map_p_of<<"Setting ftell to "<<inputfile_offset<<std::endl;
                    }

                }else{
                    fclose(p_in_file);
                    if (DEBUG)
                    {
                        logf_map_p_of<<"Break since the current input_buffer is full. "
                            <<"Input file is: "<<inputfile<<" stream positions is: "
                            <<inputfile_offset<<std::endl;
                    }
                    break;//the current input buffer if full, position is set to last one

                }
            }else{//c=EOF, need to read from next file

                fclose(p_in_file);
                svec_map_in_files.pop_back();
                if (svec_map_in_files.size()>0)//there is more file
                {
                    inputfile = svec_map_in_files.back();
                    strcpy(cinputfile, inputfile.c_str());
                    cinputfile[inputfile.length()] = '\0';
                    inputfile_offset = 0;
                    p_in_file = fopen(cinputfile, "rb");
                    if(DEBUG)
                    {
                        logf_map_p_of<<"need to read next file: "
                            <<inputfile << " from position: "<< inputfile_offset<<std::endl;
                    }
                }else{
                    //I am done
                    my_f_flag = 0;
                    if (DEBUG)
                    {
                        logf_map_p_of<<"Break since I finish all my files."<<std::endl;
                    }
                    break;

                }

            }//end if c!=EOF

        }//end while data_end <in_buffer_size (input buffer not full)
        if (line)
            free(line);

    }//end if my_f_flag == 1



    *my_f_flaga = my_f_flag;
    *num_elea = num_ele;
    *data_enda = data_end;
    *inputfile_offseta = inputfile_offset;

    if (DEBUG)
    {
        logf_map_p_of<<"read_line_to_input_buffer: exit with my_f_flag = "<<my_f_flag
            <<"num_ele = "<<num_ele<<" data_end = "<<data_end<<" inputfile_offset = "
            <<inputfile_offset<<std::endl;
        char file_name[MAXLINE];
        debug_num_input_buffer++;
        sprintf(file_name,"%s/output_map_input_buffer_%d_%d", 
                out_base.c_str(), me, debug_num_input_buffer);
        write_output_files(file_name, input_buffer, data_end);
    }

    return 0;
}

uint64_t MapReduce::read_word_to_input_buffer(int read_modea, int *my_f_flaga, 
        long int *inputfile_offseta, uint64_t *num_elea, uint64_t *data_enda)
{

    if (DEBUG){
        logf_map_p_of<<"read_word_to_input_buffer: enter."<<std::endl;
    }

    int my_f_flag = *my_f_flaga;
    uint64_t num_ele = 0;
    uint64_t data_end = 0;
    long int inputfile_offset = *inputfile_offseta;
    std::string inputfile;
    char *cinputfile;

    if (my_f_flag == 1) //I am not done reading all my input
    {
        //open file for read
        char cinputfile[MAXLINE];
        if (svec_map_in_files.size() > 0)
        {
            inputfile = svec_map_in_files.back();
            strcpy(cinputfile, inputfile.c_str());
            cinputfile[inputfile.length()] = '\0';
            if(DEBUG)
            {
                logf_map_p_of<<"my_f_flag = 1, need to read file: "
                    <<inputfile << " from position: "<< inputfile_offset<<std::endl;
            }
        }else{
            printf("P%d: error: no input file to map.\n", me);
        }

        FILE *p_in_file;
        p_in_file = fopen(cinputfile, "rb");
        char tmp_word[MAXLINE];
        int c=0, word_len=0;
        int i = 0;//for update the ele_ptr

        while (data_end < in_buffer_size)
        {
            fseek(p_in_file, inputfile_offset, SEEK_SET);
            c = fscanf(p_in_file, "%s", tmp_word);
            word_len = strlen(tmp_word);
            tmp_word[word_len] = '\0';
            if (DEBUG)
            {
                logf_map_p_of<<"Get "<<tmp_word<<" from "<<cinputfile
                    <<" at postion "<<inputfile_offset<<std::endl;
            }
            if (c != EOF){
                if ((word_len+1+data_end)<in_buffer_size)//fit in the buffer
                {
                    memcpy(&input_buffer[data_end], tmp_word, (word_len+1));
                    if (DEBUG)
                    {
                        logf_map_p_of<<"Adding "<<tmp_word<<" to input_buffer at position "
                            <<data_end<<std::endl;
                    }

                    ele_ptr[i] = &input_buffer[data_end];
                    i++;
                    data_end += (word_len+1);
                    num_ele++;
                    inputfile_offset = ftell(p_in_file);

                    if (DEBUG)
                    {
                        logf_map_p_of<<"Setting ftell to "<<inputfile_offset<<std::endl;
                    }

                }else{
                    fclose(p_in_file);
                    if (DEBUG)
                    {
                        logf_map_p_of<<"Break since the current input_buffer is full. "
                            <<"Input file is: "<<inputfile<<" stream positions is: "
                            <<inputfile_offset<<std::endl;
                    }
                    break;//the current input buffer if full, position is set to last one

                }
            }else{//c=EOF, need to read from next file

                fclose(p_in_file);
                svec_map_in_files.pop_back();
                if (svec_map_in_files.size()>0)//there is more file
                {
                    inputfile = svec_map_in_files.back();
                    strcpy(cinputfile, inputfile.c_str());
                    cinputfile[inputfile.length()] = '\0';
                    inputfile_offset = 0;
                    p_in_file = fopen(cinputfile, "rb");
                    if(DEBUG)
                    {
                        logf_map_p_of<<"need to read next file: "
                            <<inputfile << " from position: "<< inputfile_offset<<std::endl;
                    }
                }else{
                    //I am done
                    my_f_flag = 0;
                    if (DEBUG)
                    {
                        logf_map_p_of<<"Break since I finish all my files."<<std::endl;
                    }
                    break;

                }

            }//end if c!=EOF

        }//end while data_end <in_buffer_size (input buffer not full)

    }//end if my_f_flag == 1



    *my_f_flaga = my_f_flag;
    *num_elea = num_ele;
    *data_enda = data_end;
    *inputfile_offseta = inputfile_offset;

    if (DEBUG)
    {
        logf_map_p_of<<"read_word_to_input_buffer: exit with my_f_flag = "<<my_f_flag
            <<"num_ele = "<<num_ele<<" data_end = "<<data_end<<" inputfile_offset = "
            <<inputfile_offset<<std::endl;
        char file_name[MAXLINE];
        debug_num_input_buffer++;
        sprintf(file_name,"%s/output_map_input_buffer_%d_%d", 
                out_base.c_str(), me, debug_num_input_buffer);
        write_output_files(file_name, input_buffer, data_end);
    }

    return 0;
}

/*
in -- read_mode (word or line); inputfile_offset
out -- inputfile_offset; my_f_flag; num_ele; data_end
make changes -- input_buffer; ele_ptr
*/
uint64_t MapReduce::read_to_input_buffer_from_map_kvfiles(int read_modea, int *my_f_flaga, 
        long int *inputfile_offseta, uint64_t *num_elea, uint64_t *data_enda)
{

    if (DEBUG){
        logf_map_p_of<<"read_to_input_buffer_from_map_kvfiles: enter."<<std::endl;
    }

    int my_f_flag = *my_f_flaga;
    uint64_t num_ele = 0;
    uint64_t data_end = 0;
    long int inputfile_offset = *inputfile_offseta;
    std::string inputfile;
    char *cinputfile;

    if (my_f_flag == 1) //I am not done reading all my input
    {
        //open file for read
        char cinputfile[MAXLINE];
        if (svec_map_in_files.size() > 0)
        {
            inputfile = svec_map_in_files.back();
            strcpy(cinputfile, inputfile.c_str());
            cinputfile[inputfile.length()] = '\0';
            if(DEBUG)
            {
                logf_map_p_of<<"my_f_flag = 1, need to read file: "
                    <<inputfile << " from position: "<< inputfile_offset<<std::endl;
            }
        }else{
            printf("P%d: error: no input file to map.\n", me);
        }

        FILE *p_in_file;
        p_in_file = fopen(cinputfile, "rb");
        char tmp_word[MAXLINE];
        int c=0, word_len=0;
        int i = 0;//for update the ele_ptr

        while (data_end < in_buffer_size)
        {
            fseek(p_in_file, inputfile_offset, SEEK_SET);
            c = fscanf(p_in_file, "%s", tmp_word);
            word_len = strlen(tmp_word);
            tmp_word[word_len] = '\0'; //not neccessary, since already added by fscanf
            if (DEBUG)
            {
                logf_map_p_of<<"Get "<<tmp_word<<" from "<<cinputfile
                    <<" at postion "<<inputfile_offset<<std::endl;
            }
            if (c != EOF){
                if ((word_len+1+data_end)<in_buffer_size)//fit in the buffer
                {
                    memcpy(&input_buffer[data_end], tmp_word, (word_len+1));
                    if (DEBUG)
                    {
                        logf_map_p_of<<"Adding "<<tmp_word<<" to input_buffer at position "
                            <<data_end<<std::endl;
                    }

                    ele_ptr[i] = &input_buffer[data_end];
                    i++;
                    data_end += (word_len+1);
                    num_ele++;
                    //skip the following value (only if read from map/reduce output kv chunks)
                    c = fscanf(p_in_file, "%s", tmp_word);
                    inputfile_offset = ftell(p_in_file);

                    if (DEBUG)
                    {
                        logf_map_p_of<<"Setting ftell to "<<inputfile_offset<<std::endl;
                    }

                }else{
                    fclose(p_in_file);
                    if (DEBUG)
                    {
                        logf_map_p_of<<"Break since the current input_buffer is full. "
                            <<"Input file is: "<<inputfile<<" stream positions is: "
                            <<inputfile_offset<<std::endl;
                    }
                    break;//the current input buffer if full, position is set to last one

                }
            }else{//c=EOF, need to read from next file

                fclose(p_in_file);
                svec_map_in_files.pop_back();
                if (svec_map_in_files.size()>0)//there is more file
                {
                    inputfile = svec_map_in_files.back();
                    strcpy(cinputfile, inputfile.c_str());
                    cinputfile[inputfile.length()] = '\0';
                    inputfile_offset = 0;
                    p_in_file = fopen(cinputfile, "rb");
                    if(DEBUG)
                    {
                        logf_map_p_of<<"need to read next file: "
                            <<inputfile << " from position: "<< inputfile_offset<<std::endl;
                    }
                }else{
                    //I am done
                    my_f_flag = 0;
                    if (DEBUG)
                    {
                        logf_map_p_of<<"Break since I finish all my files."<<std::endl;
                    }
                    break;

                }

            }//end if c!=EOF

        }//end while data_end <in_buffer_size (input buffer not full)

    }//end if my_f_flag == 1



    *my_f_flaga = my_f_flag;
    *num_elea = num_ele;
    *data_enda = data_end;
    *inputfile_offseta = inputfile_offset;

    if (DEBUG)
    {
        logf_map_p_of<<"read_to_input_buffer: exit with my_f_flag = "<<my_f_flag
            <<"num_ele = "<<num_ele<<" data_end = "<<data_end<<" inputfile_offset = "
            <<inputfile_offset<<std::endl;
        char file_name[MAXLINE];
        debug_num_input_buffer++;
        sprintf(file_name,"%s/output_map_input_buffer_%d_%d", 
                out_base.c_str(), me, debug_num_input_buffer);
        write_output_files(file_name, input_buffer, data_end);
    }

    return 0;
}


/*
for debugging:
file name to print into
buffer 
*/
uint64_t MapReduce::write_output_files(char *file_name, char *buffer, uint64_t size)
{
    FILE *p_file = fopen(file_name,"w");
    fwrite(buffer, 1, size, p_file);
    fclose(p_file);

}

/*not for debugging, when files are large, spill to disk*/
void MapReduce::spill(char *page, char *fname, uint64_t size)
{
    /*spill the kv page onto a disk file*/
    std::ofstream spill_file;
    spill_file.open(fname, std::ofstream::app);
    spill_file.write(page, size);
    spill_file.close();

    /*store the spill file name to kv*/
    //spill_files.push_back(fname);
    //num_spill_files++;
}



uint64_t MapReduce::reduce(int i, uint32_t (*myreduce) (MapReduce *, const char *key, uint32_t keysize, const char *mv, uint32_t mvsize, char *out_kv))
{
    //if (DEBUG)
    //{
    //    logf_reduce_p_of<<"Eneter reduce."<<std::endl;
    //}
    reduce_num++;

    //double reduce_s_timer = omp_get_wtime();
    //logf_reduce_p_of<<"Reduce start time: "<<std::setprecision(15)<<reduce_s_timer<<std::endl;

    //convert each page of kv pairs to one page of kmv
    //merge all pages of kmv to kmv with unique keys
    KMV *kmv = new KMV(this);
    //for each kv file, bring it to memory and convert
    std::streampos kv_file_size;
    //char *kv_buffer;
    //TODO, current only work on one file, need to add iteration to work on all kv files
    uint64_t max_kv_size =0;
    for (int i = 0; i<ivec_spillf_kv_after_all2all.size(); i++)
    {
        if (ivec_spillf_kv_after_all2all[i] > max_kv_size)
        {
            max_kv_size=ivec_spillf_kv_after_all2all[i];
            //if (DEBUG)
            //{
            //    logf_reduce_p_of<<"max kv size is: "<<max_kv_size<<std::endl;
            //}
        }

    }//end for i=0

    uint64_t reduce_out = 0;
    reduce_out = kmv->reduce(max_kv_size, myreduce);
    //kmv->merge();


    //if (DEBUG)
    //{
    //    logf_reduce_p_of<<"Exit reduce."<<std::endl;
    //}

    //double reduce_e_timer = omp_get_wtime();
    //double reduce_time = reduce_e_timer - reduce_s_timer;
    //logf_reduce_p_of<<"Reduce end time: "<<reduce_e_timer<<std::endl;
    //logf_reduce_p_of<<"Reduce time: "<<reduce_time<< " seconds."<<std::endl;

    return reduce_out;
}
