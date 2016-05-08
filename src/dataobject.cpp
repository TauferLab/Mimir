#include <string.h>
#include <sys/stat.h>
#include "dataobject.h"
#include <mpi.h>
#include <omp.h>


#include "log.h"
#include "config.h"

#include "const.h"

#include "memory.h"

using namespace MAPREDUCE_NS;


int DataObject::object_id = 0;

void DataObject::addRef(DataObject *data){
  if(data)
    data->ref++;
}

void DataObject::subRef(DataObject *data){
  if(data){
    data->ref--;
    if(data->ref==0){
      delete data;
    }
  }
}

DataObject::DataObject(
  DataType _datatype,
  int _blocksize,
  int _maxblock,
  int _maxmemsize,
  int _outofcore,
  std::string _filepath,
  int _threadsafe){

  datatype = _datatype;
 
  blocksize = (uint64_t)_blocksize * UNIT_1M_SIZE;
  maxblock = _maxblock;

  //printf("maxmemsize=%d\n", _maxmemsize);

  maxmemsize = (uint64_t)_maxmemsize * UNIT_1G_SIZE;
  outofcore = _outofcore;
  threadsafe = _threadsafe;
  filepath = _filepath;

  maxbuf = (int)(maxmemsize / blocksize);
  //printf("maxmemsize=%ld, blocksize=%ld, maxbuf=%d\n", maxmemsize, blocksize, maxbuf);

  nitem = nblock = nbuf = 0;

  blocks = new Block[maxblock];
  buffers = new Buffer[maxbuf];

  for(int i = 0; i < maxblock; i++){
    blocks[i].datasize = 0;
    blocks[i].bufferid = -1;
  }
  for(int i = 0; i < maxbuf; i++){
    buffers[i].buf = NULL;
    buffers[i].blockid = -1;
    buffers[i].ref = 0;
  }

  ref=0;

  id = DataObject::object_id++;

  omp_init_lock(&lock_t);

  LOG_PRINT(DBG_DATA, "DATA: DataObject create. (type=%d)\n", datatype);
 }

DataObject::~DataObject(){
  omp_destroy_lock(&lock_t);

  if(outofcore){
    for(int i = 0; i < nblock; i++){
      std::string filename;
      _get_filename(i, filename);
      remove(filename.c_str());
    }
  }

  for(int i = 0; i < nbuf; i++){
    if(buffers[i].buf) mem_aligned_free(buffers[i].buf);
  }
  delete [] buffers;
  delete [] blocks;

  LOG_PRINT(DBG_DATA, "DATA: DataObject destory. (type=%d)\n", datatype);
}

void DataObject::_get_filename(int blockid, std::string &fname){
  char str[MAXLINE+1];

  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if(datatype==ByteType) sprintf(str,"MTMR.BT");
  else if(datatype==KVType) sprintf(str,"MTMR.KV");
  else if(datatype==KMVType) sprintf(str, "MTMR.KMV");
  else sprintf(str, "MTMR.UN");

  sprintf(str, "%s.%d.%d.%d", str, rank, id, blockid);
  fname = str;
}

int DataObject::acquire_block(int blockid){
  if(!outofcore) return 0;

  if(threadsafe) omp_set_lock(&lock_t);
  int bufferid = blocks[blockid].bufferid;
  if(bufferid == -1){
    int i;
    for(i = 0; i < nbuf; i++){
      // the buffer is empty and the
      if(buffers[i].ref == 0){
        std::string filename;

        int oldid = buffers[i].blockid;
        if(oldid != -1){
          _get_filename(oldid, filename);
          FILE *fp = fopen(filename.c_str(), "wb");
          if(!fp) LOG_ERROR("Error: cannot open tmp file %s\n", filename.c_str());
          fwrite(buffers[i].buf, blocksize, 1, fp);
          fclose(fp);
          blocks[oldid].bufferid = -1;
        }

        bufferid=i;
        buffers[i].blockid = blockid;
        buffers[i].ref     = 0;
        blocks[blockid].bufferid=bufferid;

        _get_filename(blockid, filename);
        FILE *fp;
        if(blocks[blockid].datasize > 0){
          fp = fopen(filename.c_str(), "rb");
          if(!fp) LOG_ERROR("Error: cannot open tmp file %s\n", filename.c_str());
          size_t ret = fread(buffers[i].buf, blocksize, 1, fp);
        }
        else{
          fp = fopen(filename.c_str(), "wb");
          if(!fp) LOG_ERROR("Error: cannot open tmp file %s\n", filename.c_str());
        }
        fclose(fp);
        break;
      }// end if
    }// end for
    if(i >= nbuf) LOG_ERROR("%s", "Cannot find an empty buffer!\n");
  }
  buffers[bufferid].ref++;
  //omp_unset_lock(&lock_t);
  if(threadsafe) omp_unset_lock(&lock_t);

  return 0;
}

/*
 * release a block according to block id
 */
void DataObject::release_block(int blockid){
  if(!outofcore) return;

  // FIXME: out of core support
  int bufferid = blocks[blockid].bufferid;
  if(bufferid==-1)
    LOG_ERROR("%s", "Error: aquired block should have buffer!\n");

  __sync_fetch_and_add(&buffers[bufferid].ref, -1);
}

/*
 * get block empty space
 */

/*
 * add an empty block and return the block id
 */
int DataObject::add_block(){
  // add counter FOP
  int blockid = -1;
  if(threadsafe)
    blockid = __sync_fetch_and_add(&nblock, 1);
  else{
    blockid = nblock;
    nblock++;
  }


  if(blockid >= maxblock){
    int tid = omp_get_thread_num();
    LOG_ERROR("Error: block count is larger than max number %d, id=%d, tid=%d!\n", maxblock, id, tid);
    return -1;
  }

  printf("id=%d, blockid=%d\n",\
    id, blockid); fflush(stdout);

  //printf("blockid=%d, maxbuf=%d, outofcore=%d, threadsafe=%d\n", blockid, maxbuf, outofcore, threadsafe);

  //printf("blockid=%d, maxbuf=%d\n", blockid, maxbuf);

  //printf("blockid=%d, maxbuf=%d\n", blockid, maxbuf);

  // has enough buffer
  if(blockid < maxbuf){
    if(buffers[blockid].buf == NULL){
      //printf("blocksize=%d\n", blocksize);
      buffers[blockid].buf = (char*)mem_aligned_malloc(MEMPAGE_SIZE, blocksize);
       //mem_bytes += blocksize;
      //printf("block pass\n");
#if SAFE_CHECK
     if(buffers[blockid].buf==NULL){
       LOG_ERROR("%s", "Error: malloc memory for data object error!\n");
     }
#endif
      nbuf++;
    }
    
    if(!buffers[blockid].buf){
      LOG_ERROR("Error: malloc memory for data object failed (block count=%d, block size=%ld)!\n", nblock, blocksize);
      return -1;
    }

    buffers[blockid].blockid = blockid;
    buffers[blockid].ref = 0;
      
    blocks[blockid].datasize = 0;
    blocks[blockid].bufferid = blockid;
    //blocks[blockid].threadid = omp_get_thread_num();
    //blocks[blockid].infile = 0;
    //blocks[blockid].fileoff = 0;

    return blockid;
  }else{
    // FIXME: out of core support
    if(outofcore){
      blocks[blockid].datasize = 0;
      blocks[blockid].bufferid = -1;
      //blocks[blockid].threadid = omp_get_thread_num();
      //blocks[blockid].infile = 0;
      //blocks[blockid].fileoff = 0;

      return blockid;
    }
  }

  LOG_ERROR("%s", "Error: memory size is larger than max size!\n");
  return -1;
}

/*
 * print the bytes data in this object
 */
void DataObject::print(int type, FILE *fp, int format){
  int line = 10;
  printf("nblock=%d\n", nblock);
  for(int i = 0; i < nblock; i++){
    acquire_block(i);
    fprintf(fp, "block %d, datasize=%ld:", i, blocks[i].datasize);
    for(int j=0; j < blocks[i].datasize; j++){
      if(j % line == 0) fprintf(fp, "\n");
      int bufferid = blocks[i].bufferid;
      fprintf(fp, "  %02X", buffers[bufferid].buf[j]);
    }
    fprintf(fp, "\n");
    release_block(i);
  }
}
