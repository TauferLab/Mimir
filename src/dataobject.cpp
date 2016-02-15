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


int DataObject::oid = 0;

void DataObject::addRef(DataObject *data){
  if(data)
    data->ref++;
}

void DataObject::subRef(DataObject *data){
  if(data){
    data->ref--;
    if(data->ref==0){
      //printf("data %d is deleted!\n", data->id); fflush(stdout);
      delete data;
    }
  }
}


// FIXME: the memory size may exceed the range of one integer number?
DataObject::DataObject(
  DataType _datatype,
  int _blocksize,
  int _maxblock,
  int _maxmemsize,
  int _outofcore,
  std::string _filepath,
  int _threadsafe){

  datatype = _datatype;
 
  blocksize = _blocksize * UNIT_SIZE;
  maxblock = _maxblock;
  maxmemsize = _maxmemsize * UNIT_SIZE;
  outofcore = _outofcore;
  threadsafe = _threadsafe;
  filepath = _filepath;

  maxbuf = _maxmemsize / _blocksize;

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

  id = DataObject::oid++;

  printf("CreateObject: id=%d, type=%d\n", id, datatype);

  omp_init_lock(&lock_t);

  mem_bytes=0;

  LOG_PRINT(DBG_DATA, "DATA: DataObject create. (type=%d)\n", datatype);
 }

DataObject::~DataObject(){
  omp_destroy_lock(&lock_t);

  if(outofcore){
    for(int i = 0; i < nblock; i++){
      std::string filename;
      getfilename(i, filename);
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

void DataObject::getfilename(int blockid, std::string &fname){
  char str[MAXLINE+1];

  // FIXME: should we use user-defined communicator?
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if(datatype==ByteType) sprintf(str,"BT");
  else if(datatype==KVType) sprintf(str,"KV");
  else if(datatype==KMVType) sprintf(str, "KMV");
  else sprintf(str, "UN");

  sprintf(str, "%s.%d.%d.%d", str, rank, id, blockid);
  fname = str;
}

#if 0
/*
 * find a buffer which can bu used
 */
int DataObject::acquirebuffer(int blockid){
  int tid = omp_get_thread_num(); 

  //printf("%d acquire buffer for block %d of object %d\n", tid, blockid, id);
 
  int i;
  for(i = 0; i < nbuf; i++){
    
    // some other threads has acquired buffer for this block
    if(blocks[blockid].bufferid != -1) return 0;

    // the buffer is empty and the
    if(buffers[i].ref == 0){
      // lock the reference
      if(__sync_bool_compare_and_swap(&buffers[i].ref, 0, -1)){
        // set buffer id
        if(__sync_bool_compare_and_swap(&blocks[blockid].bufferid, -1, i)){
          std::string filename;

          int oldid = buffers[i].blockid;
          if(oldid != -1){
            getfilename(oldid, filename);
            FILE *fp = fopen(filename.c_str(), "wb");
            if(!fp) LOG_ERROR("Error: cannot open tmp file %s\n", filename.c_str());
            fwrite(buffers[i].buf, blocksize, 1, fp);
            fclose(fp);
            blocks[oldid].bufferid = -1;
          }
          
          buffers[i].blockid = blockid;

          getfilename(blockid, filename);
          FILE *fp;
          if(blocks[blockid].datasize > 0){
            //printf("Data Object %d: read from file %s to block %d\n", id, filename.c_str(), blockid);
            fp = fopen(filename.c_str(), "rb");
            if(!fp) LOG_ERROR("Error: cannot open tmp file %s\n", filename.c_str());
            size_t ret = fread(buffers[i].buf, blocksize, 1, fp);
          }else{
            fp = fopen(filename.c_str(), "wb");
            if(!fp) LOG_ERROR("Error: cannot open tmp file %s\n", filename.c_str());
          }
          fclose(fp);

          __sync_bool_compare_and_swap(&buffers[i].ref, -1, 1);
          return 1;
        }// end if bufferid

        // some other threads has acquired buffer for this block
        __sync_bool_compare_and_swap(&buffers[i].ref, -1, 0);
        return 0;
      }// end if ref
    }// end if
  }// end for

  return -1;
}

/* 
 * acquire a block according to the blockid
 */
//#if 0
int DataObject::acquireblock(int blockid){
  // if the block will not be flushed into disk
  if(!outofcore) return 0;

  if(blockid < 0 || blockid >= nblock){
    LOG_ERROR("Error: the block id %d isn't correcti, nblock=%d!\n", blockid, nblock);
  }
  
  int tid = omp_get_thread_num();

again:
  // the block is in the memory
  int bufferid = blocks[blockid].bufferid;

  while(bufferid != -1){
    int ref = buffers[bufferid].ref;
    if(ref != -1){
      // FIXME: the buffer may be flushed into disk.
      if(__sync_bool_compare_and_swap(&buffers[bufferid].ref, ref, ref+1)){
        return 0;
      }
    }
    
    bufferid = blocks[blockid].bufferid;
  }

  // find empty buffer
  int ret = acquirebuffer(blockid);
  if(ret==1) return 0;
  if(ret==0) goto again;

  printf("blockid=%d, nblock=%d\n", blockid, nblock);

  LOG_ERROR("%s", "Error: acquire block error!\n");
  return -1;
}
#endif


//int DataObject::acquirebuffer(int blockid){
//}

int DataObject::acquireblock(int blockid){
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
          getfilename(oldid, filename);
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

        getfilename(blockid, filename);
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
void DataObject::releaseblock(int blockid){
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
int DataObject::addblock(){
  // add counter FOP
  int blockid = -1;
  if(threadsafe)
    blockid = __sync_fetch_and_add(&nblock, 1);
  else{
    blockid = nblock;
    nblock++;
    //printf("nblock=%d, maxblock=%d\n", nblock, maxblock);
  }

  //if(id==2){
  //  printf("addblock: blockid=%d, datatype=%d\n", blockid, datatype);
  //}

  if(blockid >= maxblock){
    int tid = omp_get_thread_num();
    LOG_ERROR("Error: block count is larger than max number %d, id=%d, tid=%d!\n", maxblock, id, tid);
    return -1;
  }

  //printf("blockid=%d, maxbuf=%d, outofcore=%d, threadsafe=%d\n", blockid, maxbuf, outofcore, threadsafe);

  //printf("blockid=%d, maxbuf=%d\n", blockid, maxbuf);

  // has enough buffer
  if(blockid < maxbuf){
    if(buffers[blockid].buf == NULL){
      //printf("blocksize=%d\n", blocksize);
      buffers[blockid].buf = (char*)mem_aligned_malloc(MEMPAGE_SIZE, blocksize);
       mem_bytes += blocksize;
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
    blocks[blockid].threadid = omp_get_thread_num();
    //blocks[blockid].infile = 0;
    //blocks[blockid].fileoff = 0;

    return blockid;
  }else{
    // FIXME: out of core support
    if(outofcore){
      blocks[blockid].datasize = 0;
      blocks[blockid].bufferid = -1;
      blocks[blockid].threadid = omp_get_thread_num();
      //blocks[blockid].infile = 0;
      //blocks[blockid].fileoff = 0;

      return blockid;
    }
  }

  LOG_ERROR("%s", "Error: memory size is larger than max size!\n");
  return -1;
}

/*
 * add a block with provided data
 *   data: data buffer
 *   datasize: bytes count of data
 * return block id if success, otherwise return -1;
 * can be used in multi-thread environment
 */
#if 0
int DataObject::addblock(char *data, int datasize){
  if(datasize > blocksize){
    LOG_ERROR("Error in DataObejct::addblock: the data size exceeds one block size.(datasize=%d, blocksize=%d)\n", datasize, blocksize);
    return -1;
  }

  int blockid = addblock();
  acquireblock(blockid);
  int bufferid = blocks[blockid].bufferid;
  memcpy(buffers[bufferid].buf, data, datasize);
  blocks[blockid].datasize = datasize;
  releaseblock(blockid);

  return 0;
}
#endif

/*
 * add data into the block with blockid
 *  return 0 if success, -1 is failed
 */
#if 0
int DataObject::adddata(int blockid, char *data, int datasize){
  if(datasize > blocksize){
    LOG_ERROR("Error: data size is larger than block size. (data size=%d, block size=%d)\n", datasize, blocksize);
  }
 
  // this block doesn't have enough space
  if(blocks[blockid].datasize + datasize > blocksize){
    return -1;
  }
  
  // copy data
  int bufferid = blocks[blockid].bufferid;
  memcpy(buffers[bufferid].buf+blocks[blockid].datasize, data, datasize);
  blocks[blockid].datasize += datasize;

  return 0;
}
#endif

/*
 * get pointer of bytes
 */
int DataObject::getbytes(int blockid, int offset, char **ptr){
  int bufferid = blocks[blockid].bufferid;

  if(offset > blocks[blockid].datasize){
    LOG_ERROR("%s", "Error: block offset is larger than block size!\n");
    return -1;
  }

  *ptr = buffers[bufferid].buf + offset;
  return 0;
}

/*
 * add bytes into a block
 */
int DataObject::addbytes(int blockid, char *buf, int len){

  int bufferid = blocks[blockid].bufferid;
  char *blockbuf = buffers[bufferid].buf;

  if(len + blocks[blockid].datasize > blocksize){
    LOG_ERROR("Error: added data (%d) is larger than block size (blockid=%d, tail=%ld, blocksize=%ld)!\n", len, blockid, blocks[blockid].datasize, blocksize);
    return -1;
  }

  memcpy(blockbuf+blocks[blockid].datasize, buf, len);
  blocks[blockid].datasize += len;

  return blocks[blockid].datasize;
}

/*
 * print the bytes data in this object
 */
void DataObject::print(int type, FILE *fp, int format){
  int line = 10;
  printf("nblock=%d\n", nblock);
  for(int i = 0; i < nblock; i++){
    acquireblock(i);
    fprintf(fp, "block %d, datasize=%ld:", i, blocks[i].datasize);
    for(int j=0; j < blocks[i].datasize; j++){
      if(j % line == 0) fprintf(fp, "\n");
      int bufferid = blocks[i].bufferid;
      fprintf(fp, "  %02X", buffers[bufferid].buf[j]);
    }
    fprintf(fp, "\n");
    releaseblock(i);
  }
}
