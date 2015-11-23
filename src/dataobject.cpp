#include <string.h>
#include <sys/stat.h>
#include "dataobject.h"

#include "log.h"
#include "config.h"

using namespace MAPREDUCE_NS;


int DataObject::oid = 0;

DataObject::DataObject(
  DataType _datatype,
  int _blocksize,
  int _maxblock,
  int _maxmemsize,
  int _outofcore,
  std::string _filepath){

  datatype = _datatype;

  blocksize = _blocksize * UNIT_SIZE;
  maxblock = _maxblock;
  maxmemsize = _maxmemsize * UNIT_SIZE;
  outofcore = _outofcore;
  filepath = _filepath;

  printf("maxmemsize=%d, blocksize=%d\n", maxmemsize, blocksize);

  maxbuf = _maxmemsize / _blocksize;  

  nitem = nblock = nbuf = 0;

  blocks = new Block[maxblock];
  buffers = new Buffer[maxbuf];

  for(int i = 0; i < maxblock; i++){
    blocks[i].datasize = 0;
    blocks[i].bufferid = -1;
    //blocks[i].infile   = 0;
    //blocks[i].fileoff  = 0;
  }
  for(int i = 0; i < maxbuf; i++){
    buffers[i].buf = NULL;
    buffers[i].blockid = -1;
    buffers[i].ref = 0;
  }

  id = DataObject::oid++;

  LOG_PRINT(DBG_DATA, "DATA: DataObject create. (type=%d)\n", datatype);
 }

DataObject::~DataObject(){
  for(int i = 0; i < nbuf; i++){
    if(buffers[i].buf) free(buffers[i].buf);
  }
  delete [] blocks;
  delete [] buffers;

  LOG_PRINT(DBG_DATA, "DATA: DataObject destory. (type=%d)\n", datatype);
}

void DataObject::getfilename(int blockid, std::string &fname){
  //fname=filepath+std::string(blockid);
  char str[MAXLINE+1];
  sprintf(str, "%d.%d", id, blockid);
  fname = str;
}

/*
 * find a buffer which can bu used
 */
int DataObject::acquirebuffer(int blockid){
  
  int i;
  for(i = 0; i < nbuf; i++){
    
    // some other threads has acquired buffer for this block
    if(blocks[blockid].bufferid != -1) return 0;

    //printf("i=%d, ref=%d, bufid=%d\n", i, buffers[i].ref, blocks[]);
   
    // the buffer is empty and the
    if(buffers[i].ref == 0){
      // lock the reference
      if(__sync_bool_compare_and_swap(&buffers[i].ref, 0, -1)){

        // set buffer id
        if(__sync_bool_compare_and_swap(&blocks[blockid].bufferid, -1, i)){
          std::string filename;

          printf("haha!\n");

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
int DataObject::acquireblock(int blockid){

  if(blockid < 0 || blockid >= nblock){
    LOG_ERROR("Error: the block id %d isn't correcti, nblock=%d!\n", blockid, nblock);
  }
  
  // if the block will not be flushed into disk
  if(!outofcore) return 0;

  LOG_PRINT(DBG_OOC, "Try to acquire block %d of object %d\n", blockid, id);

again:
  // the block is in the memory
  int bufferid = blocks[blockid].bufferid;

  //printf("bufferid=%d\n", bufferid);

  while(bufferid != -1){
    int ref = buffers[bufferid].ref;
    if(ref != -1){
      // FIXME: the buffer may be flushed into disk.
      if(__sync_bool_compare_and_swap(&buffers[bufferid].ref, ref, ref+1))
        return 0;
    }
    
    bufferid = blocks[blockid].bufferid;
  }

  // find empty buffer
  int ret = acquirebuffer(blockid);
  if(ret) return 0;

  if(ret==0) goto again;

  LOG_PRINT(DBG_OOC, "Failed: cquire block %d of object %d\n", blockid, id);

  LOG_ERROR("%s", "Error: acquire block error!\n");
  return -1;
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
int DataObject::getblockspace(int blockid){
  return (blocksize - blocks[blockid].datasize);
}

/*
 * get offset of block tail
 */
int DataObject::getblocktail(int blockid){
  return blocks[blockid].datasize;
}

/*
 * add an empty block and return the block id
 */
int DataObject::addblock(){
  // add counter FOP
 
  int blockid = __sync_fetch_and_add(&nblock, 1);

  //printf("blockid=%d, nblock=%d\n", blockid, nblock);

  if(blockid >= maxblock){
    LOG_ERROR("%s", "Error: block count is larger than max number!\n");
    return -1;
  }

  printf("maxbuf=%d\n", maxbuf);

  // has enough buffer
  if(blockid < maxbuf){
    if(buffers[blockid].buf == NULL){
      buffers[blockid].buf = (char*)malloc(blocksize);
      nbuf++;
    }
    
    if(!buffers[blockid].buf){
      LOG_ERROR("Error: malloc memory for data object failed (block count=%d, block size=%d)!\n", nblock, blocksize);
      return -1;
    }

    buffers[blockid].blockid = blockid;
    buffers[blockid].ref = 0;
      
    blocks[blockid].datasize = 0;
    blocks[blockid].bufferid = blockid;
    //blocks[blockid].infile = 0;
    //blocks[blockid].fileoff = 0;

    return blockid;
  }else{
    // FIXME: out of core support
    if(outofcore){
      blocks[blockid].datasize = 0;
      blocks[blockid].bufferid = -1;
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
}

/*
 * add data into the block with blockid
 *  return 0 if success, -1 is failed
 */
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
    LOG_ERROR("Error: added data (%d) is larger than block size (blockid=%d, tail=%d, blocksize=%d)!\n", len, blockid, blocks[blockid].datasize, blocksize);
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
  for(int i = 0; i < nblock; i++){
    acquireblock(i);
    fprintf(fp, "block %d, datasize=%d:", i, blocks[i].datasize);
    for(int j=0; j < blocks[i].datasize; j++){
      if(j % line == 0) fprintf(fp, "\n");
      int bufferid = blocks[i].bufferid;
      fprintf(fp, "  %02X", buffers[bufferid].buf[j]);
    }
    fprintf(fp, "\n");
    releaseblock(i);
  }
}
