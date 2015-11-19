#include <string.h>
#include <sys/stat.h>
#include "dataobject.h"

#include "log.h"
#include "config.h"

using namespace MAPREDUCE_NS;

// FIXME: out of core design may face problems
//        when running in multi-threads environment 

DataObject::DataObject(
  DataType _datatype,
  int _blocksize,
  int _maxblock,
  int _maxmemsize,
  int _outofcore,
  std::string _filename){

  datatype = _datatype;

  blocksize = _blocksize * UNIT_SIZE;
  maxblock = _maxblock;
  maxmemsize = _maxmemsize * UNIT_SIZE;
  outofcore = _outofcore;
  filename = _filename;

  maxbuf = _maxmemsize / _blocksize;  

  nitem = nblock = nbuf = 0;

  blocks = new Block[maxblock];
  buffers = new Buffer[maxbuf];

  for(int i = 0; i < maxblock; i++){
    blocks[i].datasize = 0;
    blocks[i].bufferid = -1;
    blocks[i].infile   = 0;
    blocks[i].fileoff  = 0;
  }
  for(int i = 0; i < maxbuf; i++){
    buffers[i].buf = NULL;
    buffers[i].blockid = -1;
    buffers[i].ref = 0;
  }

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

/*
 * find a buffer which can bu used
 */
int DataObject::findbuffer(){
  
  int i;
  for(i = 0; i < nbuf; i++){
    // this buffer can be 
    if(buffers[i].ref == 0){
      int blockid = buffers[i].blockid;
      if(blockid != -1){
        int64_t off;
        if(blocks[blockid].infile)
          off = blocks[blockid].fileoff;
        else{
          struct stat statbuf;
          stat(filename.c_str(), &statbuf);
          off = statbuf.st_size;
        }
        FILE *fp = fopen(filename.c_str(), "wb+");
        if(!fp){
          printf("Error: cannot open data file!\n");
          return -1;
        }
        fseek(fp, off, SEEK_SET);
        fwrite(buffers[blockid].buf, blocksize, 1, fp);
        fclose(fp);
        blocks[blockid].bufferid = -1;
        blocks[blockid].infile = 1;
        blocks[blockid].fileoff = off;

        buffers[i].blockid = -1;
        buffers[i].ref     = 0;
        return i;
      }
    }
  }

  return -1;
}

/* 
 * acquire a block according to the blockid
 */
int DataObject::acquireblock(int blockid){
  if(blockid < 0 || blockid > nblock){
    LOG_ERROR("Warning: the block id %d isn't correct!\n", blockid);
  }
  
  // if the block will not be flushed into disk
  if(!outofcore) return 0;

  // the block is in the memory
  int bufferid = blocks[blockid].bufferid;
  if(bufferid != -1){
    // FIXME: the buffer may be flushed into disk.
    buffers[bufferid].ref++;
    return 0;
  }

  // FIXME: out of core support
  // the block is in the file
  if(blocks[blockid].infile){
    // find empty buffer
    int i = findbuffer();
    // read block
    FILE *fp = fopen(filename.c_str(), "rb");
    if(!fp) return -1;
    fseek(fp, blocks[blockid].fileoff, SEEK_SET);
    size_t ret = fread(buffers[i].buf, blocksize, 1, fp);
    fclose(fp);
    buffers[i].blockid = blockid;
    buffers[i].ref = 1;
    blocks[blockid].bufferid = i;
    return 0;
  }

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
  if(bufferid != -1){
    buffers[bufferid].ref--;
  }
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

  if(blockid >= maxblock){
    LOG_ERROR("%s", "Error: block count is larger than max number!\n");
    return -1;
  }

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
    blocks[blockid].infile = 0;
    blocks[blockid].fileoff = 0;

    return blockid;
  }else{
    // FIXME: out of core support
    if(outofcore){
      int i = findbuffer();
      if(i == -1){
        printf("Error: cannot find empty buffer!\n");
        return -1;
      }
      buffers[i].blockid = blockid;
      buffers[i].ref     = 0;
      blocks[blockid].datasize = 0;
      blocks[blockid].bufferid = i;
      blocks[blockid].infile = 0;
      blocks[blockid].fileoff = 0;

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
