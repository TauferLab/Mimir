/**
 * @file   dataobject.cpp
 * @Author Tao Gao (taogao@udel.edu)
 * @date   September 1st, 2016
 * @brief  This file provides implementation to handle data objects.
 *
 * Detail description.
 */
#include <string.h>
#include <sys/stat.h>
#include "dataobject.h"
#include <mpi.h>
#include "log.h"
#include "config.h"
#include "const.h"
#include "memory.h"
#include "mapreduce.h"

using namespace MIMIR_NS;


int DataObject::object_id = 0;
int DataObject::cur_page_count=0;
int DataObject::max_page_count=0;

void DataObject::addRef(DataObject *data){
  if(data)
    data->ref++;
}

void DataObject::subRef(DataObject *data){
  if(data){
    data->ref--;
    if(data->ref==0){
      delete data;
      data = NULL;
    }
  }
}

DataObject::DataObject(int _me, int _nprocs, 
    DataType _datatype,
    int64_t _pagesize,
    int _maxpages){
    me=_me;
    nprocs=_nprocs;
    datatype=_datatype;
    pagesize=_pagesize;
    maxpages=_maxpages;

    id=ref=0; 

    npages=0;

    totalsize=0; 

    pages = (Page*)mem_aligned_malloc(MEMPAGE_SIZE,maxpages*sizeof(Page));

    for(int i = 0; i < maxpages; i++){
        pages[i].datasize = 0;
        pages[i].buffer = NULL;
    }

    ipage=-1;

    id = DataObject::object_id++;

    LOG_PRINT(DBG_DATA, me, nprocs, "DATA: DataObject %d create. (maxpages=%d)\n", id, maxpages);

}

DataObject::~DataObject(){
    for(int i = 0; i < npages; i++){
        if(pages[i].buffer != NULL) mem_aligned_free(pages[i].buffer);
    }
    mem_aligned_free(pages);
    DataObject::cur_page_count-=npages;

    LOG_PRINT(DBG_DATA, me, nprocs, "DATA: DataObject %d destory.\n", id);
}

int DataObject::add_page(){
    int pageid=npages;
    npages++;
    if(npages>maxpages) 
       LOG_ERROR("Error: page count (%d) is larger than than maximum (%d).", npages, maxpages);
    pages[pageid].datasize=0;
    pages[pageid].buffer=(char*)mem_aligned_malloc(MEMPAGE_SIZE, pagesize);

    ipage = pageid;

    LOG_PRINT(DBG_DATA, me, nprocs, "DATA: DataObject %d add one page %d.\n", id, pageid);

    return pageid;
}

/*
 * print the bytes data in this object
 */
void DataObject::print(int type, FILE *fp, int format){
  int line = 10;
  printf("nblock=%d\n", npages);
  for(int i = 0; i < npages; i++){
    acquire_page(i);
#if 0
    fprintf(fp, "block %d, datasize=%ld:", i, blocks[i].datasize);
    for(int j=0; j < blocks[i].datasize; j++){
      if(j % line == 0) fprintf(fp, "\n");
      int bufferid = blocks[i].bufferid;
      fprintf(fp, "  %02X", buffers[bufferid].buf[j]);
    }
#endif
    fprintf(fp, "\n");
    release_page(i);
  }
}
