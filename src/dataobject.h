/**
 * @file   dataobject.h
 * @Author Tao Gao (taogao@udel.edu)
 * @date   September 1st, 2016
 * @brief  This file provides interfaces to handle data objects.
 *
 * Detail description.
 */
#ifndef DATA_OBJECT_H
#define DATA_OBJECT_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
//#include <string>

#include "log.h"
#include "const.h"
#include "memory.h"

namespace MIMIR_NS {

#define CHECK_PAGEID(pageid) {\
  if(pageid<0 || pageid>=npages)\
    LOG_ERROR("Error: page id (%d) is out of the range.", pageid);}

/// Datatype
enum DataType{ByteType, KVType};

class DataObject{
public:
    DataObject(int me, int nprocs,
        DataType type=ByteType,
        int64_t pagesize=1,
        int maxpages=4);

    virtual ~DataObject();

    int acquire_page(int pageid){
        CHECK_PAGEID(pageid);
        ipage=pageid;
        off=0;
        ptr=pages[pageid].buffer;
        return 0;
    }

    void release_page(int pageid){
        CHECK_PAGEID(pageid);
        return;
    }

    void delete_page(int pageid){
        CHECK_PAGEID(pageid);
        if(ref==1){        
            mem_aligned_free(pages[pageid].buffer);
            pages[pageid].buffer=NULL;
            pages[pageid].datasize=0;
        }
    }

    int add_page();

    int get_npages(){
        return npages;
    }

    int64_t get_page_size(int pageid){
        CHECK_PAGEID(pageid);
        return pages[pageid].datasize;
    }

    void set_page_size(int pageid, int64_t datasize){
        CHECK_PAGEID(pageid);
        pages[pageid].datasize=datasize;
    }

    char *get_page_buffer(int pageid){
        CHECK_PAGEID(pageid);
        return pages[pageid].buffer;
    }

    int64_t get_total_size(){
        return totalsize;
    }

    virtual void print(int type = 0, FILE *fp=stdout, int format=0);

public:
    int64_t pagesize;      ///< page size

protected:
    int me, nprocs;

    int id,ref;

    int64_t totalsize;    ///< datasize

    DataType datatype;    ///< data type
    int npages,maxpages;  ///< number of page

    int      ipage;        ///< index of current page
    char    *ptr;
    int64_t  off;          ///< current offset

    struct Page{
        int64_t   datasize;
        char     *buffer;
    }*pages;

public:
    static int object_id;
    static int64_t mem_bytes;

    //static int max_page_count;
    static void addRef(DataObject *);
    static void subRef(DataObject *);
};

}

#endif
