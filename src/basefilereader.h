/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_BASE_FILE_READER_H
#define MIMIR_BASE_FILE_READER_H

#include "inputsplit.h"
#include "baserecordformat.h"

namespace MIMIR_NS {

class BaseFileReader{
  public:
    BaseFileReader() {
    }
    virtual ~BaseFileReader() {
    }
    virtual bool open()=0;
    virtual void close()=0;
    virtual BaseRecordFormat* next()=0;
};

}

#endif
