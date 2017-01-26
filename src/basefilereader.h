#ifndef MIMIR_BASE_FILE_READER_H
#define MIMIR_BASE_FILE_READER_H

#include "inputsplit.h"
#include "baserecordformat.h"

namespace MIMIR_NS {

class BaseFileReader{
  public:
    virtual bool open()=0;
    virtual void close()=0;
    virtual BaseRecordFormat* next()=0;
};

}

#endif
