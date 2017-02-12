#ifndef MIMIR_BASE_FILE_WRITER_H
#define MIMIR_BASE_FILE_WRITER_H

#include <stdio.h>
#include <stdlib.h>

#include <string>
#include <sstream>

#include "log.h"
#include "interface.h"
#include "globals.h"

namespace MIMIR_NS {

class BaseFileWriter : public Writable {
  public:
    BaseFileWriter(const char *filename) {
        std::ostringstream oss;
        oss << mimir_world_size << "." << mimir_world_rank;
        this->filename = filename + oss.str();
    }

    virtual bool open() {
        fp = fopen(filename.c_str(), "w");
        if (!fp) {
            LOG_ERROR("Open file %s error!\n", filename.c_str());
        }
	record_count = 0;
        LOG_PRINT(DBG_IO, "Open output file %s.\n", filename.c_str());	
	return true;
    }

    virtual void close() {
        LOG_PRINT(DBG_IO, "Close output file %s.\n", filename.c_str());	
    	fclose(fp);
    }

    virtual void write(BaseRecordFormat *) {
	 record_count++;
    }

    virtual uint64_t get_record_count() { return record_count; }

  protected:
    std::string filename;
    FILE *fp;
    uint64_t record_count;
};

}

#endif
