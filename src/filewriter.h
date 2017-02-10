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

    bool open() {
        fp = fopen(filename.c_str(), "w");
        if (!fp) {
            LOG_ERROR("Open file %s error!\n", filename.c_str());
        }
        return true;
    }

    void close() {
        fclose(fp);
    }

    void write(BaseRecordFormat *) = 0;

  protected:
    std::string filename;
    FILE *fp;
};

}

#endif
