//
// (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego
//     Supercomputer Center, National University of Defense Technology,
//     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
//
//     See COPYRIGHT in top-level directory.
//

#ifndef MIMIR_FILE_PARSER_H
#define MIMIR_FILE_PARSER_H

namespace MIMIR_NS {

class FileParser
{
  public:
    FileParser() {}

    ~FileParser() {}

    int to_line(char *buffer, int len, bool islast)
    {
        if (len == 0) return -1;

        int i = 0;
        for (i = 0; i < len; i++) {
            if (*(buffer + i) == '\n') break;
        }

        if (i < len) {
            buffer[i] = '\0';
            return i + 1;
        }

        if (islast) {
            buffer[len] = '\0';
            return len + 1;
        }

        return -1;
    }
};

} // namespace MIMIR_NS

#endif
