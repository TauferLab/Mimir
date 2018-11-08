//
// (c) 2017 by University of Delaware, Argonne National Laboratory, San Diego
//     Supercomputer Center, National University of Defense Technology,
//     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
//
//     See COPYRIGHT in top-level directory.
//

#ifndef MIMIR_VAR_128_INT_H
#define MIMIR_VAR_128_INT_H

#include <type_traits>
#include <typeinfo>
#ifndef _MSC_VER
#include <cxxabi.h>
#endif
#include <memory>
#include <string>
#include <cstdlib>

#include "interface.h"

int encode_varint(char *buf, uint64_t x);
uint64_t decode_varint(char *buf);

inline int text_file_repartition(const char *buffer, int bufsize, bool islast)
{
    int idx = 0;
    for (idx = 0; idx < bufsize; idx++) {
        if (buffer[idx] == '\n') {
            idx = idx + 1;
            break;
        }
    }

    if (idx == bufsize && islast) {
        return bufsize;
    }

    return idx;
}

#endif
