#ifndef MIMIR_VAR_128_INT_H
#define MIMIR_VAR_128_INT_H

#include <type_traits>
#include <typeinfo>
#ifndef _MSC_VER
#   include <cxxabi.h>
#endif
#include <memory>
#include <string>
#include <cstdlib>

#include "interface.h"

int encode_varint(char *buf, uint64_t x);
uint64_t decode_varint(char *buf);

inline int text_file_repartition (const char* buffer, int bufsize, bool islast)
{
    int idx = 0;
    for (idx = 0; idx < bufsize; idx ++) {
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


#if 0
template <typename KeyType, typename ValType, int KeyLen, int ValLen>
inline void mimir_copy (Readable<KeyType,ValType,KeyLen,ValLen> *input, 
                        Writable<kEYtYPE,ValType,KeyLen,ValLen> *output, void *ptr) {
    BaseRecordFormat *record = NULL;
    while ((record = input->read()) != NULL) {
        output->write(record);
    }
}


inline int encode_varint(char *buf, uint64_t x)
{
    int n;

    n = 0;

    while (x > 127) {
        buf[n++] = (char) (0x80 | (x & 0x7F));
        x >>= 7;
    }

    buf[n++] = (char) x;
    return n;
}

inline uint64_t decode_varint(char *buf)
{
    int      shift, n;
    uint64_t x, c;

    n = 0;
    x = 0;

    for (shift = 0; shift < 64; shift += 7) {
        c = (uint64_t) buf[n++];
        x |= (c & 0x7F) << shift;
        if ((c & 0x80) == 0) {
            break;
        }
    }

    return x;
}
#endif

#endif
