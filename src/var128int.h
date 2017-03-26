/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */

#ifndef MIMIR_VAR_128_INT_H
#define MIMIR_VAR_128_INT_H

int encode_varint(char *buf, uint64_t x);
uint64_t decode_varint(char *buf);

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
