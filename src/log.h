/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef LOG_H
#define LOG_H

#include <algorithm>

#include <cstring>

#include <mpi.h>

#include "config.h"

#define _DEBUG

#define DBG_GEN 1
#define DBG_DATA 2
#define DBG_COMM 4
#define DBG_IO 8
#define DBG_MEM 16
#define DBG_VERBOSE 0x100

#define MIMIR_MAX_LOG_LEN 256

extern int mimir_world_rank;
extern int mimir_world_size;
extern char dump_buffer[MIMIR_MAX_LOG_LEN];

#define LOG_DUMP_BUFFER dump_buffer

#ifdef _DEBUG
#define LOG_PRINT(type, ...)                                                   \
    do {                                                                       \
        char _s[MIMIR_MAX_LOG_LEN] = "";                                       \
        if (DBG_LEVEL & (type)) {                                              \
            sprintf(_s, __VA_ARGS__);                                          \
            fprintf(stdout, "%d[%d] %s:%d %s",                                 \
                    mimir_world_rank, mimir_world_size, __FILE__, __LINE__, _s);   \
            fflush(stdout);                                                    \
        }                                                                      \
    } while (0)
#else
#define LOG_PRINT(type, ...)
#endif

#define LOG_WARNING(...)                                                       \
    do {                                                                       \
        char _s[MIMIR_MAX_LOG_LEN] = "";                                       \
        sprintf(_s, __VA_ARGS__);                                              \
        fprintf(stderr, "%d[%d] %s:%d %s",                                     \
                mimir_world_rank, mimir_world_size, __FILE__, __LINE__, _s);   \
        fflush(stderr);                                                        \
    } while (0)

#define LOG_ERROR(...)                                                         \
    do {                                                                       \
        char _s[MIMIR_MAX_LOG_LEN] = "";                                       \
        sprintf(_s, __VA_ARGS__);                                              \
        fprintf(stderr, "%d[%d] %s:%d %s",                                     \
                mimir_world_rank, mimir_world_size, __FILE__, __LINE__, _s);   \
        fflush(stderr);                                                        \
        MPI_Abort(MPI_COMM_WORLD, 1);                                          \
    } while (0)

#endif

#define LOG_DUMP_MEMORY(addr, length)                                          \
    do {                                                                       \
        int _len = std::min(length, MIMIR_MAX_LOG_LEN / 2 - 1);                \
        memset(dump_buffer, 0, MIMIR_MAX_LOG_LEN);                             \
        for (int i = 0; i < _len; ++i) {                                       \
            sprintf(&dump_buffer[2*i], "%02hhX", ((const unsigned char*)(addr))[i]); \
        }                                                                      \
        dump_buffer[2*(length)] = '\0';                                        \
    } while (0)

#define LOG_DUMP_MEMORY_BUF(addr, length, buf)                                 \
    do {                                                                       \
        int _len = std::min(length, MIMIR_MAX_LOG_LEN / 2 - 1);                \
        memset((buf), 0, 2*length);                                            \
        for (int i = 0; i < _len; ++i) {                                       \
            sprintf((buf) + 2 * i, "%02hhX", ((const unsigned char*)(addr))[i]); \
        }                                                                      \
        (buf)[2*(length)] = '\0';                                              \
    } while (0)
