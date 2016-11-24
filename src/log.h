#ifndef LOG_H
#define LOG_H

#include <mpi.h>

#include "config.h"

#define _DEBUG

#define DBG_GEN     1
#define DBG_DATA    2
#define DBG_COMM    4
#define DBG_IO      8
#define DBG_MEM    16

#ifdef _DEBUG
#define LOG_PRINT(type, rank, size, format, ...) \
  {\
    if(DBG_LEVEL & (type)){\
      fprintf(stdout, "%d[%d] ", rank, size);\
      fprintf(stdout, (format), __VA_ARGS__);\
      fflush(stdout);\
    }\
  };
#else
#define LOG_PRINT(type, format, ...)
#endif

#define LOG_WARNING(rank, size, format, ...) \
  {\
    fprintf(stderr, "%d[%d] ", rank, size);\
    fprintf(stdout, format, __VA_ARGS__);\
  };

#define LOG_ERROR(format,...) \
  {\
    fprintf(stderr, format, __VA_ARGS__);\
    MPI_Abort(MPI_COMM_WORLD, 1);\
  };

#endif
