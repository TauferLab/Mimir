#ifndef LOG_H
#define LOG_H

#include <mpi.h>

#define _DEBUG

#define DBG_GEN     1
#define DBG_DATA    2
#define DBG_COMM    4
#define DBG_OOC     8
#define DBG_CVT    16

#define DBG_LEVEL DBG_CVT
//#define DBG_LEVEL  DBG_COMM

#ifdef _DEBUG
#define LOG_PRINT(type, format, ...) \
  {\
    if(DBG_LEVEL & (type)){\
      printf((format), __VA_ARGS__);\
      fflush(stdout);\
    }\
  }
#else
#define LOG_PRINT(type, format, ...)
#endif

#define LOG_WARNING(format, ...) \
  {\
    fprintf(stdout, format, __VA_ARGS__);\
  }

#define LOG_ERROR(format,...) \
  {\
    fprintf(stdout, format, __VA_ARGS__);\
    MPI_Abort(MPI_COMM_WORLD, 1);\
  };

#endif
