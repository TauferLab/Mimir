#ifndef LOG_H
#define LOG_H

#define _DEBUG

#define DBG_GEN     1

#define DBG_LEVEL (DBG_GEN)

#ifdef _DEBUG
#define LOG_PRINT(type, format, ...) \
  {\
    if(DBG_LEVEL & (type)){\
      fprintf(stdout, (format), __VA_ARGS__);\
    }\
  }
#else
#define LOG_PRINT(type, format, ...)
#endif

#define LOG_WARNING(format, ...) \
  {\
    fprintf(stderr, format, __VA_ARGS__);\
  }

#define LOG_ERROR(format,...) \
  {\
    fprintf(stderr, format, __VA_ARGS__);\
    exit(1);\
  };

#endif
