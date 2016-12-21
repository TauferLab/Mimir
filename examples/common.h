#ifndef MIMIR_EXAMPLES_COMMON
#define MIMIR_EXAMPLES_COMMON

void check_envars(int rank, int size);

void output(int rank, int size, const char *prefix, const char *common);

#endif
