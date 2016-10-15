#ifndef _MEMORY_H_
#define _MEMORY_H_

#include <stdio.h>
#include <stdlib.h>

#include <malloc.h>

void *mem_aligned_malloc(size_t, size_t);
void *mem_aligned_free(void *);
int mem_alloc_init();

void record_memory_usage(const char *);

int64_t get_max_mmap();

extern int64_t peakmem;

#endif
