/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef _MEMORY_H_
#define _MEMORY_H_

#include <stdio.h>
#include <stdlib.h>

#include <malloc.h>

void *mem_aligned_malloc(size_t, size_t);
void *mem_aligned_free(void*);
extern int64_t peakmem;

#endif
