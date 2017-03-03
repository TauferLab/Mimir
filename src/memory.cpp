/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ctype.h>

#include "log.h"
#include "memory.h"
#include "globals.h"

#include <malloc.h>
#ifdef BGQ
#include <spi/include/kernel/memory.h>
#endif

int64_t peakmem = 0;

void *mem_aligned_malloc(size_t alignment, size_t size)
{
    void *ptr = NULL;

    size_t align_size = (size + alignment - 1) / alignment * alignment;
    int err = posix_memalign(&ptr, alignment, align_size);

    if (err != 0) {
        int64_t memsize = get_mem_usage();
        LOG_ERROR("Error: malloc memory error %d (align=%ld; size=%ld; \
            aligned_size=%ld; memsize=%ld)\n", err, alignment, size, align_size, memsize);
        return NULL;
    }

    if (RECORD_PEAKMEM == 1) {
        int64_t vmsize = get_mem_usage();
        if (vmsize > peakmem) peakmem = vmsize;
    }

    return ptr;
}

void *mem_aligned_free(void *ptr)
{
    free(ptr);

    return NULL;
}
