/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#include <unordered_set>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ctype.h>
#include "log.h"
#include "stat.h"
#include "memory.h"
#include "globals.h"
#include "ac_config.h"

#include <malloc.h>
#ifdef BGQ
#include <spi/include/kernel/memory.h>
#endif

#if HAVE_LIBMEMKIND 
#include "hbwmalloc.h"
#endif

int64_t peakmem = 0;

std::unordered_set<void*> mcdram_ptrs;

// hint : 0 - allocate the memory in the normal memory region; 
// 	  1 - allocate the memory in the high-speed memory region
void *mem_aligned_malloc(size_t alignment, size_t size, int hint)
{
    void *ptr = NULL;

    PROFILER_RECORD_TIME_START;

    size_t align_size = (size + alignment - 1) / alignment * alignment;

// has MCDRAM interfaces
#ifdef HAVE_LIBMEMKIND
    if (USE_MCDRAM && hint == MCDRAM_ALLOCATE) {
        // allocate on MCDRAM    
        hbw_posix_memalign(&ptr, alignment, align_size);
        // if failed, allocate on DRAM
        if (ptr == NULL) {
            int ret = posix_memalign(&ptr, alignment, align_size);
            if (ptr == NULL) {
                int64_t memsize = get_mem_usage();
                LOG_ERROR("Error: malloc memory error %d (align=%ld; size=%ld; \
                           aligned_size=%ld; memsize=%ld)\n", ret, alignment, size, align_size, memsize);
                return NULL;
            }
        } else {
            LOG_PRINT(DBG_MEM, "Allocate on MCDRAM size=%ld, policy=%d, %d\n",
                      align_size, hbw_get_policy(), HBW_POLICY_BIND);
            mcdram_ptrs.insert(ptr);
        }
    } else {
        int ret = posix_memalign(&ptr, alignment, align_size);
        if (ptr == NULL) {
            int64_t memsize = get_mem_usage();
            LOG_ERROR("Error: malloc memory error %d (align=%ld; size=%ld; \
                aligned_size=%ld; memsize=%ld)\n", ret, alignment, size, align_size, memsize);
            return NULL;
        }
    }
#else
    int ret = posix_memalign(&ptr, alignment, align_size);
    if (ptr == NULL) {
        int64_t memsize = get_mem_usage();
        LOG_ERROR("Error: malloc memory error %d (align=%ld; size=%ld; \
            aligned_size=%ld; memsize=%ld)\n", ret, alignment, size, align_size, memsize);
        return NULL;
    }
#endif

    for (size_t i = 0; i < align_size; i += MEMPAGE_SIZE) {
        *((char*)ptr + i) = 0;
    }

    PROFILER_RECORD_TIME_END(TIMER_MEM_ALLOCATE);

    //if (RECORD_PEAKMEM == 1) {
    int64_t vmsize = get_mem_usage();
    if (vmsize > peakmem) peakmem = vmsize;
    //}

    return ptr;
}

void *mem_aligned_free(void *ptr)
{

#if HAVE_LIBMEMKIND
    if (mcdram_ptrs.find(ptr) != mcdram_ptrs.end()) {
        hbw_free(ptr);
        mcdram_ptrs.erase(ptr);
    } else {
        free(ptr);
    }
#else
    free(ptr);
#endif

    return NULL;
}
