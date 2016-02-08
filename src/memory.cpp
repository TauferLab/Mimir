#include "log.h"
#include "memory.h"

void *mem_aligned_malloc(size_t alignment, size_t size){
  void *ptr=NULL;

  //printf("mem_aligned_malloc, alignment=%ld, size=%ld\n", alignment, size); fflush(stdout);
  
  //void *ptr = aligned_alloc(alignment, size);
  size_t align_size = (size+alignment-1)/alignment*alignment;
  if(posix_memalign(&ptr, alignment, align_size)){
    LOG_ERROR("malloc memory with alignment %ld and size %ld error!", alignment, size);
    return NULL;
  }

  //printf("ptr=%p\n", ptr); fflush(stdout);

  return ptr;
}

void *mem_aligned_free(void *ptr){
  free(ptr);
  return NULL;
}
