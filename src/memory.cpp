#include "log.h"
#include "memory.h"

void *mem_aligned_malloc(size_t alignment, size_t size){
  void *ptr=NULL;

  size_t align_size = (size+alignment-1)/alignment*alignment;
  if(posix_memalign(&ptr, alignment, align_size) || !ptr){
    LOG_ERROR("Error: malloc memory with alignment %ld and size %ld error!", alignment, size);
    return NULL;
  }

  return ptr;
}

void *mem_aligned_free(void *ptr){
  free(ptr);
  return NULL;
}
