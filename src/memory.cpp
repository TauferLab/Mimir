#include "log.h"
#include "memory.h"

void *mem_align_malloc(size_t alignment, size_t size){
  void *ptr=NULL;
  
  //void *ptr = aligned_alloc(alignment, size);
  if(posix_memalign(&ptr, alignment, size)){
    LOG_ERROR("malloc memory with alignment %ld and size %ld error!", alignment, size);
    return NULL;
  }

  return ptr;
}

void *mem_alignd_free(void *ptr){
  free(ptr);
  return NULL;
}
