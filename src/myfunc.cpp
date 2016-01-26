#include "myfunc.h"

void *mymalloc(size_t alignment, size_t size){
  void *ptr;
  //void *ptr = aligned_alloc(alignment, size);
  posix_memalign(&ptr, alignment, size);
  return ptr;
}

void *myfree(void *ptr){
  free(ptr);
  return NULL;
}
