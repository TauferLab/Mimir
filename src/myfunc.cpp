#include "myfunc.h"

void *mymalloc(size_t alignment, size_t size){
  void *ptr = aligned_alloc(alignment, size);
  return ptr;
}

void *myfree(void *ptr){
  free(ptr);
}
