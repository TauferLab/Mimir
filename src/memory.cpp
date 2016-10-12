#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ctype.h>

#include "log.h"
#include "memory.h"

#include <malloc.h>
#ifdef BGQ
#include <spi/include/kernel/memory.h>
#endif

int64_t peakmem=0;

//int64_t maxmem=0;
//int64_t curmem=0;

void record_memory_usage(){
  char procname[100], line[100];

#ifdef BGQ
  long unsigned int shared = 0;
  long unsigned int persist = 0;
  long unsigned int heapavail = 0;
  long unsigned int stackavail = 0;
  long unsigned int stacksize = 0;
  long unsigned int heap = 0;
  long unsigned int guard = 0;
  long unsigned int mmap = 0;

  Kernel_GetMemorySize(KERNEL_MEMSIZE_GUARD, &guard);
  Kernel_GetMemorySize(KERNEL_MEMSIZE_SHARED, &shared);
  Kernel_GetMemorySize(KERNEL_MEMSIZE_PERSIST, &persist);
  Kernel_GetMemorySize(KERNEL_MEMSIZE_HEAPAVAIL, &heapavail);
  Kernel_GetMemorySize(KERNEL_MEMSIZE_STACKAVAIL, &stackavail);
  Kernel_GetMemorySize(KERNEL_MEMSIZE_STACK, &stacksize);
  Kernel_GetMemorySize(KERNEL_MEMSIZE_HEAP, &heap);
  Kernel_GetMemorySize(KERNEL_MEMSIZE_MMAP, &mmap);

  if ((heap + stacksize) > peakmem) {
      peakmem = heap + stacksize;
  }

#else
  pid_t pid=getpid();

  int64_t vmpeak=0;
  sprintf(procname,"/proc/%ld/status", (long)pid);
  FILE *fp=fopen(procname,"r");
  
  while(fgets(line, 100, fp)){
    if(strncmp(line, "VmPeak:", 7) == 0){
      //printf("line=%s\n", line);
      char *p = line + 7;
      while(isspace(*p)) ++p;
      vmpeak=strtoull(p, NULL, 0);
    }
    //if(strncmp(line, "VmSize:", 7) == 0){
    //  char *p = line + 7;
    //  while(isspace(*p)) ++p;
    //  vmsize=strtoull(p, NULL, 0); 
    //}
  }

  fclose(fp);
  if(vmpeak>peakmem) peakmem=vmpeak;
#endif

#if 0
  struct mallinfo mi = mallinfo();
  int64_t vmpeak = (int64_t)mi.arena + (int64_t)mi.hblkhd+mi.usmblks + (int64_t)mi.uordblks+mi.fsmblks + (int64_t)mi.fordblks;
  if(vmsize>peakmem) peakmem=vmsize;
#endif


  //printf("%s: %ld %ld\n", str, vmpeak, vmsize);
}

void *mem_aligned_malloc(size_t alignment, size_t size){
  void *ptr=NULL;

  size_t align_size = (size+alignment-1)/alignment*alignment;
  //posix_memalign(&ptr, alignment, align_size);
  ptr=malloc(align_size);
  if(!ptr){
    LOG_ERROR("Error: malloc memory with alignment %ld and size %ld error!, peakmem=%ld\n", alignment, size, peakmem);
    return NULL;
  }

  record_memory_usage();

  //curmem += size;
  //if(curmem > maxmem) maxmem = curmem;
 
  return ptr;
}

void *mem_aligned_free(void *ptr){
  free(ptr);
  record_memory_usage();

  return NULL;
}
