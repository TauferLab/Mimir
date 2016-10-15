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

int64_t get_max_mmap(){
#define BUFSIZE 1024
    int stderr_save;
    char buffer[BUFSIZE];
    fflush(stderr); //clean everything first
    stderr_save = dup(STDERR_FILENO); //save the stdout state
    freopen("/dev/null", "a", stderr); //redirect stdout to null pointer
    setvbuf(stderr, buffer, _IOFBF, BUFSIZE); //set buffer to stdout
    malloc_stats();
    freopen("/dev/null", "a", stderr); //redirect stdout to null again
    dup2(stderr_save, STDERR_FILENO); //restore the previous state of stdout
    setvbuf(stderr, NULL, _IONBF, BUFSIZE); //disable buffer to print to screen instantly
    char *p, *temp;
    int64_t maxmmap=0;
    p = strtok_r(buffer, "\n", &temp);
    do {
      if(strncmp(p, "max mmap bytes   =", 18) == 0){
        char *word = p + 18;
        while(isspace(*word)) ++word;
        maxmmap=strtoull(word, NULL, 0);
      }
      p = strtok_r(NULL, "\n", &temp);
    } while (p != NULL);
    //fprintf(out, ",maxmmap:%ld", maxmmap);
    return maxmmap;
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

int mem_alloc_init()
{
#ifdef BGQ
    /*
     * Configuration suggested by Hal Finkel:
     mallopt(M_MMAP_THRESHOLD, sysconf(_SC_PAGESIZE));                mallopt(M_MMAP_THRESHOLD, sysconf(_SC_PAGESIZE));
     mallopt(M_TRIM_THRESHOLD, 0);                    mallopt(M_TRIM_THRESHOLD, 0);
     mallopt(M_TOP_PAD, 0);                   mallopt(M_TOP_PAD, 0);
     */

    /*
     * According to the glibc 2.12.2 patch ( https://repo.anl-external.org/viewvc/bgq-driver/V1R1M1/toolchain/glibc-2.12.2.diff?revision=1&content-type=text%2Fplain&pathrev=4 ),
     * Blue Gene/Q uses these values:
     *
     * DEFAULT_TRIM_THRESHOLD is 1024 * 1024;
     * DEFAULT_MMAP_THRESHOLD is 1024 * 1024;
     * DEFAULT_TOP_PAD is 0.
     *
     * According to http://man7.org/linux/man-pages/man3/mallopt.3.html, the default value for these 3 arguments
     * is 128 * 1024 in the glibc.
     */

    mallopt(M_MMAP_THRESHOLD, 128 * 1024);
    mallopt(M_TRIM_THRESHOLD, 128 * 1024);
    mallopt(M_TOP_PAD, 128 * 1024);
#endif
    return 0;
}

void *mem_aligned_free(void *ptr){
  free(ptr);
  record_memory_usage();

  return NULL;
}
