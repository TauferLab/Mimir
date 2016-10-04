#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ctype.h>

#include "log.h"
#include "memory.h"


int64_t peakmem=0;

//int64_t maxmem=0;
//int64_t curmem=0;

void record_memory_usage(){
#if 1
  char procname[100], line[100];

  pid_t pid=getpid();

  int64_t vmpeak,vmsize;
  sprintf(procname,"/proc/%ld/status", (long)pid);
  FILE *fp=fopen(procname,"r");
  
  while(fgets(line, 100, fp)){
    if(strncmp(line, "VmPeak:", 7) == 0){
      //printf("line=%s\n", line);
      char *p = line + 7;
      while(isspace(*p)) ++p;
      vmpeak=strtoull(p, NULL, 0);
    }
    if(strncmp(line, "VmSize:", 7) == 0){
      char *p = line + 7;
      while(isspace(*p)) ++p;
      vmsize=strtoull(p, NULL, 0); 
    }
  }

  fclose(fp);
#endif

#if 0
  struct mallinfo mi = mallinfo();
  int64_t vmsize = (int64_t)mi.arena + (int64_t)mi.hblkhd+mi.usmblks + (int64_t)mi.uordblks+mi.fsmblks + (int64_t)mi.fordblks;
#endif

  if(vmsize>peakmem) peakmem=vmsize;

  //printf("%s: %ld %ld\n", str, vmpeak, vmsize);
}

void *mem_aligned_malloc(size_t alignment, size_t size){
  void *ptr=NULL;

  size_t align_size = (size+alignment-1)/alignment*alignment;
  //posix_memalign(&ptr, alignment, align_size);
  ptr=malloc(align_size);
  if(!ptr){
    LOG_ERROR("Error: malloc memory with alignment %ld and size %ld error!\n", alignment, size);
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
