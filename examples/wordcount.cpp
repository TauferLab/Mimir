#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <string.h>
#include <sys/stat.h>
#include <omp.h>

#include "mapreduce.h"
#include "config.h"

using namespace MAPREDUCE_NS;

#include "stat.h"

#ifndef WC_M
void fileread(MapReduce *, const char *, void *);
#else
void map(MapReduce *mr, char *word, void *ptr);
#endif

void countword(MapReduce *, char *, int,  MultiValueIterator *, void*);
void output(const char *filename, MapReduce *mr);

int me, nprocs;

int commmode=0;
int inputsize=512;
int blocksize=512;
int gbufsize=8;
int lbufsize=16;

uint64_t nword, nunique;
double t1, t2, t3;

int main(int argc, char *argv[])
{
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &provided);
  if (provided < MPI_THREAD_FUNNELED) MPI_Abort(MPI_COMM_WORLD, 1);

  //printf("test!\n");

  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  //printf("here!\n");

  if(argc < 2){
    if(me == 0) printf("Syntax: wordcount filepath\n");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  if(argc > 2){
    commmode=atoi(argv[2]);
  } 
  if(argc > 3){
    blocksize=atoi(argv[3]);
  }
  if(argc > 4){
    gbufsize=atoi(argv[4]);
  }
  if(argc > 5){
    lbufsize=atoi(argv[5]);
  }

  char *filedir=argv[1];

  // copy files
#if 1 
  char dir[100];
  sprintf(dir, "/tmp/mtmr_mpi.%d", me);

  char cmd[1024+1];
  sprintf(cmd, "mkdir %s", dir);
  system(cmd);
  sprintf(cmd, "cp -r %s %s", filedir, dir);
  system(cmd);

  filedir=dir;
#endif

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);

#if 1
  mr->set_threadbufsize(lbufsize);
  mr->set_sendbufsize(gbufsize);
  mr->set_blocksize(blocksize);
  mr->set_inputsize(inputsize);
  mr->set_maxmem(32);
  mr->set_commmode(commmode);
#endif

  mr->set_outofcore(0);

  MPI_Barrier(MPI_COMM_WORLD);

  t1 = MPI_Wtime();

  //char filename[2048+1];
  //sprintf(filename, "%s/512M.%d.txt", argv[1], me);

#ifndef WC_M
  nword = mr->map(filedir, 0, 1, fileread, NULL);
#else
  char whitespace[20] = " \n";
  //printf("filedir=%s\n", filedir);
  nword = mr->map(filedir, 0, 1, whitespace, map, NULL);
#endif

  //printf("map end!\n"); fflush(stdout);

  //mr->output();

  t2 = MPI_Wtime();

  nunique = mr->reduce(countword, 1, NULL);
  
  t3 = MPI_Wtime();

  MPI_Barrier(MPI_COMM_WORLD);

  //mr->output();

  output("mtmr.wc", mr);
 
  delete mr;

  // clear files
#if 1 
  sprintf(cmd, "rm -rf %s", dir);
  system(cmd);
#endif

  MPI_Finalize();
}

#ifndef WC_M
void fileread(MapReduce *mr, const char *fname, void *ptr){
  int tid = omp_get_thread_num();

  struct stat stbuf;
  int flag = stat(fname,&stbuf);
  if (flag < 0) {
    printf("ERROR: Could not query file size\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }
  int filesize = stbuf.st_size;

  FILE *fp = fopen(fname,"r");
  char *text = new char[filesize+1];

#if GATHER_STAT
  double t1 = omp_get_wtime();
#endif

  int nchar = fread(text,1,filesize,fp);

#if GATHER_STAT
  double t2 = omp_get_wtime();
  st.inc_timer(tid, TIMER_MAP_IO, t2-t1);
#endif

  text[nchar] = '\0';
  fclose(fp);

  char one[10]={"1"};
  char *saveptr = NULL;
  char whitespace[20] = " \n";
  char *word = strtok_r(text,whitespace,&saveptr);
  while (word) {
    int len=strlen(word)+1;
    if(len <= 8192)
      mr->add(word,len,one,2);
    word = strtok_r(NULL,whitespace,&saveptr);
  }

  delete [] text;

#if GATHER_STAT
  double t3 = omp_get_wtime();
  st.inc_timer(tid, TIMER_MAP_SEEK, t3-t2);
#endif
}
#else
void map(MapReduce *mr, char *word, void *ptr){
  int len=strlen(word)+1;
  char one[10]={"1"};

  if(len <= 8192)
    mr->add(word,len,one,2);
}
#endif

void countword(MapReduce *mr, char *key, int keysize,  MultiValueIterator *iter, void* ptr){
  uint64_t count=0;
  
  for(iter->Begin(); !iter->Done(); iter->Next()){
    count+=atoi(iter->getValue());
  }
  
  char count_str[100];
  sprintf(count_str, "%lu", count);

  //printf("add: key=%s,count_str=%s\n", key, count_str);

  mr->add(key, keysize, count_str, strlen(count_str)+1);
}

void output(const char *filename, MapReduce *mr){
  char tmp[1000];
  
  sprintf(tmp, "/scratch/rice/g/gao381/results/mtmr-mpi/wc/%s.%d.%d.%d.%d.P.%d.%d.csv", filename, lbufsize, gbufsize, blocksize, commmode, nprocs, me);
  FILE *fp = fopen(tmp, "a+");
  fprintf(fp, "%ld,%ld,%g,%g,%g\n", nword, nunique, t3-t1, t2-t1, t3-t2);
  fclose(fp);

  sprintf(tmp, "/scratch/rice/g/gao381/results/mtmr-mpi/wc/%s.%d.%d.%d.%d.T.%d.%d.csv", filename, lbufsize, gbufsize, blocksize, commmode, nprocs, me); 
  fp = fopen(tmp, "a+");
  mr->show_stat(0, fp);
  fclose(fp);
}
