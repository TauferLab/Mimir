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

void map(MapReduce *mr, char *word, void *ptr);
void countword(MapReduce *, char *, int,  MultiValueIterator *, int, void*);

#define USE_LOCAL_DISK  0
//#define OUTPUT_KV

//#define PART_REDUCE

void output(const char *filename, const char *outdir, \
  const char *prefix, MapReduce *mr);

//#define PPN 2
int me, nprocs;
int nbucket=17, estimate=0, factor=32;
const char* inputsize="512M";
const char* blocksize="64M";
const char* gbufsize="64M";
const char* lbufsize="4K";
const char* commmode="a2a";

int main(int argc, char *argv[])
{
#ifdef MTMR_MULTITHREAD
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &provided);
  if (provided < MPI_THREAD_FUNNELED){
    fprintf(stderr, "MPI don't support multithread!");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
#else
  MPI_Init(&argc, &argv);
#endif

  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  if(argc <= 3){
    if(me == 0) printf("Syntax: wordcount filepath prefix outdir\n");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  char *filedir=argv[1];
  const char *prefix = argv[2];
  const char *outdir = argv[3];

  if(me==0){
    printf("input dir=%s\n", filedir);
    printf("prefix=%s\n", prefix);
    printf("output dir=%s\n", outdir);
  }

  // copy files
#if USE_LOCAL_DISK
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
  mr->set_commmode(commmode);
  mr->set_nbucket(estimate,nbucket,factor);
  mr->set_maxmem(32);
#endif

  mr->set_outofcore(0);

#ifdef KV_HINT
  mr->set_KVtype(StringKV);
#endif

  MPI_Barrier(MPI_COMM_WORLD);

  //double t1 = MPI_Wtime();

  char whitespace[20] = " \n";
  mr->map_text_file(filedir, 1, 1, whitespace, map, NULL);

  //double t2 = MPI_Wtime();

#ifdef PART_REDUCE
  mr->reduce(countword, 1, NULL);
#else
  mr->reduce(countword, 0, NULL);
#endif
  //double t3 = MPI_Wtime();

  MPI_Barrier(MPI_COMM_WORLD);

#ifdef OUTPUT_KV
  char tmpfile[100];
  sprintf(tmpfile, "results%d.txt", me);
  FILE *outfile=fopen(tmpfile, "w");
  mr->output(0, outfile);
  fclose(outfile);
#endif

  output("mtmr.wc", outdir, prefix, mr);
 
  delete mr;

  // clear files
#if USE_LOCAL_DISK 
  sprintf(cmd, "rm -rf %s", dir);
  system(cmd);
#endif

  MPI_Finalize();
}

void map(MapReduce *mr, char *word, void *ptr){
  int len=strlen(word)+1;
  char one[10]={"1"};

  if(len <= 8192)
    mr->add_key_value(word,len,one,2);
}

void countword(MapReduce *mr, char *key, int keysize,  MultiValueIterator *iter, int lastreduce, void* ptr){
  uint64_t count=0;
  
  for(iter->Begin(); !iter->Done(); iter->Next()){
    count+=atoi(iter->getValue());
  }
  
  char count_str[100];
  sprintf(count_str, "%lu", count);
  mr->add_key_value(key, keysize, count_str, strlen(count_str)+1);
}

void output(const char *filename, const char *outdir, const char *prefix, MapReduce *mr){
  char header[1000];
  char tmp[1000];

  if(estimate)
    sprintf(header, "%s/%s_c%s-b%s-i%s-f%d-%s.%d", \
      outdir, prefix, gbufsize, blocksize, inputsize, factor, commmode, nprocs); 
  else
    sprintf(header, "%s/%s_c%s-b%s-i%s-h%d-%s.%d", \
      outdir, prefix, gbufsize, blocksize, inputsize, nbucket, commmode, nprocs); 

  sprintf(tmp, "%s.%d.txt", header, me);

  FILE *fp = fopen(tmp, "w+");
  MapReduce::print_stat(mr, fp);
  fclose(fp);
  
  MPI_Barrier(MPI_COMM_WORLD);

  if(me==0){
    time_t t = time(NULL);
    struct tm tm = *localtime(&t);
    char timestr[1024];
    sprintf(timestr, "%d-%d-%d-%d:%d:%d", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
    char infile[1024+1];
    sprintf(infile, "%s.*.txt", header);
    char outfile[1024+1];
    sprintf(outfile, "%s_%s.txt", header, timestr);
    char cmd[8192+1];
    sprintf(cmd, "cat %s>>%s", infile, outfile);
    system(cmd);
    sprintf(cmd, "rm %s", infile);
    system(cmd);
  }
}
