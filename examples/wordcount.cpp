/**
 * @file   wordcount.cpp
 * @Author Tao Gao (taogao@udel.edu)
 * @date   September 1st, 2016
 * @brief  This file provides interfaces to application programs.
 *
 * This file includes two classes: MapReduce and MultiValueIter.
 */
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
void mergeword(MapReduce *, char *, int, char *, int, char *, int, void*);

#define USE_LOCAL_DISK  0
//#define OUTPUT_KV

//#define PART_REDUCE

void output(const char *filename, const char *outdir, \
  const char *prefix, MapReduce *mr);

//#define PPN 2
int me, nprocs;
//int nbucket=17, estimate=0, factor=32;
//const char* inputsize="512M";
//const char* blocksize="64M";
//const char* gbufsize="64M";
//const char* lbufsize="4K";
//const char* commmode="a2a";

// MR_BUCKET_SIZE
// MR_INBUF_SIZE
// MR_PAGE_SIZE
// MR_COMM_SIZE
int nbucket, estimate=0, factor=32;
const char* inputsize;
const char* blocksize;
const char* gbufsize;
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

  char *bucket_str = getenv("MR_BUCKET_SIZE");
  inputsize = getenv("MR_INBUF_SIZE");
  blocksize = getenv("MR_PAGE_SIZE");
  gbufsize = getenv("MR_COMM_SIZE");
  if(bucket_str==NULL || inputsize==NULL\
    || blocksize==NULL || gbufsize==NULL){
    if(me==0) printf("Please set correct environment variables!\n");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
  nbucket = atoi(bucket_str);

  if(me==0){
    printf("input dir=%s\n", filedir);
    printf("prefix=%s\n", prefix);
    printf("output dir=%s\n", outdir);
    printf("inputsize=%s\n", inputsize);
    printf("blocksize=%s\n", blocksize);
    printf("gbufsize=%s\n", gbufsize);
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
  mr->set_KVtype(StringKFixedV, -1, sizeof(int64_t));
#endif

  MPI_Barrier(MPI_COMM_WORLD);

  //double t1 = MPI_Wtime();

  char whitespace[20] = " \n";
#ifndef COMPRESS
  mr->map_text_file(filedir, 1, 1, whitespace, map);
#else
  mr->map_text_file(filedir, 1, 1, whitespace, map, mergeword, NULL);
  //mr->compress(countword, NULL);
#endif

  //double t2 = MPI_Wtime();

#ifdef PART_REDUCE
  mr->reducebykey(mergeword, NULL);
#else
  mr->reduce(countword, 0, NULL);
#endif
  //double t3 = MPI_Wtime();

  MPI_Barrier(MPI_COMM_WORLD);

#ifdef OUTPUT_KV
  char tmpfile[100];
  sprintf(tmpfile, "results%d.txt", me);
  FILE *outfile=fopen(tmpfile, "w");
  mr->output(1, outfile);
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
  int len=(int)strlen(word)+1;
  //char one[10]={"1"};

  //printf("word=%s\n", word); fflush(stdout);

  int64_t one=1;
  if(len <= 8192)
    mr->add_key_value(word,len,(char*)&one,sizeof(one));
}

void countword(MapReduce *mr, char *key, int keysize,  MultiValueIterator *iter, int lastreduce, void* ptr){
  int64_t count=0;
 
  for(iter->Begin(); !iter->Done(); iter->Next()){
    count+=*(int64_t*)iter->getValue();
  }
  
  mr->add_key_value(key, keysize, (char*)&count, sizeof(count));
}

void mergeword(MapReduce *mr, char *key, int keysize, \
  char *val1, int val1size, char *val2, int val2size, void* ptr){
  int64_t count=*(int64_t*)(val1)+*(int64_t*)(val2);
 
  mr->add_key_value(key, keysize, (char*)&count, sizeof(count));
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
#ifdef BGQ
    FILE* finalize_script = fopen(prefix, "w");
    fprintf(finalize_script, "#!/bin/zsh\n");
    fprintf(finalize_script, "cat %s>>%s\n", infile, outfile);
    fprintf(finalize_script, "rm %s\n", infile);
    fclose(finalize_script);
#else
    char cmd[8192+1];
    sprintf(cmd, "cat %s>>%s", infile, outfile);
    system(cmd);
    sprintf(cmd, "rm %s", infile);
    system(cmd);
#endif
  }
}
