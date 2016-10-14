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
//#include "config.h"

using namespace MAPREDUCE_NS;

//#include "stat.h"

void map(MapReduce *mr, char *word, void *ptr);
void countword(MapReduce *, char *, int,  MultiValueIterator *, int, void*);
void mergeword(MapReduce *, char *, int, char *, int, char *, int, void*);
void output(const char *filename, const char *outdir, \
  const char *prefix, MapReduce *mr);

int rank, size;
int nbucket, estimate=0, factor=32;
const char* inputsize;
const char* blocksize;
const char* gbufsize;
const char* commmode="a2a";

int main(int argc, char *argv[])
{
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  if(argc <= 3){
    if(rank == 0) printf("Syntax: wordcount filepath prefix outdir\n");
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
    if(rank==0) printf("Please set correct environment variables!\n");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
  nbucket = atoi(bucket_str);

  if(rank==0){
    printf("input dir=%s\n", filedir);
    printf("prefix=%s\n", prefix);
    printf("output dir=%s\n", outdir);
    printf("inputsize=%s\n", inputsize);
    printf("blocksize=%s\n", blocksize);
    printf("gbufsize=%s\n", gbufsize);
  }

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);

  mr->set_sendbufsize(gbufsize);
  mr->set_blocksize(blocksize);
  mr->set_inputsize(inputsize);
  mr->set_commmode(commmode);
  mr->set_nbucket(estimate,nbucket,factor);
  mr->set_maxmem(32);

  mr->set_outofcore(0);

#ifdef KV_HINT
  mr->set_KVtype(StringKFixedV, -1, sizeof(int64_t));
#endif

  MPI_Barrier(MPI_COMM_WORLD);

  char whitespace[20] = " \n";
#ifndef COMPRESS
  mr->map_text_file(filedir, 1, 1, whitespace, map);
#else
  mr->map_text_file(filedir, 1, 1, whitespace, map, mergeword, NULL);
#endif

#ifdef PART_REDUCE
  mr->reducebykey(mergeword, NULL);
#else
  mr->reduce(countword, 0, NULL);
#endif

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

  MPI_Finalize();
}

void map(MapReduce *mr, char *word, void *ptr){
  int len=(int)strlen(word)+1;

  int64_t one=1;
  if(len <= 1024)
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
      outdir, prefix, gbufsize, blocksize, inputsize, factor, commmode, size);
  else
    sprintf(header, "%s/%s_c%s-b%s-i%s-h%d-%s.%d", \
      outdir, prefix, gbufsize, blocksize, inputsize, nbucket, commmode, size);

  sprintf(tmp, "%s.%d.txt", header, rank);

  FILE *fp = fopen(tmp, "w+");
  MapReduce::print_stat(mr, fp);
  fclose(fp);

  MPI_Barrier(MPI_COMM_WORLD);

  if(rank==0){
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
