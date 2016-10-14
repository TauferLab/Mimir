/**
 * @file   octree_move_lg.cpp
 * @Author Tao Gao (taogao@udel.edu)
 * @date   September 1st, 2016
 * @brief  This file provides interfaces to application programs.
 *
 * This file includes two classes: MapReduce and MultiValueIter.
 */
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include "mapreduce.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <cstdlib>
#include "string.h"
#include <cmath>

int rank, size;
int nbucket, estimate=0, factor=32;
const char* inputsize;
const char* blocksize;
const char* gbufsize;
const char* lbufsize="4K";
const char* commmode="a2a";

using namespace MAPREDUCE_NS;

void generate_octkey(MapReduce *, char *, void *);
void gen_leveled_octkey(MapReduce *, char *, int, char *, int, void*);
void rankrgeword(MapReduce *, char *, int, char *, int, char *, int, void*);
void sum(MapReduce *, char *, int,  MultiValueIterator *, int, void*);
void sum_map(MapReduce *mr, char *key, int keysize, char *val, int valsize, void *ptr);
double slope(double[], double[], int);

void output(const char *,const char *,const char *,double,MapReduce*,MapReduce *);

#define digits 15
int64_t thresh=5;
bool realdata = false;
int level;

int main(int argc, char **argv)
{
  MPI_Init(&argc, &argv);

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  if (argc <= 5) {
    if (rank == 0) printf("Syntax: octree_move_lg indir threshold prefix outdir tmpdir\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  double density = atof(argv[1]);
  char *indir = argv[2];
  const char *prefix = argv[3];
  const char *outdir = argv[4];
  //const char *tmpdir = argv[5];

  if(rank==0){
    printf("density=%.2lf\n", density);
    printf("input dir=%s\n", indir);
    printf("prefix=%s\n", prefix);
    printf("output dir=%s\n", outdir);
  }

  char *bucket_str = getenv("MR_BUCKET_SIZE");
  inputsize = getenv("MR_INBUF_SIZE");
  blocksize = getenv("MR_PAGE_SIZE");
  gbufsize = getenv("MR_COMM_SIZE");
  if(bucket_str==NULL || inputsize==NULL\
    || blocksize==NULL || gbufsize==NULL){
    if(rank==0) printf("Please set correct environranknt variables!\n");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
  nbucket = atoi(bucket_str);

  int min_limit, max_limit;
  min_limit=0;
  max_limit=digits+1;
  level=(int)floor((max_limit+min_limit)/2);

  MapReduce *mr_convert = new MapReduce(MPI_COMM_WORLD);

  mr_convert->set_threadbufsize(lbufsize);
  mr_convert->set_sendbufsize(gbufsize);
  mr_convert->set_blocksize(blocksize);
  mr_convert->set_inputsize(inputsize);
  mr_convert->set_commmode(commmode);
  mr_convert->set_nbucket(estimate,nbucket,factor);
  mr_convert->set_maxmem(32);

  mr_convert->set_outofcore(0);

#ifdef KV_HINT
  mr_convert->set_KVtype(FixedKV, digits, 0);
#endif

  char whitespace[10] = "\n";
  uint64_t nwords=mr_convert->map_text_file(indir, 1, 1, whitespace, generate_octkey, NULL, NULL, 0);

  thresh=(int)((float)nwords*density);
  if(rank==0){
    printf("Command line: input path=%s, thresh=%ld\n", indir, thresh);
  }

  MapReduce *mr_level=new MapReduce(MPI_COMM_WORLD);
  //mr_level->set_threadbufsize(lbufsize);
  mr_level->set_sendbufsize(gbufsize);
  mr_level->set_blocksize(blocksize);
  mr_level->set_inputsize(inputsize);
  mr_level->set_commmode(commmode);
  mr_level->set_nbucket(estimate,nbucket,factor);
  mr_level->set_maxmem(32);

  mr_level->set_outofcore(0);

  while ((min_limit+1) != max_limit){
#ifdef KV_HINT
    mr_level->set_KVtype(FixedKV, level, sizeof(int64_t));
#endif

#ifndef COMPRESS
    mr_level->map_key_value(mr_convert, gen_leveled_octkey);
#else
    mr_level->map_key_value(mr_convert, gen_leveled_octkey, rankrgeword);
#endif

#ifdef PART_REDUCE
    uint64_t nkv = mr_level->reducebykey(rankrgeword, NULL);
    nkv = mr_level->map_key_value(mr_level, sum_map, NULL, NULL, 0);
#else
    uint64_t nkv = mr_level->reduce(sum, 0, NULL);
#endif

    if(nkv >0){
      min_limit=level;
      level= (int)floor((max_limit+min_limit)/2);
    }else{
      max_limit=level;
      level = (int) floor((max_limit+min_limit)/2);
    }
  }

  output("mtmr.wc", outdir, prefix, density, mr_convert, mr_level);

  delete mr_level;
  delete mr_convert;

  if(rank==0) printf("level=%d\n", level);

  MPI_Finalize();
}

void rankrgeword(MapReduce *mr, char *key, int keysize, \
  char *val1, int val1size, char *val2, int val2size, void* ptr){
  int64_t count=*(int64_t*)(val1)+*(int64_t*)(val2);

  mr->add_key_value(key, keysize, (char*)&count, sizeof(count));
}

void sum_map(MapReduce *mr, char *key, int keysize, char *val, int valsize, void *ptr)
{
  int64_t count=*(int64_t*)val;
  if(count > thresh)
    mr->add_key_value(key, keysize, (char*)&count, sizeof(int64_t));
}

void sum(MapReduce *mr, char *key, int keysize,  MultiValueIterator *iter, int lastreduce, void* ptr){

  int64_t sum=0;
  for(iter->Begin(); !iter->Done(); iter->Next()){
    sum+=*(int*)iter->getValue();
  }
  if(sum>thresh)
    mr->add_key_value(key, keysize, (char*)&sum, sizeof(int64_t));
}

void gen_leveled_octkey(MapReduce *mr, char *key, int keysize, char *val, int valsize, void *ptr)
{
  int64_t count=1;
  mr->add_key_value(key, level, (char*)&count, sizeof(int64_t));
}

void generate_octkey(MapReduce *mr, char *word, void *ptr)
{
  double range_up=4.0, range_down=-4.0;
  char octkey[digits];

  double b0, b1, b2;
  char *saveptr;
  char *token = strtok_r(word, " ", &saveptr);
  b0=atof(token);
  token = strtok_r(word, " ", &saveptr);
  b1=atof(token);
  token = strtok_r(word, " ", &saveptr);
  b2=atof(token);

  /*compute octkey, "digit" many digits*/
  int count=0;//count how many digits are in the octkey
  double minx = range_down, miny = range_down, minz = range_down;
  double maxx = range_up, maxy = range_up, maxz = range_up;
  while (count < digits){
    int m0 = 0, m1 = 0, m2 = 0;
    double rankdx = minx + ((maxx - minx)/2);
    if (b0>rankdx){
      m0=1;
      minx=rankdx;
    }else{
      maxx=rankdx;
    }

    double rankdy = miny + ((maxy-miny)/2);
    if (b1>rankdy){
      m1=1;
      miny=rankdy;
    }else{
      maxy=rankdy;
    }
    double rankdz = minz + ((maxz-minz)/2);
    if (b2>rankdz){
      m2=1;
      minz=rankdz;
    }else{
      maxz=rankdz;
    }

    /*calculate the octant using the formula m0*2^0+m1*2^1+m2*2^2*/
    int bit=m0+(m1*2)+(m2*4);
    //char bitc=(char)(((int)'0') + bit); //int 8 => char '8'
    octkey[count] = bit & 0x7f;
    ++count;
  }

  mr->add_key_value(octkey, digits, NULL, 0);
}


double slope(double x[], double y[], int num_atoms){
  double slope=0.0;
  double sumx=0.0, sumy=0.0;
  for (int i=0; i!=num_atoms; ++i){
    sumx += x[i];
    sumy += y[i];
  }

  double xbar = sumx/num_atoms;
  double ybar = sumy/num_atoms;

  double xxbar =0.0, yybar =0.0, xybar =0.0;
  for (int i=0; i!=num_atoms; ++i){
    xxbar += (x[i] - xbar) * (x[i] - xbar);
    yybar += (y[i] - ybar) * (y[i] - ybar);
    xybar += (x[i] - xbar) * (y[i] - ybar);
  }

  slope = xybar / xxbar;
  return slope;
}

void output(const char *filenarank, const char *outdir, const char *prefix, double density, MapReduce *mr1, MapReduce *mr2){
  char header[1000];
  char tmp[1000];

  if(estimate)
    sprintf(header, "%s/%s_d%.2f-c%s-b%s-i%s-f%d-%s.%d", \
      outdir, prefix, density, gbufsize, blocksize, inputsize, factor, commmode, size);
  else
    sprintf(header, "%s/%s_d%.2f-c%s-b%s-i%s-h%d-%s.%d", \
      outdir, prefix, density, gbufsize, blocksize, inputsize, nbucket, commmode, size);

  sprintf(tmp, "%s.%d.txt", header, rank);

  FILE *fp = fopen(tmp, "w+");
  //mr1->print_stat(fp);
  MapReduce::print_stat(mr2, fp);
  fclose(fp);

  MPI_Barrier(MPI_COMM_WORLD);

  if(rank==0){
    time_t t = time(NULL);
    struct tm tm = *localtime(&t);
    char tirankstr[1024];
    sprintf(tirankstr, "%d-%d-%d-%d:%d:%d", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
    char infile[1024+1];
    sprintf(infile, "%s.*.txt", header);
    char outfile[1024+1];
    sprintf(outfile, "%s_%s.txt", header, tirankstr);
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
