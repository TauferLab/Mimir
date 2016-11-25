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
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <cstdlib>
#include <cmath>

#include "mapreduce.h"
#include "common.h"

using namespace MIMIR_NS;
int rank, size;

void generate_octkey(MapReduce *, char *, void *);
void gen_leveled_octkey(MapReduce *, char *, int, char *, int, void*);
void combiner(MapReduce *, char *, int, char *, int, char *, int, void*);
void sum(MapReduce *, char *, int,  MultiValueIterator *, void*);
//void sum_map(MapReduce *mr, char *key, int keysize, char *val, int valsize, void *ptr);
double slope(double[], double[], int);

#define digits 15
int64_t thresh=5;
bool realdata = false;
int level;

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc < 6) {
        if (rank == 0) printf("Syntax: octree_move_lg threshold indir prefix outdir\n");
        MPI_Abort(MPI_COMM_WORLD,1);
    }

    double density = atof(argv[1]);
    char *indir = argv[2];
    char *prefix = argv[3];
    char *outdir = argv[4];
    const char *tmpdir = argv[5];
 
    if(rank==0){
        printf("density=%f\n", density);
        printf("input dir=%s\n", indir);
        printf("prefix=%s\n", prefix);
        printf("output dir=%s\n", outdir);
        printf("tmp dir=%s\n", tmpdir);  
    }

    check_envars(rank, size);

    int min_limit, max_limit;
    min_limit=0;
    max_limit=digits+1;
    level=(int)floor((max_limit+min_limit)/2);

    MapReduce *mr_convert = new MapReduce(MPI_COMM_WORLD);

#ifdef KHINT
    mr_convert->set_key_length(digits);
#endif
#ifdef VHINT
    mr_convert->set_value_length(0);
#endif

    char whitespace[10] = "\n";
    uint64_t nwords=mr_convert->map_text_file(indir, 1, 1, whitespace, generate_octkey, NULL, 0);

    thresh=(int64_t)((float)nwords*density);
    if(rank==0){
        printf("Command line: input path=%s, thresh=%ld\n", indir, thresh);
    }

    MapReduce *mr_level=new MapReduce(MPI_COMM_WORLD);

#ifdef COMBINE
    mr_level->set_combiner(combiner);
#endif

    while ((min_limit+1) != max_limit){
#ifdef KHINT
        mr_level->set_key_length(level);
#endif
#ifdef VHINT
        mr_level->set_value_length(sizeof(int64_t));
#endif
        mr_level->map_key_value(mr_convert, gen_leveled_octkey);
        uint64_t nkv = mr_level->reduce(sum, NULL);

        //mr_level->output(stdout, StringType, Int64Type);
        //if(rank==0) printf("nkv=%ld\n", nkv);

        if(nkv >0){
            min_limit=level;
            level= (int)floor((max_limit+min_limit)/2);
        }else{
            max_limit=level;
            level = (int)floor((max_limit+min_limit)/2);
        }
    }

    delete mr_level;

    output(rank, size, prefix, outdir);

    delete mr_convert;

    if(rank==0) printf("level=%d\n", level);

    MPI_Finalize();
}

void combiner(MapReduce *mr, char *key, int keysize, \
  char *val1, int val1size, char *val2, int val2size, void* ptr){
  int64_t count=*(int64_t*)(val1)+*(int64_t*)(val2);

  mr->update_key_value(key, keysize, (char*)&count, sizeof(count));
}

//void sum_map(MapReduce *mr, char *key, int keysize, char *val, int valsize, void *ptr)
//{
//  int64_t count=*(int64_t*)val;
//  if(count > thresh)
//    mr->add_key_value(key, keysize, (char*)&count, sizeof(int64_t));
//}

void sum(MapReduce *mr, char *key, int keysize,  MultiValueIterator *iter, void* ptr){

    int64_t sum=0;
    for(iter->Begin(); !iter->Done(); iter->Next()){
        sum+=*(int64_t*)iter->getValue();
    }
    //printf("sum=%d, thresh=%ld\n", sum, thresh);
    if(sum>thresh){
        //printf("sum=%ld\n", sum); fflush(stdout);
        mr->add_key_value(key, keysize, (char*)&sum, sizeof(int64_t));
    }
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


