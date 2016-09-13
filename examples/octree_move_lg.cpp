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

int me, nprocs;
//int nbucket=17, estimate=0, factor=1;
//const char* commmode="a2a";
//const char* inputsize="512M";
//const char* blocksize="64M";
//const char* gbufsize="64M";
//const char* lbufsize="4K";

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

using namespace MAPREDUCE_NS;

void generate_octkey(MapReduce *, char *, void *);
void gen_leveled_octkey(MapReduce *, char *, int, char *, int, void*);
void sum(MapReduce *, char *, int,  MultiValueIterator *, int, void*);

double slope(double[], double[], int);

void output(const char *,const char *,const char *,float,MapReduce*,MapReduce *);

#define digits 15
int thresh=5;
bool realdata = false;
int level;

int main(int argc, char **argv)
{
  //int provided;
  //MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &provided);
  //if (provided < MPI_THREAD_FUNNELED) MPI_Abort(MPI_COMM_WORLD, 1);

  MPI_Init(&argc, &argv); 

  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  if (argc <= 5) {
    if (me == 0) printf("Syntax: octree_move_lg indir threshold prefix outdir tmpdir\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  float density = atof(argv[1]);
  char *indir = argv[2];
  const char *prefix = argv[3];
  const char *outdir = argv[4];
  //const char *tmpdir = argv[5];

  if(me==0){
    printf("density=%.2f\n", density);
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
    if(me==0) printf("Please set correct environment variables!\n");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
  nbucket = atoi(bucket_str);

  int min_limit, max_limit;
  min_limit=0;
  max_limit=digits+1;
  level=floor((max_limit+min_limit)/2);

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
  uint64_t nwords=mr_convert->map_text_file(indir, 1, 1, whitespace, generate_octkey, NULL, NULL, 0, 0);

  thresh=nwords*density;
  if(me==0){
    printf("Command line: input path=%s, thresh=%d\n", indir, thresh);
  }

  MapReduce *mr_level=new MapReduce(MPI_COMM_WORLD);
  mr_level->set_threadbufsize(lbufsize);
  mr_level->set_sendbufsize(gbufsize); 
  mr_level->set_blocksize(blocksize);
  mr_level->set_inputsize(inputsize);
  mr_level->set_commmode(commmode);
  mr_level->set_nbucket(estimate,nbucket,factor);
  mr_level->set_maxmem(32);

  mr_level->set_outofcore(0);

  while ((min_limit+1) != max_limit){
//#ifdef PART_REDUCE
#ifdef KV_HINT
    mr_level->set_KVtype(FixedKV, level, sizeof(int));
#endif
//#else
//#ifdef KV_HINT
//    mr_level->set_KVtype(FixedKV, level, 0);
//#endif
//#endif
    mr_level->map_key_value(mr_convert, gen_leveled_octkey);
//#ifdef KV_HINT
//    mr_level->set_KVtype(FixedKV, level, sizeof(int));
//#endif

    //printf("level=%d\n", level);

#ifdef PART_REDUCE
    uint64_t nkv = mr_level->reduce(sum, 1, NULL);
#else
    uint64_t nkv = mr_level->reduce(sum, 0, NULL);
#endif

    //uint64_t nkv = mr_level->reduce(sum);
    //if(me==0) {
    //  printf("min=%d,max=%d,level=%d\n",min_limit,max_limit,level);
    //  printf("nkv=%ld\n", nkv);
    //}

    if(nkv >0){
      min_limit=level;
      level= floor((max_limit+min_limit)/2);
    }else{
      max_limit=level;
      level =  floor((max_limit+min_limit)/2);
    }
  }

  output("mtmr.wc", outdir, prefix, density, mr_convert, mr_level); 

  delete mr_convert;
  delete mr_level;

  if(me==0) printf("level=%d\n", level);

  MPI_Finalize();	
}


void sum(MapReduce *mr, char *key, int keysize,  MultiValueIterator *iter, int lastreduce, void* ptr){

  int sum=0;
#ifdef PART_REDUCE
  //if(me==0) printf("count=%d\n", iter->getCount());
  for(iter->Begin(); !iter->Done(); iter->Next()){
    sum+=*(int*)iter->getValue();
  }
  //if(me==0) printf("sum=%d\n", sum);

  if (lastreduce){
    if(sum>thresh){
      //if(me==0) printf("sum=%d\n", sum);
      mr->add_key_value(key, keysize, (char*)&sum, (int)sizeof(int));
    }
  }else{
    mr->add_key_value(key, keysize, (char*)&sum, (int)sizeof(int)); 
  }
#else
  sum=iter->getCount();
  if(sum>thresh)
    mr->add_key_value(key, keysize, (char*)&sum, (int)sizeof(int)); 
#endif
}

void gen_leveled_octkey(MapReduce *mr, char *key, int keysize, char *val, int valsize, void *ptr)
{
//#ifdef PART_REDUCE
  int count=1;
  mr->add_key_value(key, level, (char*)&count, (int)sizeof(int));
//#else
//  mr->add_key_value(key, level, NULL, 0);
//#endif
}

void generate_octkey(MapReduce *mr, char *word, void *ptr)
{
  double range_up=10.0, range_down=-10.0;
  char octkey[digits];
  double coords[512];//hold x,y,z
  char ligand_id[256];
  int num_coor=0;

  char *saveptr;
  char *token = strtok_r(word, " ", &saveptr);
  memcpy(ligand_id,token,strlen(token));

  while (token != NULL){
    token = strtok_r(NULL, " ", &saveptr);
    if (token){
      coords[num_coor] = atof(token);
      num_coor++;
    }
  }
	
  const int num_atoms = floor((num_coor-2)/3);
  double *x = new double[num_atoms];
  double *y = new double[num_atoms];
  double *z = new double[num_atoms];
  /*x,y,z double arries store the cooridnates of ligands */
  for (int i=0; i!=(num_atoms); i++){
    x[i] = coords[3*i];
    y[i] = coords[3*i+1];
    z[i] = coords[3*i+2];
  }
  delete [] x;
  delete [] y;
  delete [] z;

  /*compute the b0, b1, b2 using linear regression*/
  double b0 = slope(x, y, num_atoms);
  double b1 = slope(y, z, num_atoms);
  double b2 = slope(x, z, num_atoms);

  /*compute octkey, "digit" many digits*/
  int count=0;//count how many digits are in the octkey
  double minx = range_down, miny = range_down, minz = range_down;
  double maxx = range_up, maxy = range_up, maxz = range_up;
  while (count < digits){
    int m0 = 0, m1 = 0, m2 = 0;
    double medx = minx + ((maxx - minx)/2);
    if (b0>medx){
      m0=1;
      minx=medx;
    }else{
      maxx=medx;
    }

    double medy = miny + ((maxy-miny)/2);
    if (b1>medy){
      m1=1;
      miny=medy;
    }else{
      maxy=medy;
    }
    double medz = minz + ((maxz-minz)/2);
    if (b2>medz){
      m2=1;
      minz=medz;
    }else{
      maxz=medz;
    }
		
    /*calculate the octant using the formula m0*2^0+m1*2^1+m2*2^2*/
    int bit=m0+(m1*2)+(m2*4);
    char bitc=(char)(((int)'0') + bit); //int 8 => char '8'
    octkey[count] = bitc;
    ++count;
  }

  if (realdata == false){
    double realkey = coords[num_coor - 1];
    char tmp[100];
    sprintf(tmp, "%f", realkey);
    //printf("octkey=%s\n", tmp);
    mr->add_key_value(tmp, digits, NULL, 0);
  }else{
    mr->add_key_value(octkey, digits, NULL, 0);
  }
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

void output(const char *filename, const char *outdir, const char *prefix, float density, MapReduce *mr1, MapReduce *mr2){
  char header[1000];
  char tmp[1000];

  if(estimate)
    sprintf(header, "%s/%s_d%.2f-c%s-b%s-i%s-f%d-%s.%d", \
      outdir, prefix, density, gbufsize, blocksize, inputsize, factor, commmode, nprocs);
  else
    sprintf(header, "%s/%s_d%.2f-c%s-b%s-i%s-h%d-%s.%d", \
      outdir, prefix, density, gbufsize, blocksize, inputsize, nbucket, commmode, nprocs);

  sprintf(tmp, "%s.%d.txt", header, me); 

  FILE *fp = fopen(tmp, "w+");
  //mr1->print_stat(fp);
  MapReduce::print_stat(mr2, fp);
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
