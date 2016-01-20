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


using namespace MAPREDUCE_NS;

void generate_octkey(MapReduce *, char *, void *);
void gen_leveled_octkey(MapReduce *, char *, int, char *, int, void*);
void sum(MapReduce *, char *, int, int, char *, int *, void *);

double slope(double[], double[], int);
void explore_level(int, int, MapReduce * );

int me,nprocs;
int digits=15;
int level;

int thresh=5;

int main(int argc, char **argv)
{
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &provided);
  if (provided < MPI_THREAD_FUNNELED) MPI_Abort(MPI_COMM_WORLD, 1);

  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  char *ipath = argv[1];
  thresh = atoi(argv[2]);

  int min_limit, max_limit;
  min_limit=0;
  max_limit=digits+1;
  level=floor((max_limit+min_limit)/2);

  MapReduce *datamr = new MapReduce(MPI_COMM_WORLD);

  char whitespace[10] = "\n";
  //mr->setKVtype(FixedKV, digits, 0);

  datamr->map_local(ipath, 1, 1, whitespace, generate_octkey, NULL);

  MapReduce *mr=new MapReduce(MPI_COMM_WORLD);

  //mr->setKVtype(GeneralKV);
  while ((min_limit+1) != max_limit){

    //printf("min_limit=%d, max_limit=%d\n", min_limit, max_limit); fflush(stdout);

    mr->map(datamr, gen_leveled_octkey, NULL);

    mr->convert();

    uint64_t nkv = mr->reduce(sum, NULL);

    //printf("level=%d, nkv=%ld\n", level, nkv);

    if(nkv >0){
      min_limit=level;
      level= floor((max_limit+min_limit)/2);
    }else{
      max_limit=level;
      level =  floor((max_limit+min_limit)/2);
    }
  }

  delete mr;
  delete datamr;

  printf("level=%d\n", level);

  MPI_Finalize();	
}

void sum(MapReduce *mr, char *key, int keysize, int nval, char *val, int *valsizes, void *ptr){
  //printf("keysize=%d, nval=%d\n", keysize, nval);
  int sum=nval;
  if (sum >= thresh)
    mr->add(key, keysize, (char*)&sum, (int)sizeof(int));
}


void gen_leveled_octkey(MapReduce *mr, char *key, int keysize, char *val, int valsize, void *ptr)
{
  mr->add(key, level, NULL, 0);
}


void generate_octkey(MapReduce *mr, char *word, void *ptr)
{
  double range_up=10.0, range_down=-10.0;
  char octkey[digits];
  bool realdata = false;//the last one is the octkey

  //printf("word=%s\n", word); fflush(stdout);

  double coords[512];//hold x,y,z
  char ligand_id[256];
  const int word_len = strlen(word);
  char word2[word_len+2];
  memcpy(word2, word, word_len);
  word2[word_len]=' ';
  word2[word_len+1]='\0';

  int num_coor=0;
  char *saveptr;
  char *token = strtok_r(word2, " ", &saveptr);
  memcpy(ligand_id,token,strlen(token));
  while (token != NULL){
    token = strtok_r(NULL, " ", &saveptr);
    if (token){
      coords[num_coor] = atof(token);
      //printf("%d:%lf\n", num_coor, coords[num_coor]);
      num_coor++;
    }
  }
	
  const int num_atoms = floor((num_coor-2)/3);

  //printf("num_coor=%d, num_atoms=%d\n", num_coor, num_atoms);

  double x[num_atoms], y[num_atoms], z[num_atoms];
  /*x,y,z double arries store the cooridnates of ligands */
  for (int i=0; i!=(num_atoms); i++){
    x[i] = coords[3*i];
    y[i] = coords[3*i+1];
    z[i] = coords[3*i+2];
  }

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
    mr->add(tmp, digits, NULL, 0);
  }else{
    mr->add(octkey, digits, NULL, 0);
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
