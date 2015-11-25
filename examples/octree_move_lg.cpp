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

// map local function
void generate_octkey(MapReduce *, char *, void *);
// map function
void gen_leveled_octkey(MapReduce *, char *, int, char *, int, void*);
// reduce function
void sum(MapReduce *, char *, int, int, char *, int *, void *);

double slope(double[], double[], int); //function inside generate_octkey
void explore_level(int, int, MapReduce * ); //explore the int level of the tree


/* --------------------------------global vars-------------------------------------- */
int me,nprocs; //for mpi process, differenciate diff processes when print debug info
int digits=15; //width of the octkey, 15 default, set by main, used by map
int thresh=50; //number of points within the octant to be considered dense, and to be further subdivide, set by main, used by reduce
bool result=true; //save results to file, true= yes, set by main, used by map and reduce
bool branch=false;//true branch down, false branch up; used by main and sum functions
//char base_dir[] = "/home/bzhang/csd173/bzhang/mrmpi-20Jun11/mrmpi_clustering/scripts/";
//char base_dir[] = "/home/zhang/mrmpi-20Jun11/mrmpi_clustering/";
const char *base_dir;
int level = 8; //level: explore this level of the oct-tree, used by main and gen_leveled_octkey
//bool realdata=false; //use real dataset or synthetic dataset, true means using real, false means using synthetic, set by main, used by map generate octkey

/*-------------------------------------------------------------------------*/

int main(int argc, char **argv)
{
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &provided);
  if (provided < MPI_THREAD_FUNNELED) MPI_Abort(MPI_COMM_WORLD, 1);
  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  char *inpath = argv[1];
  thresh = atoi(argv[2]);

  /*var initilization*/
  int min_limit, max_limit; //level: explore this level of the oct-tree
  min_limit=0;
  max_limit=digits+1;
  level=floor((max_limit+min_limit)/2);

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);

  printf("P%d, before map_local.\n",me);

  mr->setKVtype(2, digits, 0);
  char whitespace[10] = " \t\r\n";
  /*uint64_t nwords =*/ mr->map_local(inpath, 1, 1, whitespace, generate_octkey, NULL);
  printf("P%d, after map_local.\n",me);
  //mr->output(2);

  /*map (at the end do the communication), so it has to pair with reduc,
    map: take first 5 dig of the octkey, emilt octkey[0--4], 1
    reduce: sum all the 1s*/

#if 1
  mr->setKVtype(0);
  while ((min_limit+1) != max_limit){
    printf("P%d, before map, leve %d.\n",me, level );

    /*uint64_t nkeys =*/ mr->map(mr, gen_leveled_octkey, NULL);
    printf("P%d, after map.\n",me);

    //mr->output();

    mr->convert();

    printf("P%d, before reduce.\n", me);

    uint64_t nkv = mr->reduce(sum, NULL);

    printf("nkv=%ld\n", nkv);

    printf("P%d, after reduce.\n", me);
    if (nkv >0){
       printf("P%d, nkv is %ld.\n",me, nkv);
      min_limit=level;
      level =  floor((max_limit+min_limit)/2);
    }else{
      printf("P%d, nkv is 0...\n", me);
      max_limit=level;
      level =  floor((max_limit+min_limit)/2);
    }
  }
#endif

  delete mr;

  MPI_Finalize();	
	
}

/*---------------function implementation-----------------------------*/
void sum(MapReduce *mr, char *key, int keysize, int nval, char *val, int *valsizes, void *ptr){
  uint64_t sum = 0;
  int offset = 0;
  for(int i = 0; i < nval; i++){
    sum += atoi(val+offset);
    offset += valsizes[i];
  }

  if (sum >= (uint64_t)thresh){
    char tmp[1024];
    sprintf(tmp, "%ld", sum);
    mr->add(key, keysize, tmp, strlen(tmp)+1);
  }
}


void gen_leveled_octkey(MapReduce *mr, char *key, int keysize, char *val, int valsize, void *ptr)
{
  char newval[2]="1";
  mr->add(key, strlen(key)+1, newval, strlen(newval)+1);
}


void generate_octkey(MapReduce *mr, char *word, void *ptr)
{
  double /*tmp=0,*/ range_up=10.0, range_down=-10.0; //the upper and down limit of the range of slopes
  char octkey[digits];
  bool realdata = false;//the last one is the octkey

  double coords[512];//hold x,y,z
  char ligand_id[256];
  const int word_len = strlen(word);
  char word2[word_len+2];
  memcpy(word2, word, word_len);
  word2[word_len]=' ';
  word2[word_len+1]='\0';

  int num_coor=0;
  //char *token = strtok(word2, " ");
  char *saveptr;
  char *token = strtok_r(word2, " ", &saveptr);
   //logf_map_t_of<<"Before while, token: |"<<token<<"|."<<std::endl;
  memcpy(ligand_id,token,strlen(token));
  while (token != NULL)
  {
    //test_index += (strlen(token)+1);
    //token = strtok(NULL, " ");
    token = strtok_r(NULL, " ", &saveptr);
    if (token){
      coords[num_coor] = atof(token);
      num_coor++;
    }
  }
	
  const int num_atoms = floor((num_coor-2)/3);
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
