#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <string.h>
#include <sys/stat.h>
#include <omp.h>

#include "mapreduce.h"

using namespace MAPREDUCE_NS;

void fileread(MapReduce *, const char *, void *);
void countword(MapReduce *, char *, int, int, char *, int *, void*);

int me, nprocs;

#define TEST_TIMES 1
double wtime[TEST_TIMES]; 

//double io_t = 0.0;
//double s = 0.0;

int main(int argc, char *argv[])
{
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &provided);
  if (provided < MPI_THREAD_FUNNELED) MPI_Abort(MPI_COMM_WORLD, 1);

  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  if(argc != 2){
    if(me == 0) printf("Syntax: wordcount filepath\n");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  if(me==0) fprintf(stdout, "wordcount test begin\n");

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
  //mr->setBlocksize(64*1024);
  mr->setKVtype(StringKeyOnly);

  for(int i = 0; i < TEST_TIMES; i++){
 
    mr->clear_stat();
    //seek_t = 0.0;
  
    MPI_Barrier(MPI_COMM_WORLD);

    double t1 = MPI_Wtime();

    uint64_t nword = mr->map(argv[1], 0, 1, fileread, NULL);

    double t2 = MPI_Wtime();

    //mr->output();

    //printf("begin convert\n");
    uint64_t nunique = mr->convert();
    //printf("end convert!\n");

    double t3 = MPI_Wtime();

    //printf("begin reduce\n");
    mr->reduce(countword, NULL);
    //printf("end reduce\n");

    //mr->output();

    double t4 = MPI_Wtime();

    MPI_Barrier(MPI_COMM_WORLD);
 
    wtime[i] = t4-t1;

    if(me==0) mr->print_stat();

    if(me==0){
      printf("%d nword=%ld, nunique=%ld, time=%g(map=%g, convert=%g, reduce=%g\n", i, nword, nunique, wtime[i], t2-t1, t3-t2, t4-t3);
    }
  }

  delete mr;

  if(me==0) fprintf(stdout, "wordcount test end.\n");

  if(me==0){
    double tsum=wtime[0], tavg=0.0, tmax=wtime[0], tmin=wtime[0];
    for(int i=1; i<TEST_TIMES; i++) {
     tsum+=wtime[i];
     if(wtime[i] > tmax) tmax=wtime[i];
     if(wtime[i] < tmin) tmin=wtime[i];
    }
    tavg = tsum/TEST_TIMES;
    printf("process count=%d\n", nprocs);
    printf("average time=%g, max time=%g, min time=%g\n", tavg, tmax, tmin);
  }


  MPI_Finalize();
}

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

  int nchar = fread(text,1,filesize,fp);

  text[nchar] = '\0';
  fclose(fp);

  char *saveptr = NULL;
  char whitespace[20] = " \t\n\f\r\0";
  char *word = strtok_r(text,whitespace,&saveptr);
  while (word) {
    char val[1]="";
    mr->add(word,strlen(word)+1,NULL,0);
    //double t1 = omp_get_wtime();
    word = strtok_r(NULL,whitespace,&saveptr);
    //double t2 = omp_get_wtime();
    //if(tid==0) seek_t += (t2-t1);
  }

  delete [] text;
}

void countword(MapReduce *mr, char *key, int keysize, int nval, char *val, int *valsizes, void *ptr){
  char count[100];
  sprintf(count, "%d", nval);
  mr->add(key, keysize, count, strlen(count)+1);
}

//char * mystrtok(char *text, ){
//}
