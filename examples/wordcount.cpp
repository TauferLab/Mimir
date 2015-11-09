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

uint64_t count=0;

int me, nprocs;

double io_t = 0.0;

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

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);

  MPI_Barrier(MPI_COMM_WORLD);

  double t1 = MPI_Wtime();

  uint64_t nword = mr->map(argv[1], 0, 1, fileread, NULL);

  double t2 = MPI_Wtime();

  //mr->output();

  mr->convert();

  double t3 = MPI_Wtime();

  //mr->output();

  mr->reduce(countword, NULL);

  double t4 = MPI_Wtime();

  MPI_Barrier(MPI_COMM_WORLD);

  //mr->output();

  delete mr;

  if(me==0){
    printf("word count=%ld, count=%ld\n", nword, count);
    printf("process=%d\n", nprocs);
    printf("results: total=%g, map=%g, convert=%g, reduce=%g, io=%g\n", t4-t1, t2-t1, t3-t2, t4-t3, io_t);
  }
  MPI_Finalize();
}

void fileread(MapReduce *mr, const char *fname, void *ptr){
  int tid = omp_get_thread_num();
  if(me==0) {
    count++;
    printf("%d[%d] tid=%d, read file name=%s\n", me, nprocs, tid, fname);
  }

  struct stat stbuf;
  int flag = stat(fname,&stbuf);
  if (flag < 0) {
    printf("ERROR: Could not query file size\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }
  int filesize = stbuf.st_size;

  FILE *fp = fopen(fname,"r");
  char *text = new char[filesize+1];

  double t1 = MPI_Wtime();
  int nchar = fread(text,1,filesize,fp);
  double t2 = MPI_Wtime();
  io_t += (t2-t1);

  text[nchar] = '\0';
  fclose(fp);

  char *line = text;
  int linesize;

  while(line && line[0]!='\0'){
    // replace '\n' with '\0'
    char *p = strchr(line, '\n');
    if(p) p[0]='\0';
    printf("line=%s\n", line);
    // get line size
    linesize = strlen(line)+1;

    char *word = line;
    while(word && word[0]!='\0'){
      char *q = strchr(word, ' ');
      if(q) q[0]='\0';
      printf("word=%s\n", word);
      int wordsize = strlen(word)+1;
      //if(me==0 && tid == 0){
      printf("wordsize=%d\n", wordsize);
      //}
      //if(strlen(word) > 0)
      mr->add(word, wordsize, "", 1);
      word += wordsize;
//#pragma omp barrier
//      count++;
    }

    line += linesize;
  }


  delete [] text;
}

void countword(MapReduce *mr, char *key, int keysize, int nval, char *val, int *valsizes, void *ptr){
  char count[100];
  sprintf(count, "%d", nval);
  mr->add(key, keysize, count, strlen(count)+1);
}
