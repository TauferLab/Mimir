#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <string.h>
#include <sys/stat.h>

#include "mapreduce.h"

using namespace MAPREDUCE_NS;

void fileread(MapReduce *, const char *, void *);
void countword(MapReduce *, char *, int, int, char *, int *, void*);

int me, nprocs;

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

  mr->map(argv[1], 1, 0, fileread, NULL);

  mr->output();

  mr->convert();

  mr->output();

  mr->reduce(countword, NULL);

  MPI_Barrier(MPI_COMM_WORLD);

  mr->output();

  delete mr;

  MPI_Finalize();
}

void fileread(MapReduce *mr, const char *fname, void *ptr){
  //printf("%d[%d] read file name=%s\n", me, nprocs, fname);

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

  char whitespace[20] = " \t\n\f\r\0";
  char *word = strtok(text,whitespace);
  while (word) {
    char val[2]="1";
    mr->add(word,strlen(word)+1,val,strlen(val)+1);
    word = strtok(NULL,whitespace);
  }

  delete [] text;
}

void countword(MapReduce *mr, char *key, int keysize, int nval, char *val, int *valsizes, void *ptr){
  char count[100];
  sprintf(count, "%d", nval);
  mr->add(key, keysize, count, strlen(count)+1);
}
