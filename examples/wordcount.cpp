#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <string.h>
#include <sys/stat.h>
#include <omp.h>

#include "mapreduce.h"

using namespace MAPREDUCE_NS;

void fileread(MapReduce *, const char *, void *);
void countword(MapReduce *, char *, int,  MultiValueIterator *, void*);

void output(const char *filename, MapReduce *mr);

int me, nprocs;

int commmode=0;
int blocksize=16;
int gbufsize=16;
int lbufsize=8;

uint64_t nword, nunique;
double t1, t2, t3;

int main(int argc, char *argv[])
{
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &provided);
  if (provided < MPI_THREAD_FUNNELED) MPI_Abort(MPI_COMM_WORLD, 1);

  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  if(argc < 2){
    if(me == 0) printf("Syntax: wordcount filepath\n");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  if(argc > 2){
    commmode=atoi(argv[2]);
  } 
  if(argc > 3){
    blocksize=atoi(argv[3]);
  }
  if(argc > 4){
    gbufsize=atoi(argv[4]);
  }
  if(argc > 5){
    lbufsize=atoi(argv[5]);
  }

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);

#if 1
  mr->set_localbufsize(lbufsize);
  mr->set_globalbufsize(gbufsize*1024);
  mr->set_blocksize(blocksize*1024);
  mr->set_maxmem(32*1024*1024);
  mr->set_commmode(commmode);
#endif

  mr->set_outofcore(0);

  MPI_Barrier(MPI_COMM_WORLD);

  t1 = MPI_Wtime();

  nword = mr->map(argv[1], 1, 1, fileread, NULL);

  t2 = MPI_Wtime();

  nunique = mr->reduce(countword, 0, NULL);
  
  t3 = MPI_Wtime();

  MPI_Barrier(MPI_COMM_WORLD);

  output("wc", mr);
 
  delete mr;

  MPI_Finalize();
}

void fileread(MapReduce *mr, const char *fname, void *ptr){
  //int tid = omp_get_thread_num();

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
  char whitespace[20] = " \n";
  char *word = strtok_r(text,whitespace,&saveptr);
  while (word) {
    int len=strlen(word)+1;
    if(len <= 8192)
      mr->add(word,len,NULL,0);
    word = strtok_r(NULL,whitespace,&saveptr);
  }

  delete [] text;
}

void countword(MapReduce *mr, char *key, int keysize,  MultiValueIterator *iter, void* ptr){
  char count[100];
  sprintf(count, "%d", iter->getCount());
  mr->add(key, keysize, count, strlen(count)+1);
}

void output(const char *filename, MapReduce *mr){
   char tmp[1000];
   sprintf(tmp, "%s.%d.%d.%d.%d.%d", filename, lbufsize, gbufsize, blocksize, commmode, me);
   FILE *fp = fopen(tmp, "w");
   fprintf(fp, "%ld,%ld,%g,%g,%g,\n", nword, nunique, t3-t1, t2-t1, t3-t2);
   mr->show_stat(0, fp);
   fclose(fp);
}
