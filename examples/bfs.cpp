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
#include <string.h>
#include <cmath>

#include "mapreduce.h"
#include "memory.h"

// Please set MR_BUCKET_SIZE, MR_INBUF_SIZE, MR_PAGE_SIZE, MR_COMM_SIZE
int nbucket, estimate=0, factor=32;
const char* inputsize;
const char* blocksize;
const char* gbufsize;
const char* commmode="a2a";

using namespace MAPREDUCE_NS;

#define BYTE_BITS 8
#define LONG_BITS (sizeof(unsigned long)*BYTE_BITS)
#define TEST_VISITED(v, vis) \
  ((vis[(v)/LONG_BITS]) & (1UL << ((v)%LONG_BITS)))
#define SET_VISITED(v, vis)  \
  ((vis[(v)/LONG_BITS]) |= (1UL << ((v)%LONG_BITS)))

// graph partition
int64_t mypartition(char *, int);
// read edge lists from files
void fileread(MapReduce *, char *, void *);
// construct graph struct
void makegraph(MapReduce *mr, char *key, int keybytes, char *value, int valuebytes, void *ptr);
// count local edge number
void countedge(char *, int, char *, int,void *);

// get root vertex
int64_t getrootvert();
// expand root vertex
void rootvisit(MapReduce* , void *);
// expand vertex
void expand(MapReduce *, char *, int, char *, int, void *);
// shrink vertex
void shrink(MapReduce *, char *, int, char *, int, void *);
// compress function
void compress(MapReduce *, char *, int, char *, int, char *, int, void*);

void output(const char *,const char *,const char *,MapReduce*);

// CSR graph
int rank, size;
int64_t  nglobalverts;     // global vertex count
int64_t  nglobaledges;     // global edge count
int64_t  nlocalverts;      // local vertex count
int64_t  nlocaledges;      // local edge count
int64_t  nvertoffset;      // local vertex's offset
int64_t quot, rem;         // quotient and reminder of globalverts/size

size_t   *rowstarts;       // rowstarts
int64_t  *columns;         // columns
unsigned long* vis;        // visited bitmap
int64_t *pred;             // pred map
int64_t root;              // root vertex
size_t   *rowinserts;      // tmp buffer for construct CSR

#define MAX_LEVEL 100
uint64_t nactives[MAX_LEVEL];

#ifdef OUTPUT_RESULT
FILE *rf = NULL;
#endif

int main(int argc, char **argv)
{
  // Initalize MPI
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  // Get pararankters
  if (argc < 7) {
    if (rank == 0) printf("Syntax: bfs N indir prefix outdir tmpdir seed\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  nglobalverts = strtoull(argv[1], NULL, 0);
  char *indir = argv[2];
  const char *prefix = argv[3];
  const char *outdir = argv[4];
  const char *tmpdir = argv[5];

  srand(atoi(argv[6]));

  if(nglobalverts<=0){
    if (rank == 0) printf("The global vertexs should be larger than zero\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if(rank==0){
    printf("global vertexs=%ld\n", nglobalverts);
    printf("input dir=%s\n", indir);
    printf("prefix=%s\n", prefix);
    printf("output dir=%s\n", outdir);
    printf("tmp dir=%s\n", tmpdir); fflush(stdout);
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

  // compute vertex partition range
  quot = nglobalverts/size;
  rem  = nglobalverts%size;
  if(rank<rem) {
    nlocalverts=quot+1;
    nvertoffset=rank*(quot+1);
  }
  else {
    nlocalverts=quot;
    nvertoffset=rank*quot+rem;
  }

#ifdef OUTPUT_RESULT
  if(rank==0){
    char rfile[100];
    sprintf(rfile, "%s_result.txt", prefix);
    rf = fopen(rfile, "a+");
  }
#endif

  // Initialize MapReduce
  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
  mr->set_sendbufsize(gbufsize);
  mr->set_blocksize(blocksize);
  mr->set_inputsize(inputsize);
  mr->set_commmode(commmode);
  mr->set_nbucket(estimate,nbucket,factor);
  mr->set_maxmem(32);
  mr->set_hash(mypartition);
  //mr->set_outofcore(0);

  if(rank==0) fprintf(stdout, "make CSR graph start.\n");

  // make graph
  MPI_Barrier(MPI_COMM_WORLD);

#ifdef KV_HINT
  mr->set_KVtype(FixedKV, sizeof(int64_t), sizeof(int64_t));
#endif

  // partition file
  char whitespace[10] = "\n";
  uint64_t nedges=mr->map_text_file(indir, 1, 1, whitespace, fileread);
  nglobaledges = nedges;

  rowstarts = new size_t[nlocalverts+1];
  rowinserts = new size_t[nlocalverts];
  rowstarts[0] = 0;
  for(int64_t i = 0; i < nlocalverts; i++){
    rowstarts[i+1] = 0;
    rowinserts[i] =0;
  }
  nlocaledges = 0;
  mr->scan(countedge,NULL);

  for(int64_t i = 0; i < nlocalverts; i++){
    rowstarts[i+1] += rowstarts[i];
  }

  if(rank==0) fprintf(stdout, "local edge=%ld\n", nlocaledges);

  // columns=(int64_t*)malloc(nlocaledges*sizeof(int64_t));
  columns=(int64_t*)mem_aligned_malloc(4096, nlocaledges*sizeof(int64_t));
  if(columns == NULL){
     fprintf(stderr, "Error: allocate buffer for edges (%ld) failed!", nlocaledges);
     MPI_Abort(MPI_COMM_WORLD, 1);
  }

  if(rank==0) fprintf(stdout, "begin make graph.\n");

  mr->map_key_value(mr, makegraph, NULL, NULL, 0);

  delete [] rowinserts;

  if(rank==0) {
     fprintf(stdout, "make CSR graph end.\n");
  }

  // make data structure
  int64_t bitmapsize = (nlocalverts + LONG_BITS - 1) / LONG_BITS;
  vis  = new unsigned long[bitmapsize];
  pred = new int64_t[nlocalverts];

  if(rank==0) fprintf(stdout, "BFS traversal start.\n");

  MPI_Barrier(MPI_COMM_WORLD);

  // Choose the root vertex
  root    = getrootvert();
  memset(vis, 0, sizeof(unsigned long)*(bitmapsize));
  for(int64_t i = 0; i < nlocalverts; i++){
    pred[i] = -1;
  }

  if(rank==0) fprintf(stdout, "Traversal start. (root=%ld)\n", root);

  // Inialize the child  vertexes of root
  mr->init_key_value(rootvisit, NULL);

  // BFS search
  int level = 0;
  do{
#ifdef KV_HINT
    mr->set_KVtype(FixedKV, sizeof(int64_t), 0);
#endif
    mr->map_key_value(mr, shrink, NULL, NULL, 0);
#ifdef KV_HINT
    mr->set_KVtype(FixedKV, sizeof(int64_t), sizeof(int64_t));
#endif
#ifdef COMPRESS
    nactives[level] = mr->map_key_value(mr, expand, compress);
#else
    nactives[level] = mr->map_key_value(mr, expand);
#endif
    level++;
  }while(nactives[level-1]);

  if(rank==0) {
    fprintf(stdout, "BFS traversal end.\n");
#ifdef OUTPUT_RESULT
    fprintf(rf, "%ld\n", root);
    for(int i=0;i<level;i++)
      fprintf(rf, "%ld\n", nactives[i]);
#endif
  }

  output("mtmr.bfs", outdir, prefix, mr);

  MPI_Barrier(MPI_COMM_WORLD);

  delete [] vis;
  delete [] pred;

  delete [] rowstarts;
  free(columns);
  delete mr;

#ifdef OUTPUT_RESULT
  if(rank==0){
    fclose(rf);
  }
#endif

  MPI_Finalize();
}

// partiton <key,value> based on the key
int64_t mypartition(char *key, int keybytes){
  int64_t v = *(int64_t*)key;
  if(v<quot*rem+rem)
    return v/(quot+1);
  else
    return (v-rem)/quot;
}

// count edge number of each vertex
void countedge(char *key, int keybytes, char *value, int valbytes,void *ptr){
  nlocaledges += 1;
  int64_t v0=*(int64_t*)key;
  rowstarts[v0-nvertoffset+1] += 1;
}

// read edge list from files
void fileread(MapReduce *mr, char *word, void *ptr)
{
  char sep[10] = " ";
  char *v0, *v1;
  char *saveptr = NULL;
  v0 = strtok_r(word,sep,&saveptr);
  v1 = strtok_r(NULL,sep,&saveptr);

  // skip self-loop edge
  if(strcmp(v0, v1) == 0){
    return;
  }
  int64_t int_v0 = strtoull(v0,NULL,0);
  int64_t int_v1 = strtoull(v1,NULL,0);
  if(int_v0>=nglobalverts || int_v1>=nglobalverts){
    fprintf(stderr, "The vertex index <%ld,%ld> is larger than maximum value %ld!\n", int_v0, int_v1, nglobalverts);
    exit(1);
  }
  mr->add_key_value((char*)&int_v0,sizeof(int64_t),(char*)&int_v1,sizeof(int64_t));
}

// make CSR graph based edge list
void makegraph(MapReduce *mr, char *key, int keybytes, char *value, int valuebytes, void *ptr){
  int64_t v0, v0_local, v1;
  v0 = *(int64_t*)key;
  v0_local = v0 - nvertoffset;

  v1=*(int64_t*)value;
  columns[rowstarts[v0_local]+rowinserts[v0_local]] = v1;
  rowinserts[v0_local]++;
}

// expand child vertexes of root
void rootvisit(MapReduce* mr, void *ptr){
  if(mypartition((char*)&root,sizeof(int64_t)) == rank){
    int64_t root_local = root-nvertoffset;
    pred[root_local] = root;
    SET_VISITED(root_local, vis);
    size_t p_end = rowstarts[root_local+1];
    for(size_t p = rowstarts[root_local]; p < p_end; p++){
      int64_t v1 = columns[p];
      mr->add_key_value((char*)&v1, sizeof(int64_t), (char*)&root, sizeof(int64_t));
    }
  }
}

// Keep active vertexes in next level only
void shrink(MapReduce *mr, char *key, int keybytes, char *value, int valuebytes, void *ptr){
  int64_t v = *(int64_t*)key;
  int64_t v_local = v - nvertoffset;

  int64_t v0 = *(int64_t*)value;

  if(!TEST_VISITED(v_local, vis)){
    SET_VISITED(v_local, vis);
    pred[v_local] = v0;
    mr->add_key_value(key, keybytes, NULL, 0);
  }
}

// Expand vertexes with the active vertexes
void expand(MapReduce *mr, char *key, int keybytes, char *value, int valuebytes, void *ptr){

  int64_t v = *(int64_t*)key;
  int64_t v_local = v-nvertoffset;

  size_t p_end = rowstarts[v_local+1];
  for(size_t p = rowstarts[v_local]; p < p_end; p++){
    int64_t v1 = columns[p];
    mr->add_key_value((char*)&v1, sizeof(int64_t), (char*)&v, sizeof(int64_t));
  }
}

// Compress KV with the sarank key
void compress(MapReduce *mr, char *key, int keysize, \
  char *val1, int val1size, char *val2, int val2size, void* ptr){

  mr->add_key_value(key, keysize, val1, val1size);
}

// Rondom chosen a vertex in the CC part of the graph
int64_t getrootvert(){
  int64_t myroot;
   // Get a root
  do{
    if(rank==0){
      myroot = rand() % nglobalverts;
    }
    MPI_Bcast((void*)&myroot, 1, MPI_INT64_T, 0, MPI_COMM_WORLD);
    int64_t myroot_proc=mypartition((char*)&myroot, (int)sizeof(int64_t));
    if(myroot_proc==rank){
      int64_t root_local=myroot-nvertoffset;
      if(rowstarts[root_local+1]-rowstarts[root_local]==0){
        myroot=-1;
      }
    }
    MPI_Bcast((void*)&myroot, 1, MPI_INT64_T,(int)myroot_proc, MPI_COMM_WORLD);
  }while(myroot==-1);
  return myroot;
}

#if 0
void printgraph(){
  for(int i = 0; i < nlocalverts; i++){
    int64_t v0 = rank*(nlocalverts)+i;
    printf("%ld", v0);
    size_t p_end = g->rowstarts[i+1];
    for(size_t p = g->rowstarts[i]; p < p_end; p++){
      int64_t v1 = g->columns[p];
      printf(" %ld", v1);
    }
    printf("\n");
  }
}
#endif

void printresult(int64_t *pred, size_t nlocalverts){
  for(size_t i = 0; i < nlocalverts; i++){
    size_t v = nlocalverts*rank+i;
    printf("%ld:%ld\n", v, pred[i]);
  }
}

void output(const char *filenarank, const char *outdir, const char *prefix, MapReduce *mr){
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
  //mr->print_stat(fp);
  MapReduce::print_stat(mr, fp);
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
