#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include <omp.h>

#include "mapreduce.h"

using namespace MAPREDUCE_NS;

#define TEST_TIMES 10

#define BYTE_BITS 8
#define LONG_BITS (sizeof(unsigned long)*BYTE_BITS)

#define TEST_VISITED(v, vis) (((vis)[(v)/LONG_BITS]) & (1UL << ((v)%LONG_BITS)))
#define SET_VISITED(v, vis)  (__sync_fetch_and_or(&((vis)[(v)/LONG_BITS]), (1UL << ((v)%LONG_BITS))) & (1UL<<((v)%LONG_BITS)))

// graph partition
int mypartition_str(char *, int);
int mypartition_int(char *, int);

// read file to edges
void fileread(MapReduce *, const char *, void *);
// construct graph struct
void makegraph(MapReduce *, char *, int, int, char *, int *, void *);
// count local edge number
void countedge(char *, int, int, char *, int *, void* ptr);
// expand root vertex
void rootvisit(MapReduce *, void *);
// expand vertex
void expand(MapReduce *, char *, int, char *, int, void *);
// shrink vertex
void shrink(MapReduce *, char *, int, int, char *, int *,void *);
void shrink_mm(MapReduce *, char *, int, char *, int,void *);

// CSR graph
typedef struct _csr_graph{
  int      lg_nglobalverts; 
  int64_t  nglobalverts;     // global vertex count
  int64_t  nlocalverts;      // local vertex count
  int64_t  nglobaledges;     // global edge count
  int64_t  nlocaledges;      // local edge count
  size_t  *rowstarts;        // rowstarts
  int64_t *columns;          // columns

  size_t   *rowinserts;      // tmp buffer for construct csr
}csr_graph;

// print graph
void printgraph(csr_graph *);
// print results
void printresult(int64_t *pred, size_t nlocalverts);

// BFS state
typedef struct _bfs_state{
  csr_graph g;               // the graph
  unsigned long* vis;        // visited bitmap
  int64_t *pred;             // pred map
  int64_t root;              // root vertex
}bfs_state;

bfs_state st; 

int me, nprocs;

double wtime[TEST_TIMES], teps[TEST_TIMES];

#define MAX_LEVEL 10
int nactives[MAX_LEVEL];

FILE *rf=NULL;

int main(int narg, char **args)
{
  int provided;
  MPI_Init_thread(&narg, &args, MPI_THREAD_FUNNELED, &provided);
  if (provided < MPI_THREAD_FUNNELED) MPI_Abort(MPI_COMM_WORLD, 1);

//  MPI_Init(&narg, &args);

  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  // parse command-line args
  if(narg < 3){
    if(me == 0) printf("Syntax: bfs N infile1 {infile2...}");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  // create log file
  //char logfile[10];
  //sprintf(logfile, "log.%d.%d", me, nprocs);
  //FILE *fp = fopen(logfile, "w");

  // set vertex count
  csr_graph *g = &st.g;
  g->lg_nglobalverts = atoi(args[1]);
  g->nglobalverts = (1 << g->lg_nglobalverts);

  if(g->nglobalverts % nprocs != 0){
    if(me == 0) printf("ERROR: process size must be the power of 2\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if(me == 0){
    char rfile[100];
    sprintf(rfile, "mt_mr.result.%d.txt", g->lg_nglobalverts);
    rf = fopen(rfile, "w");
  }

  g->nlocalverts = g->nglobalverts / nprocs;

  //printf("");

  // create mapreduce
  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);

  // set hash function
  mr->sethash(mypartition_str);

  mr->setKVtype(1);

  //mr->setGlobalbufsize(16);
  mr->setBlocksize(64*1024);
  mr->setOutofcore(0);

  mr->setCommMode(0);

  if(me==0) fprintf(stdout, "make CSR graph start.\n");

  // make graph
  MPI_Barrier(MPI_COMM_WORLD);

  // read edge list
  int nedges = mr->map(args[2],1,0,fileread,&st);
  g->nglobaledges = nedges;

  //mr->output();

  //printf("begin convert\n");

  // convert edge list to kmv
  mr->convert();

  //printf("end convert\n");

  //mr->output();

  // initialize CSR structure
  g->rowstarts = new size_t[g->nlocalverts+1]; 
  g->rowinserts = new size_t[g->nlocalverts];
  g->rowstarts[0] = 0;
  for(int i = 0; i < g->nlocalverts; i++){
    g->rowstarts[i+1] = 0;
    g->rowinserts[i] =0;
  }

  g->nlocaledges = 0;

  // single thread is used to gather information
  mr->scan(countedge, &st);

  for(int i = 0; i < g->nlocalverts; i++){
    g->rowstarts[i+1] += g->rowstarts[i];
  }
  
  g->columns   = new int64_t[g->nlocaledges];

  // begin to make CSR graph
  mr->reduce(makegraph,&st);

  delete [] g->rowinserts;

  if(me==0) fprintf(stdout, "make CSR graph end.\n");

  int64_t *visit_roots = new int64_t[TEST_TIMES];

  srand(0);
  for(int i = 0; i < TEST_TIMES; i++){
    visit_roots[i] = rand() % (g->nglobalverts);
  }

  // print graph
  //printgraph(g);

  // begin do traversal
  int bitmapsize = (g->nlocalverts + LONG_BITS - 1) / LONG_BITS;

  // create structure
  st.vis  = new unsigned long[bitmapsize];
  st.pred = new int64_t[g->nlocalverts];

  if(me==0) fprintf(stdout, "BFS traversal start.\n");

  MPI_Barrier(MPI_COMM_WORLD);

  int ksize = (int)sizeof(int64_t);
  mr->sethash(mypartition_int);

  int test_count = 0;
  for(int index=0; index < TEST_TIMES; index++){
    if(me==0)
      fprintf(stdout, "Traversal %d start. (root=%ld)\n", index, visit_roots[index]);

    double map_t=0.0, convert_t=0.0, reduce_t=0.0;
    double start_t = MPI_Wtime();  

    // set root vertex
    st.root = visit_roots[index];
    memset(st.vis, 0, sizeof(unsigned long)*(bitmapsize));
    for(int i = 0; i < g->nlocalverts; i++){
      st.pred[i] = -1;
    }
 
    //uint64_t nactives = 0;
    mr->setKVtype(2, ksize, ksize);
    int count = mr->map(rootvisit, &st);
    if(count == 0) continue;
    //printf("map:\n");
    //mr->output(2);

    int level = 0;
    do{
      //printf("begin convert:\n");

      double t1 = MPI_Wtime();

#ifndef BFS_MM
      mr->convert();
      double t2 = MPI_Wtime();

      //printf("convert:\n");
      //mr->output(2);

      //printf("begin reduce:\n");
      mr->setKVtype(2, ksize, 0);
      mr->reduce(shrink, &st);
#else
      double t2 = MPI_Wtime();
      mr->setKVtype(2, ksize, 0);
      mr->map_local(mr, shrink_mm, &st);
#endif

      double t3 = MPI_Wtime();
      
      //printf("reduce:\n");
      //mr->output(2);

      //printf("begin map:\n");
      mr->setKVtype(2, ksize, ksize);
      nactives[level] = mr->map(mr, expand, &st);

      //printf("actives=%d\n", nactives[level]);

      //fprintf(stdout, "")

      double t4 = MPI_Wtime();

      map_t += (t4-t3);
      convert_t += (t2-t1);
      reduce_t += (t3-t2);

      //printf("map:\n");
      //mr->output(2);

      level++;
    }while(nactives[level-1]);

    double stop_t = MPI_Wtime();
   
    MPI_Barrier(MPI_COMM_WORLD);

    wtime[test_count] = stop_t - start_t;
    test_count++;    

    if(me==0){
      fprintf(rf, "%ld\n", st.root);
      fprintf(rf, "%d\n", level);
      for(int k =0; k < level; k++){
        fprintf(rf, "%d\n", nactives[k]);
      }
      fprintf(rf, "\n");
      fprintf(stdout, "Traversal %d end. (time=%g s %g %g %g)\n", index, stop_t-start_t, map_t, convert_t, reduce_t);
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);

  if(me==0) fprintf(stdout, "BFS traversal end.\n");

  if(me==0){
    double avg_teps=0.0;
    for(int i=0; i<test_count; i++){
      teps[i] = (g->nglobaledges)/wtime[i];
      avg_teps += teps[i];
    }
    avg_teps /= test_count;

    double max_teps=teps[0],min_teps=teps[0];
    for(int i=1; i<test_count; i++){
      if(teps[i] > max_teps) max_teps = teps[i];
      if(teps[i] < min_teps) min_teps = teps[i];
    }

    fprintf(stdout, "process count=%d, vertex count=%ld, edge count=%ld\n", nprocs, g->nglobalverts, g->nglobaledges);
    fprintf(stdout, "Results: average=%g, max=%g, min=%g\n", avg_teps, max_teps, min_teps);

  }

  delete [] visit_roots;

  // delete buffers
  delete [] st.vis;
  delete [] st.pred; 

  delete [] g->rowstarts;
  delete [] g->columns;

  delete mr;

  if(me==0){
    fclose(rf);
  }

  // close log file
  //fclose(fp);

  MPI_Finalize();
}

int mypartition_str(char *key, int keybytes){
  int64_t v = atoi(key) - 1;

  return v/(st.g.nlocalverts);
}

int mypartition_int(char *key, int keybytes){
  int64_t v = *(int64_t*)key; 

  return v/(st.g.nlocalverts);
}

void fileread(MapReduce *mr, const char *fname, void *ptr){
  //int tid = omp_get_thread_num();
  //printf("%d input file=%s\n", tid, fname);
  bfs_state *st = (bfs_state*)ptr;
  csr_graph *g = &(st->g);

  struct stat stbuf;
  int flag = stat(fname,&stbuf);
  if(flag < 0){
    printf("ERROR: Counld not query file size\n");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  int filesize = stbuf.st_size;

  FILE *fp = fopen(fname, "r");
  char *text = new char[filesize+1];
  int nchar = fread(text,1,filesize,fp);
  text[nchar] = '\0';
  fclose(fp);

  char *v0=NULL, *v1=NULL, *val;
  
  char *line = text;
  int linesize;

  while(line && line[0]!='\0'){
    // replace '\n' with '\0'
    char *p = strchr(line, '\n');
    if(p) p[0]='\0';
    // get line size
    linesize = strlen(line)+1;

    // get v0,v1,val
    v0 = line;
    v1 = strchr(v0, ' ');
    v1[0] = '\0';
    v1 = v1+1;
    val = strchr(v1, ' ');
    val[0] = '\0';
    val = val +1;

    if(atoi(v0)<=0 || atoi(v0) > g->nglobalverts){
      fprintf(stderr, "Error: vertex %s is larger than max vertex index!\n", v0);
      exit(1);
    }

    if(atoi(v1)<=0 || atoi(v1) > g->nglobalverts){
      fprintf(stderr, "Error: vertex %s is larger than max vertex index!\n", v1);
      exit(1);
    }

    if(strcmp(v0, v1) == 0){
      line += linesize;
      continue;
    }

    //printf("%s,%s,%s\n", v0, v1, val);
    mr->add(v0, strlen(v0)+1, v1, strlen(v1)+1);
    mr->add(v1, strlen(v1)+1, v0, strlen(v0)+1);    

    line += linesize;
  }

  delete [] text;
}

void makegraph(MapReduce *mr, char *key, int keybytes, int nvalues,char *multivalue,  int *valuebytes, void *ptr){
 // printf("key=%s, multivalue=%s\n", key, multivalue);

  // get graph strcuture
  bfs_state *st = (bfs_state*)ptr;
  csr_graph *g = &(st->g);

  int64_t v0, v0_local, v1;
  char *value;
  int offset=0;

  v0 = atoi(key)-1;
  v0_local = v0 % (g->nlocalverts);  

  //printf("key=%s\n", key);

  // different threads handle different vertex
  for(int i = 0; i < nvalues;i++){
    value = multivalue+offset;
    //printf("value=%s\n", value);
    v1 = atoi(value)-1;
    g->columns[g->rowstarts[v0_local]+g->rowinserts[v0_local]] = v1;
    g->rowinserts[v0_local]++;
    //offset += valuebytes[i];
    offset += strlen(value)+1;
  }
}

void countedge(char *key, int keybytes, int nval, char *multivalue, int *valuebyte, void* ptr){
  bfs_state *st = (bfs_state*)ptr;
  csr_graph *g = &(st->g);

  //printf("countedge, key=%s, n=%d\n", key, nvalues);

  g->nlocaledges += nval;

  int64_t v0 = atoi(key)-1;
  g->rowstarts[v0%(g->nlocalverts)+1] = nval;
}

void rootvisit(MapReduce *mr, void *ptr){
  bfs_state *st = (bfs_state*)ptr;
  csr_graph *g = &(st->g);

  if((st->root)/(g->nlocalverts) == me){

    int64_t root_local = (st->root)%(g->nlocalverts);
    
    st->pred[root_local] = st->root;
    SET_VISITED(root_local, (st->vis));

    size_t p_end = g->rowstarts[root_local+1];
    for(size_t p = g->rowstarts[root_local]; p < p_end; p++){
      int64_t v = g->columns[p];
      //printf("%ld, %ld\n", v, st->root);
      mr->add((char*)&v, sizeof(int64_t), (char*)&(st->root), sizeof(int64_t));
    } 
  }
}

void expand(MapReduce *mr, char *key, int keybytes, char *value, int valuebytes, void *ptr){
  bfs_state *st = (bfs_state*)ptr;
  csr_graph *g = &(st->g);
 
  int64_t v = *(int64_t*)key;
  int64_t v_local = v%(g->nlocalverts);

  size_t p_end = g->rowstarts[v_local+1];
  for(size_t p = g->rowstarts[v_local]; p < p_end; p++){
    int64_t v1 = g->columns[p];
    mr->add((char*)&v1, sizeof(int64_t), (char*)&v, sizeof(int64_t));
  }
}

void shrink(MapReduce *mr, char *key, int keybytes, int nvaluse, char *multivalue, int *valuebytes,void *ptr){
  bfs_state *st = (bfs_state*)ptr;
  csr_graph *g = &(st->g);

  int64_t v = *(int64_t*)key;
  int64_t v_local = v % (g->nlocalverts);

  int64_t v0 = *(int64_t*)multivalue;
 
  if(!TEST_VISITED(v_local, st->vis)){  
    if(SET_VISITED(v_local, st->vis)==0){
      //printf("key=%s, keybytes=%d\n", key, keybytes);
      st->pred[v_local] = v0;
      mr->add(key, keybytes, NULL, 0);
    }
  }
}

void shrink_mm(MapReduce *mr, char *key, int keybytes, char *value, int valuebytes,void *ptr){
  bfs_state *st = (bfs_state*)ptr;
  csr_graph *g = &(st->g);

  int64_t v = *(int64_t*)key;
  int64_t v_local = v % (g->nlocalverts);

  int64_t v0 = *(int64_t*)value;
 
  if(!TEST_VISITED(v_local, st->vis)){  
    if(SET_VISITED(v_local, st->vis)==0){
      //printf("%ld\n", v);
      st->pred[v_local] = v0;
      mr->add(key, keybytes, NULL, 0);
    }
  }
}

void printgraph(csr_graph *g){
  for(int i = 0; i < g->nlocalverts; i++){
    int64_t v0 = me*(g->nlocalverts)+i;
    printf("%ld", v0);
    size_t p_end = g->rowstarts[i+1];
    for(size_t p = g->rowstarts[i]; p < p_end; p++){
      int64_t v1 = g->columns[p];
      printf(" %ld", v1);
    }
    printf("\n");
  }
}

void printresult(int64_t *pred, size_t nlocalverts){
  for(size_t i = 0; i < nlocalverts; i++){
    size_t v = nlocalverts*me+i;
    printf("%ld:%ld\n", v, pred[i]);
  }
}
