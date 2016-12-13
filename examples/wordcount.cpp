#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <string.h>
#include <sys/stat.h>

#include "mapreduce.h"
#include "common.h"

//#define VALUE_STRING

using namespace MIMIR_NS;

int rank, size;

void map(MapReduce *mr, char *word, void *ptr);
void countword(MapReduce *, char *, int,  void*);
void combiner(MapReduce *, const char *, int, \
    const char *, int, const char *, int, void*);

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if(argc < 5){
        if(rank == 0)
            printf("Syntax: wordcount filepath prefix outdir tmpdir\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    char *filedir=argv[1];
    const char *prefix = argv[2];
    const char *outdir = argv[3];
    const char *tmpdir = argv[4];

    if(rank==0){
        printf("input dir=%s\n", filedir);
        printf("prefix=%s\n", prefix);
        printf("output dir=%s\n", outdir);
        printf("tmp dir=%s\n", tmpdir); 
    }

    check_envars(rank, size);

    MapReduce *mr = new MapReduce(MPI_COMM_WORLD);

#ifdef COMBINE
    mr->set_combiner(combiner);
#endif
#ifdef KHINT
    mr->set_key_length(-1);
#endif
#ifdef VHINT
#ifdef VALUE_STRING
    mr->set_value_length(-1);
#else
    mr->set_value_length(sizeof(int64_t));
#endif
#endif

    uint64_t nwords=mr->map_text_file(filedir, 1, 1, " \n", map, NULL); 

    //mr->output(stdout, StringType, Int64Type);

    uint64_t nunique=mr->reduce(countword, NULL);

    fprintf(stdout, "number of words=%ld, number of unique words=%ld\n", nwords, nunique);

    //mr->output(stdout, StringType, StringType);

    output(rank, size, prefix, outdir);

    delete mr;

    MPI_Finalize();
}


void map(MapReduce *mr, char *word, void *ptr){
    int len=(int)strlen(word)+1;
    if(len <= 1024){
#ifdef VALUE_STRING
        char tmp[10]={"1"};
        mr->add_key_value(word,len,tmp,2);
#else
        int64_t one=1;
        mr->add_key_value(word,len,(char*)&one,sizeof(one));
#endif
    }
}

void countword(MapReduce *mr, char *key, int keysize, void* ptr){
    int64_t count=0;

    const void *val=mr->get_first_value();
    while(val != NULL){
#ifdef VALUE_STRING
        count+=strtoull((const char*)val, NULL, 0);
#else
        count+=*(int64_t*)val;
#endif
        val=mr->get_next_value();
    }

#ifdef VALUE_STRING
    char tmp[20]={0};
    sprintf(tmp, "%ld", count);
    mr->add_key_value(key, keysize, tmp, (int)strlen(tmp)+1);
#else
    mr->add_key_value(key, keysize, (char*)&count, sizeof(count));
#endif
}

void combiner(MapReduce *mr, const char *key, int keysize, \
    const char *val1, int val1size, \
    const char *val2, int val2size, void* ptr){

#ifdef VALUE_STRING
    int64_t count=strtoull(val1, NULL, 0)+strtoull(val2, NULL, 0);
    char tmp[20]={0};
    sprintf(tmp, "%ld", count);
    mr->update_key_value(key, keysize, tmp, (int)strlen(tmp)+1);
#else
    int64_t count=*(int64_t*)(val1)+*(int64_t*)(val2);
    mr->update_key_value(key, keysize, (char*)&count, sizeof(count));
#endif
}

