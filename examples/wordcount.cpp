#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <string.h>
#include <sys/stat.h>

#include "mapreduce.h"
#include "common.h"

using namespace MIMIR_NS;

int rank, size;

void map(MapReduce *mr, char *word, void *ptr);
void countword(MapReduce *, char *, int,  MultiValueIterator *iter, void*);
void combiner(MapReduce *, char *, int, \
    char *, int, char *, int, void*);

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if(argc <= 3){
        if(rank == 0)
            printf("Syntax: wordcount filepath prefix outdir\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    char *filedir=argv[1];
    const char *prefix = argv[2];
    const char *outdir = argv[3];

    check_envars(rank, size);

    MapReduce *mr = new MapReduce(MPI_COMM_WORLD);

#ifdef COMBINE
    mr->set_combiner(combiner);
#endif
#ifdef KHINT
    mr->set_key_length(-1);
#endif
#ifdef VHINT
    mr->set_value_length(sizeof(int64_t));
#endif

    mr->map_text_file(filedir, 1, 1, " \n", map, NULL); 

    mr->output(stdout, StringType, Int64Type);

    mr->reduce(countword, NULL);

    mr->output(stdout, StringType, Int64Type);

    output(rank, size, prefix, outdir);

    delete mr;

    MPI_Finalize();
}


void map(MapReduce *mr, char *word, void *ptr){
    //printf("word=%s\n", word);

    int len=(int)strlen(word)+1;
    int64_t one=1;
    if(len <= 1024)
        mr->add_key_value(word,len,(char*)&one,sizeof(one));
}

void countword(MapReduce *mr, char *key, int keysize, MultiValueIterator *iter, void* ptr){
    int64_t count=0;

    for(iter->Begin(); !iter->Done(); iter->Next()){
        count+=*(int64_t*)iter->getValue();
        //printf("count=%ld\n", count);
    }

    mr->add_key_value(key, keysize, (char*)&count, sizeof(count));
}

void combiner(MapReduce *mr, char *key, int keysize, \
    char *val1, int val1size, \
    char *val2, int val2size, void* ptr){

    int64_t count=*(int64_t*)(val1)+*(int64_t*)(val2);

    mr->update_key_value(key, keysize, (char*)&count, sizeof(count));
}

