#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "cmapreduce.h"
#include "common.h"

int rank, size;

void map(void *mr, char *word, void *ptr);
void countword(void *, char *, int, void *);
void combiner(void *, const char *, int, const char *, int, const char *, int, void *);

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc < 5) {
        if (rank == 0)
            printf("Syntax: wordcount filepath prefix outdir tmpdir\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    char *filedir = argv[1];
    const char *prefix = argv[2];
    const char *outdir = argv[3];
    const char *tmpdir = argv[4];

    if (rank == 0) {
        printf("input dir=%s\n", filedir);
        printf("prefix=%s\n", prefix);
        printf("output dir=%s\n", outdir);
        printf("tmp dir=%s\n", tmpdir);
    }

    check_envars(rank, size);

    void *mr = create_mr_object(MPI_COMM_WORLD);

#ifdef COMBINE
    set_combiner(mr, combiner);
#endif
#ifdef KHINT
    set_key_length(mr, -1);
#endif
#ifdef VHINT
    set_value_length(mr, sizeof(int64_t));
#endif

    map_text_file(mr, filedir, 1, 1, " \n", map, NULL, 1);

    // mr->output(stdout, StringType, Int64Type);

    reduce(mr, countword, NULL);

    // mr->output(stdout, StringType, Int64Type);

    output(rank, size, prefix, outdir);

    destroy_mr_object(mr);

    MPI_Finalize();

    return 0;
}

void map(void *mr, char *word, void *ptr)
{
    // printf("word=%s\n", word);

    int len = (int) strlen(word) + 1;
    int64_t one = 1;
    if (len <= 1024)
        add_key_value(mr, word, len, (char *) &one, sizeof(one));
}

void countword(void *mr, char *key, int keysize, void *ptr)
{
    int64_t count = 0;

    const void *val = get_first_value(mr);
    while (val != NULL) {
        count += *(int64_t *) val;
        val = get_next_value(mr);
    }
    // for(iter->Begin(); !iter->Done(); iter->Next()){
    //    count+=*(int64_t*)iter->getValue();
    // printf("count=%ld\n", count);
    //}

    // printf("sum: key=%s, count=%ld\n", key, count);

    add_key_value(mr, key, keysize, (char *) &count, sizeof(count));
}

void combiner(void *mr, const char *key, int keysize, const char *val1,
              int val1size, const char *val2, int val2size, void *ptr)
{

    int64_t count = *(int64_t *) (val1) + *(int64_t *) (val2);

    update_key_value(mr, key, keysize, (char *) &count, sizeof(count));
}
