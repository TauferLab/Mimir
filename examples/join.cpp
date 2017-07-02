/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <stdint.h>

#include "mimir.h"

using namespace MIMIR_NS;

int rank, size;

struct SingleVal {
    int         tag;
    int64_t     val;
};

struct JoinedVal {
    int64_t     val1;
    int64_t     val2;
};

void map (Readable<char*,void> *input,
          Writable<char*,SingleVal> *output, void *ptr);
void reduce (Readable<char*,SingleVal> *input,
             Writable<char*,JoinedVal> *output, void *ptr);

int main (int argc, char *argv[])
{
    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc < 3) {
        if (rank == 0)
            fprintf(stdout, "Usage: %s output input ...\n", argv[0]);
        return 0;
    }

    std::string output = argv[1];
    std::vector<std::string> input;
    for (int i = 2; i < argc; i++) {
        input.push_back(argv[i]);
    }
    MimirContext<char*, SingleVal, char*, void, char*, JoinedVal>* ctx 
        = new MimirContext<char*, SingleVal, char*, void, char*, JoinedVal>(MPI_COMM_WORLD,
                                           map, reduce, input, output);
    ctx->map();
    ctx->reduce();
    //ctx->output(outputkv);
    delete ctx;

    MPI_Finalize();
}

void map (Readable<char*,void> *input,
          Writable<char*,SingleVal> *output, void *ptr)
{
    char      *line = NULL;
    char      *key = NULL;
    SingleVal  val;

    while (input->read(&line, NULL) == 0) {

        char *saveptr = NULL, *word = NULL;

        printf("line=%s\n", line);

        word = strtok_r(line, " ", &saveptr);
        if (word == NULL) printf("Input file format error!\n");
        val.tag = atoi(word);

        word = strtok_r(NULL, " ", &saveptr);
        if (word == NULL) printf("Input file format error!\n");
        key = word;

        word = strtok_r(NULL, " ", &saveptr);
        if (word == NULL) printf("Input file format error!\n");
        val.val = strtoull(word, NULL, 0);

        output->write(&key, &val);
    }
}

void reduce (Readable<char*,SingleVal> *input,
             Writable<char*,JoinedVal> *output, void *ptr) 
{
    std::vector<int64_t> db1;
    //std::vector<int64_t> db2;

    char *key = NULL;
    SingleVal val;
    while (input->read(&key, &val) == 0) {
        if (val.tag == 0) db1.push_back(val.val);
        //else if (val.tag == 1) db2.push_back(val.val);
    }

    while (input->read(&key, &val) == 0) {
        if (val.tag == 0) db1.push_back(val.val);
        //else if (val.tag == 1) db2.push_back(val.val);
        auto iter1 = db1.begin();
        for (; iter1 != db1.end(); iter1 ++) {
            JoinedVal jval;
            jval.val1 = *iter1;
            jval.val2 = val.val;
            printf("key=%s, val={%ld, %ld}\n", key, jval.val1, jval.val2);
            output->write(&key, &jval);
        }
    }

    //auto iter1 = db1.begin();
    //for (; iter1 != db1.end(); iter1 ++) {
    //    auto iter2 = db2.begin();
    //    for (; iter2 != db2.end(); iter2 ++) {
    //        JoinedVal jval;
    //        jval.val1 = *iter1;
    //        jval.val2 = *iter2;
    //        printf("key=%s, val={%ld, %ld}\n", key, jval.val1, jval.val2);
    //        output->write(&key, &jval);
    //    }
    //}
}
