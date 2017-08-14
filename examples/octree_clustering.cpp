/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mpi.h>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/stat.h>

#include "common.h"
#include "mimir.h"

using namespace MIMIR_NS;
int rank, size;

void generate_octkey (Readable<char*, void> *input,
                      Writable<char, uint64_t> *output, void *ptr);
void gen_leveled_octkey (Readable<char, uint64_t> *input,
                         Writable<char, uint64_t> *output, void *ptr);
void combine (Combinable<char, uint64_t> *combiner,
              char *key, uint64_t *val1, uint64_t *val2, 
              uint64_t *rval, void *ptr);
void sum (Readable<char, uint64_t> *input,
          Writable<char, uint64_t> *output, void *ptr);

#define digits 15
uint64_t thresh = 5;
int level;

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc < 4) {
        if (rank == 0) printf("Usage: %s density output input ...\n", argv[0]);
        return 0;
    }

    double density = atof(argv[1]);
    std::string output = argv[2];
    std::vector<std::string> input;
    for (int i = 3; i < argc; i++) {
        input.push_back(argv[i]);
    }

    int min_limit, max_limit;
    min_limit = 0;
    max_limit = digits + 1;
    level = (int) floor((max_limit + min_limit) / 2);

    MimirContext<char, uint64_t, char*, void> *ctx 
        = new MimirContext<char, uint64_t, char*, void>(input, output,
                                                        MPI_COMM_WORLD,
                                                        NULL, NULL, NULL,
                                                        digits, 1, 1, 1);

    uint64_t nwords = ctx->map(generate_octkey, NULL, false);
    thresh = (int64_t) ((float) nwords * density);

    if (rank == 0) printf("nwords=%ld\n", nwords);

    while ((min_limit + 1) != max_limit) {

        MimirContext<char, uint64_t, char, uint64_t> *level_ctx 
            = new MimirContext<char, uint64_t, char, uint64_t>(std::vector<std::string>(),
                                                               std::string(),
                                                               MPI_COMM_WORLD,
#ifdef COMBINER
                                                               combine,
#else
                                                               NULL,
#endif
                                                               NULL, NULL,
                                                               level, 1,
                                                               digits, 1);

        level_ctx->insert_data_handle(ctx->get_data_handle());
        uint64_t totalkv = level_ctx->map(gen_leveled_octkey);
        uint64_t nkv = level_ctx->reduce(sum);

	if (rank == 0) printf("level=%d, totalkv=%ld, nkv=%ld\n", level, totalkv, nkv);

        if (nkv > 0) {
            min_limit = level;
            level = (int) floor((max_limit + min_limit) / 2);
        }
        else {
            max_limit = level;
            level = (int) floor((max_limit + min_limit) / 2);
        }

        delete level_ctx;
    }

    delete ctx;

    if (rank == 0) printf("level=%d\n", level);

    MPI_Finalize();
}

void combine (Combinable<char, uint64_t> *combiner,
              char *key, uint64_t *val1, uint64_t *val2,
              uint64_t *rval, void *ptr)
{
    *rval = *(val1) + *(val2);
}

void sum (Readable<char, uint64_t> *input,
          Writable<char, uint64_t> *output, void *ptr)
{
    char key[digits];
    uint64_t val = 0;
    uint64_t sum = 0;

    while ((input->read(key, &val)) == 0) {
        sum += val;
    }

    if (sum > thresh) {
        output->write(key, &sum);
    }

}

void gen_leveled_octkey (Readable<char, uint64_t> *input,
                         Writable<char, uint64_t> *output, void *ptr)
{
    char key[digits];;
    uint64_t val = 0;
    uint64_t count = 1;
    while ((input->read(key, &val)) == 0) {
	output->write(key, &count);
    }
}

void generate_octkey (Readable<char*, void> *input,
                      Writable<char, uint64_t> *output, void *ptr)
{
    char *word = NULL;
    char octkey[digits];
    uint64_t count = 0;

    while ((input->read(&word, NULL)) == 0) {

        double range_up = 4.0, range_down = -4.0;

        double b0, b1, b2;
        char *saveptr = NULL;
        char *token = strtok_r(word, " ", &saveptr);
	b0 = atof(token);
	token = strtok_r(NULL, " ", &saveptr);
        b1 = atof(token);
        token = strtok_r(NULL, " ", &saveptr);
        b2 = atof(token);

        double minx = range_down, miny = range_down, minz = range_down;
        double maxx = range_up, maxy = range_up, maxz = range_up;

	count = 0;        
	while (count < digits) {
            
            int m0 = 0, m1 = 0, m2 = 0;
            double rankdx = minx + ((maxx - minx) / 2);
            if (b0 > rankdx) {
                m0 = 1;
                minx = rankdx;
            }
            else {
                maxx = rankdx;
            }

            double rankdy = miny + ((maxy - miny) / 2);
            if (b1 > rankdy) {
                m1 = 1;
                miny = rankdy;
            }
            else {
                maxy = rankdy;
            }

            double rankdz = minz + ((maxz - minz) / 2);
            if (b2 > rankdz) {
                m2 = 1;
                minz = rankdz;
            }
            else {
                maxz = rankdz;
            }

            int bit = m0 + (m1 * 2) + (m2 * 4);
	    octkey[count] = (char)(bit & 0x7f);
            ++count;
        }
        //octkey[digits+1] = '\0';
        //for (int i = 0; i < digits; i++)
        //    printf("%x", octkey[i]);
        //printf("\n");
        output->write(octkey, &count);
    }
}
