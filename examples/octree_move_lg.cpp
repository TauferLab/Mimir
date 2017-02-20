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

void generate_octkey(Readable *input, Writable *output, void *ptr);
void gen_leveled_octkey(Readable *input, Writable *output, void *ptr);
void combiner(Combinable *combiner, KVRecord *kv1, KVRecord *kv2, void *ptr);
void sum(Readable *input, Writable *output, void *ptr);
double slope(double[], double[], int);

#define digits 15
int64_t thresh = 5;
bool realdata = false;
int level;

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    Mimir_Init(&argc, &argv, MPI_COMM_WORLD);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc < 6) {
        if (rank == 0)
            printf("Syntax: octree_move_lg threshold indir prefix outdir\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    double density = atof(argv[1]);
    char *indir = argv[2];
    char *prefix = argv[3];
    char *outdir = argv[4];
    const char *tmpdir = argv[5];

    if (rank == 0) {
        printf("density=%f\n", density);
        printf("input dir=%s\n", indir);
        printf("prefix=%s\n", prefix);
        printf("output dir=%s\n", outdir);
        printf("tmp dir=%s\n", tmpdir);
    }

    check_envars(rank, size);

    int min_limit, max_limit;
    min_limit = 0;
    max_limit = digits + 1;
    level = (int) floor((max_limit + min_limit) / 2);

    MimirContext mimir;

#ifdef KVHINT
    mimir.set_key_length(digits);
    mimir.set_value_length(0);
#endif

    InputSplit* splitinput = FileSplitter::getFileSplitter()->split(indir);
    StringRecord::set_whitespace("\n");
    FileReader<StringRecord> reader(splitinput);
    KVContainer octkeys;
    mimir.set_map_callback(generate_octkey);
    mimir.set_shuffle_flag(false);
    uint64_t ret = mimir.mapreduce(&reader, &octkeys);
    uint64_t nwords = 0;
    MPI_Allreduce(&ret, &nwords, 1, MPI_UINT64_T, 
                  MPI_SUM, MPI_COMM_WORLD);
    thresh = (int64_t) ((float) nwords * density);
    if (rank == 0) {
        printf("Command line: input path=%s, thresh=%ld\n", indir, thresh);
    }

    mimir.set_map_callback(gen_leveled_octkey);
    mimir.set_reduce_callback(sum);
    mimir.set_shuffle_flag(true);
#ifdef COMBINE
    mimir.set_combine_callback(combiner);
#endif

    while ((min_limit + 1) != max_limit) {
#ifdef KVHINT
        mimir.set_key_length(level);
        mimir.set_value_length(sizeof(int64_t));
#endif
        KVContainer loctkeys;
        uint64_t ret = mimir.mapreduce(&octkeys, &loctkeys);
        uint64_t nkv = 0;
        MPI_Allreduce(&ret, &nkv, 1, MPI_UINT64_T, 
                      MPI_SUM, MPI_COMM_WORLD);

        if (nkv > 0) {
            min_limit = level;
            level = (int) floor((max_limit + min_limit) / 2);
        }
        else {
            max_limit = level;
            level = (int) floor((max_limit + min_limit) / 2);
        }
    }

    char newprefix[1000];
    sprintf(newprefix, "%s-d%.2f", prefix, density);
    output(rank, size, newprefix, outdir);

    if (rank == 0)
        printf("level=%d\n", level);

    Mimir_Finalize();
    MPI_Finalize();
}

void combiner(Combinable *combiner, KVRecord *kv1, KVRecord *kv2, void *ptr)
{
    int64_t count = *(int64_t *) (kv1->get_val()) 
                  + *(int64_t *) (kv2->get_val());
    KVRecord update_record(kv1->get_key(),
                           kv1->get_key_size(),
                           (char *) &count, sizeof(count));
    combiner->update(&update_record);
}

void sum(Readable *input, Writable *output, void *ptr)
{
    int64_t sum = 0;
    KMVRecord *kmv = NULL;
    char *val = NULL;

    BaseRecordFormat *input_record = NULL;
    while ((input_record = input->read()) != NULL) {
        kmv = (KMVRecord*)input_record;
        val = NULL;
        sum = 0;
        while ((val = kmv->get_next_val()) != NULL) {
            sum += *(int64_t *)val;
        }
        if (sum > thresh) {
            KVRecord output_record(kmv->get_key(), kmv->get_key_size(),
                           (char *) &sum, sizeof(sum));
            output->write(&output_record);
        }
    }
}

void gen_leveled_octkey(Readable *input, Writable *output, void *ptr)
{
    int64_t count = 1;
    BaseRecordFormat *input_record = NULL;
    while ((input_record = input->read()) != NULL) {
        KVRecord *kv = (KVRecord*)input_record;
        KVRecord output_record(kv->get_key(), level, 
                               (char*)&count, sizeof(int64_t));
        output->write(&output_record);
    }
}

void generate_octkey(Readable *input, Writable *output, void *ptr)
{
    BaseRecordFormat *record = NULL;
    while ((record = input->read()) != NULL) {
        char *word = record->get_record();
        double range_up = 4.0, range_down = -4.0;
        char octkey[digits];

        double b0, b1, b2;
        char *saveptr;
        char *token = strtok_r(word, " ", &saveptr);
        b0 = atof(token);
        token = strtok_r(word, " ", &saveptr);
        b1 = atof(token);
        token = strtok_r(word, " ", &saveptr);
        b2 = atof(token);

        int count = 0;              // count how many digits are in the octkey
        double minx = range_down, miny = range_down, minz = range_down;
        double maxx = range_up, maxy = range_up, maxz = range_up;
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
            octkey[count] = bit & 0x7f;
            ++count;
        }

        KVRecord output_record(octkey, digits, NULL, 0);
        output->write(&output_record);
    }
}

double slope(double x[], double y[], int num_atoms)
{
    double slope = 0.0;
    double sumx = 0.0, sumy = 0.0;
    for (int i = 0; i != num_atoms; ++i) {
        sumx += x[i];
        sumy += y[i];
    }

    double xbar = sumx / num_atoms;
    double ybar = sumy / num_atoms;

    double xxbar = 0.0, yybar = 0.0, xybar = 0.0;
    for (int i = 0; i != num_atoms; ++i) {
        xxbar += (x[i] - xbar) * (x[i] - xbar);
        yybar += (y[i] - ybar) * (y[i] - ybar);
        xybar += (x[i] - xbar) * (y[i] - ybar);
    }

    slope = xybar / xxbar;
    return slope;
}
