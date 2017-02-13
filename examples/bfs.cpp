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
#include <string.h>
#include <string>
#include <sys/stat.h>

#include "common.h"
#include "mimir.h"

using namespace MIMIR_NS;

//#define COLUMN_SINGLE_BUFFER

#define BYTE_BITS 8
#define LONG_BITS (sizeof(unsigned long) * BYTE_BITS)
#define TEST_VISITED(v, vis)                                                   \
    ((vis[(v) / LONG_BITS]) & (1UL << ((v) % LONG_BITS)))
#define SET_VISITED(v, vis)                                                    \
    ((vis[(v) / LONG_BITS]) |= (1UL << ((v) % LONG_BITS)))

int mypartition(const char *, int);
void fileread(Readable *input, Writable *output, void *ptr);
void makegraph(KVContainer *container);
void countedge(KVContainer *container);

int64_t getrootvert();
void rootvisit(Readable *input, Writable *output, void *ptr);
void expand(Readable *input, Writable *output, void *ptr);
void combiner(Combinable *combiner, KVRecord *kv1, KVRecord *kv2, void *ptr);

int rank, size;
int64_t nglobalverts;           // global vertex count
int64_t nglobaledges;           // global edge count
int64_t nlocalverts;            // local vertex count
int64_t nlocaledges;            // local edge count
int64_t nvertoffset;            // local vertex's offset
int64_t quot, rem;              // quotient and reminder of globalverts/size

size_t *rowstarts;              // rowstarts

#ifndef COLUMN_SINGLE_BUFFER
int ncolumn = 0;
int64_t ncolumnedge = 8 * 1024 * 1024;
int64_t **columns;              // columns
#define GET_COL_IDX(pos) ((pos) / (ncolumnedge))
#define GET_COL_OFF(pos) ((pos) % (ncolumnedge))
#else
int64_t *columns;               // columns
#endif

unsigned long *vis;             // visited bitmap
int64_t *pred;                  // pred map
int64_t root;                   // root vertex
size_t *rowinserts;             // tmp buffer for construct CSR

#define MAX_LEVEL 100
uint64_t nactives[MAX_LEVEL];

#ifdef OUTPUT_RESULT
FILE *rf = NULL;
#endif

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    Mimir_Init(&argc, &argv, MPI_COMM_WORLD);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Get pararankters
    if (argc < 7) {
        if (rank == 0)
            printf("Syntax: bfs N indir prefix outdir seed\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    nglobalverts = strtoull(argv[1], NULL, 0);
    char *indir = argv[2];
    char *prefix = argv[3];
    char *outdir = argv[4];
    const char *tmpdir = argv[5];
    srand(atoi(argv[6]));

    if (rank == 0) {
        printf("globalverts=%ld\n", nglobalverts);
        printf("input dir=%s\n", indir);
        printf("prefix=%s\n", prefix);
        printf("output dir=%s\n", outdir);
        printf("tmp dir=%s\n", tmpdir);
        printf("index=%d\n", atoi(argv[6]));
    }

    check_envars(rank, size);

    // compute vertex partition range
    quot = nglobalverts / size;
    rem = nglobalverts % size;
    if (rank < rem) {
        nlocalverts = quot + 1;
        nvertoffset = rank * (quot + 1);
    }
    else {
        nlocalverts = quot;
        nvertoffset = rank * quot + rem;
    }

#ifdef OUTPUT_RESULT
    if (rank == 0) {
        char rfile[100];
        sprintf(rfile, "%s_result.txt", prefix);
        rf = fopen(rfile, "a+");
    }
#endif

    MimirContext mimir;
    //MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
    mimir.set_hash_callback(mypartition);

    if (rank == 0)
        fprintf(stdout, "make CSR graph start.\n");

#ifdef KVHINT
    mimir.set_key_length(sizeof(int64_t));
    mimir.set_value_length(sizeof(int64_t));
#endif

    // partition file
    InputSplit* splitinput = FileSplitter::getFileSplitter()->split(indir);
    StringRecord::set_whitespace("\n");
    FileReader<StringRecord> reader(splitinput);
    KVContainer *edges_container = new KVContainer();
    mimir.set_map_callback(fileread);

    uint64_t nedges = mimir.mapreduce(&reader, edges_container, NULL);
    nglobaledges = nedges;

    // mr->output(stdout, Int64Type, Int64Type);

    rowstarts = new size_t[nlocalverts + 1];
    rowinserts = new size_t[nlocalverts];
    rowstarts[0] = 0;
    for (int64_t i = 0; i < nlocalverts; i++) {
        rowstarts[i + 1] = 0;
        rowinserts[i] = 0;
    }
    nlocaledges = 0;
    countedge(edges_container);

    for (int64_t i = 0; i < nlocalverts; i++) {
        rowstarts[i + 1] += rowstarts[i];
    }

    if (rank == 0)
        fprintf(stdout, "local edge=%ld\n", nlocaledges);

    // columns=(int64_t*)malloc(nlocaledges*sizeof(int64_t));
#ifndef COLUMN_SINGLE_BUFFER
    ncolumn = (int) (nlocaledges / ncolumnedge) + 1;
    if (rank == 0) {
        fprintf(stdout, "ncolumn=%d\n", ncolumn);
        fflush(stdout);
    }
    int64_t edge_left = nlocaledges;
    columns = new int64_t*[ncolumn];
    for (int i = 0; i < ncolumn - 1; i++) {
        columns[i] = (int64_t *) mem_aligned_malloc(4096, ncolumnedge * sizeof(int64_t));
        if (columns[i] == NULL) {
            fprintf(stderr, "Error: allocate buffer for edges (%ld) failed!\n", nlocaledges);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        edge_left -= ncolumnedge;
    }
    columns[ncolumn - 1] = (int64_t *) mem_aligned_malloc(4096, edge_left * sizeof(int64_t));
#else
    columns = (int64_t *) mem_aligned_malloc(4096, nlocaledges * sizeof(int64_t));
    if (columns == NULL) {
        fprintf(stderr, "Error: allocate buffer for edges (%ld) failed!\n", nlocaledges);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
#endif

    if (rank == 0)
        fprintf(stdout, "begin make graph.\n");

    makegraph(edges_container);
    delete edges_container;
    delete[] rowinserts;

    // mr->output(stdout, Int64Type, Int64Type);

    if (rank == 0) {
        fprintf(stdout, "make CSR graph end.\n");
    }

    // make data structure
    int64_t bitmapsize = (nlocalverts + LONG_BITS - 1) / LONG_BITS;
    vis = new unsigned long[bitmapsize];
    pred = new int64_t[nlocalverts];

    if (rank == 0)
        fprintf(stdout, "BFS traversal start.\n");

    MPI_Barrier(MPI_COMM_WORLD);

    // Choose the root vertex
    root = getrootvert();
    memset(vis, 0, sizeof(unsigned long) * (bitmapsize));
    for (int64_t i = 0; i < nlocalverts; i++) {
        pred[i] = -1;
    }

    if (rank == 0)
        fprintf(stdout, "Traversal start. (root=%ld)\n", root);

#ifdef COMBINE
    mimir.set_combine_callback(combiner);
#endif

    // Inialize the child  vertexes of root
    //mr->init_key_value(rootvisit, NULL);
    KVContainer *in_container = new KVContainer();
    KVContainer *out_container = NULL;

    mimir.set_map_callback(rootvisit);
    mimir.mapreduce(NULL, in_container);

    // mr->output(stdout, Int64Type, Int64Type);

    mimir.set_map_callback(expand);

    // BFS search
    int level = 0;
    do {
        //nactives[level] = mr->map_key_value(mr, expand);
        out_container = new KVContainer();
        nactives[level] = mimir.mapreduce(in_container, out_container);
        delete in_container;
        in_container = out_container;
        // mr->output(stdout, Int64Type, Int64Type);
        level++;
    } while (nactives[level - 1]);

    delete in_container;

    if (rank == 0) {
        fprintf(stdout, "BFS traversal end.\n");
#ifdef OUTPUT_RESULT
        fprintf(rf, "%ld\n", root);
        for (int i = 0; i < level; i++)
            fprintf(rf, "%ld\n", nactives[i]);
#endif
    }

    MPI_Barrier(MPI_COMM_WORLD);

    delete[] vis;
    delete[] pred;

    delete[] rowstarts;
#ifndef COLUMN_SINGLE_BUFFER
    for (int i = 0; i < ncolumn; i++)
        free(columns[i]);
    delete[] columns;
#else
    free(columns);
#endif

    output(rank, size, prefix, outdir);

    //delete mr;

#ifdef OUTPUT_RESULT
    if (rank == 0) {
        fclose(rf);
    }
#endif

    Mimir_Finalize();
    MPI_Finalize();
}

// partiton <key,value> based on the key
int mypartition(const char *key, int keybytes)
{
    int64_t v = *(int64_t *) key;
    int v_rank = 0;
    if (v < quot * rem + rem)
        v_rank = (int) (v / (quot + 1));
    else
        v_rank = (int) ((v - rem) / quot);
    return v_rank;
}

// count edge number of each vertex
void countedge(KVContainer *edges)
{
    edges->open();
    KVRecord *edge = NULL;
    while ((edge = (KVRecord*)(edges->read())) != NULL) {
        nlocaledges += 1;
        int64_t v0 = *(int64_t *) edge->get_key();
        rowstarts[v0 - nvertoffset + 1] += 1;
    }
    edges->close();
}

// read edge list from files
void fileread(Readable *input, Writable *output, void *ptr)
{
    BaseRecordFormat *record = NULL;
    while ((record = input->read()) != NULL) {
        char *word = record->get_record();
        char sep[10] = " ";
        char *v0, *v1;
        char *saveptr = NULL;
        v0 = strtok_r(word, sep, &saveptr);
        v1 = strtok_r(NULL, sep, &saveptr);

        // skip self-loop edge
        if (strcmp(v0, v1) == 0) {
            return;
        }
        int64_t int_v0 = strtoull(v0, NULL, 0);
        int64_t int_v1 = strtoull(v1, NULL, 0);
        if (int_v0 >= nglobalverts || int_v1 >= nglobalverts) {
            fprintf(stderr,
                "The vertex index <%ld,%ld> is larger than maximum value %ld!\n",
                int_v0, int_v1, nglobalverts);
            exit(1);
        }
        KVRecord output_record((char *) &int_v0, sizeof(int64_t), 
                               (char *) &int_v1, sizeof(int64_t));
        output->write(&output_record);
    }
}

// make CSR graph based edge list
void makegraph(KVContainer *edges)
{
    edges->open();

    KVRecord *edge = NULL;
    while ((edge = (KVRecord*)(edges->read())) != NULL) {
        int64_t v0, v0_local, v1;
        v0 = *(int64_t *) edge->get_key();
        v0_local = v0 - nvertoffset;

        v1 = *(int64_t *) edge->get_val();
#ifndef COLUMN_SINGLE_BUFFER
        size_t pos = rowstarts[v0_local] + rowinserts[v0_local];
        columns[GET_COL_IDX(pos)][GET_COL_OFF(pos)] = v1;
#else
        columns[rowstarts[v0_local] + rowinserts[v0_local]] = v1;
#endif
        rowinserts[v0_local]++;
    }

    edges->close();
}

// expand child vertexes of root
void rootvisit(Readable *input, Writable *output, void *ptr)
{
    if (mypartition((char *) &root, sizeof(int64_t)) == rank) {
        int64_t root_local = root - nvertoffset;
        pred[root_local] = root;
        SET_VISITED(root_local, vis);
        size_t p_end = rowstarts[root_local + 1];
        for (size_t p = rowstarts[root_local]; p < p_end; p++) {
#ifndef COLUMN_SINGLE_BUFFER
            int64_t v1 = columns[GET_COL_IDX(p)][GET_COL_OFF(p)];
#else
            int64_t v1 = columns[p];
#endif
            KVRecord output_record((char *) &v1, sizeof(int64_t),
                                   (char *) &root, sizeof(int64_t));
            output->write(&output_record);
            //mr->add_key_value((char *) &v1, sizeof(int64_t), 
            //                  (char *) &root, sizeof(int64_t));
        }
    }
}

// Keep active vertexes in next level only
void expand(Readable *input, Writable *output, void *ptr)
{
    BaseRecordFormat *input_record = NULL;

    while ((input_record = input->read()) != NULL) {
        int64_t v = *(int64_t *) (((KVRecord*)input_record)->get_key());
        int64_t v_local = v - nvertoffset;

        int64_t v0 = *(int64_t *) (((KVRecord*)input_record)->get_val());

    // printf("v0=%ld\n", v0);

        if (!TEST_VISITED(v_local, vis)) {
            SET_VISITED(v_local, vis);
            pred[v_local] = v0;
            // mr->add_key_value(key, keybytes, NULL, 0);
            size_t p_end = rowstarts[v_local + 1];
            for (size_t p = rowstarts[v_local]; p < p_end; p++) {
#ifndef COLUMN_SINGLE_BUFFER
                int64_t v1 = columns[GET_COL_IDX(p)][GET_COL_OFF(p)];
#else
                int64_t v1 = columns[p];
#endif
                KVRecord output_record((char *) &v1, sizeof(int64_t), 
                                       (char *) &v, sizeof(int64_t));
                output->write(&output_record);
                //mr->add_key_value((char *) &v1, sizeof(int64_t), 
                //              (char *) &v, sizeof(int64_t));
            }
        }
    }
}

// Compress KV with the sarank key
void combiner(Combinable *combiner, KVRecord *kv1, KVRecord *kv2, void *ptr)
{
    combiner->update(kv1);
}

// Rondom chosen a vertex in the CC part of the graph
int64_t getrootvert()
{
    int64_t myroot;
    // Get a root
    do {
        if (rank == 0) {
            myroot = rand() % nglobalverts;
        }
        MPI_Bcast((void *) &myroot, 1, MPI_INT64_T, 0, MPI_COMM_WORLD);
        int64_t myroot_proc = mypartition((char *) &myroot, (int) sizeof(int64_t));
        if (myroot_proc == rank) {
            int64_t root_local = myroot - nvertoffset;
            if (rowstarts[root_local + 1] - rowstarts[root_local] == 0) {
                myroot = -1;
            }
        }
        MPI_Bcast((void *) &myroot, 1, MPI_INT64_T, (int) myroot_proc, MPI_COMM_WORLD);
    } while (myroot == -1);
    return myroot;
}

void printresult(int64_t * pred, size_t nlocalverts)
{
    for (size_t i = 0; i < nlocalverts; i++) {
        size_t v = nlocalverts * rank + i;
        printf("%ld:%ld\n", v, pred[i]);
    }
}
