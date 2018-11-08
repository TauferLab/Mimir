//
// (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego
//     Supercomputer Center, National University of Defense Technology,
//     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
//
//     See COPYRIGHT in top-level directory.
//

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

#define BYTE_BITS 8
#define LONG_BITS (sizeof(unsigned long) * BYTE_BITS)
#define TEST_VISITED(v, vis) \
    ((vis[(v) / LONG_BITS]) & (1UL << ((v) % LONG_BITS)))
#define SET_VISITED(v, vis) \
    ((vis[(v) / LONG_BITS]) |= (1UL << ((v) % LONG_BITS)))

int mypartition(int64_t *, int64_t *, int);
void fileread(Readable<char *, void> *input, Writable<int64_t, int64_t> *output,
              void *ptr);
void makegraph(int64_t *v0, int64_t *v1, void *ptr);
void countedge(int64_t *v0, int64_t *v1, void *ptr);

int64_t getrootvert();
void rootvisit(Readable<int64_t, int64_t> *input,
               Writable<int64_t, int64_t> *output, void *ptr);
void expand(Readable<int64_t, int64_t> *input,
            Writable<int64_t, int64_t> *output, void *ptr);
void combiner(Combinable<int64_t, int64_t> *combiner, int64_t *key,
              int64_t *val1, int64_t *val2, int64_t *rval, void *ptr);

void printresult(FILE *fp, int64_t *pred, size_t nlocalverts);

int rank, size;
int64_t nglobalverts; // global vertex count
int64_t nglobaledges; // global edge count
int64_t nlocalverts;  // local vertex count
int64_t nlocaledges;  // local edge count
int64_t nvertoffset;  // local vertex's offset
int64_t quot, rem;    // quotient and reminder of globalverts/size

size_t *rowstarts; // rowstarts

// Put edges into mutiple columns to avoid large buffer allocation
int ncolumn = 0;
int64_t ncolumnedge = 8 * 1024 * 1024;
int64_t **columns; // columns
#define GET_COL_IDX(pos) ((pos) / (ncolumnedge))
#define GET_COL_OFF(pos) ((pos) % (ncolumnedge))

unsigned long *vis; // visited bitmap
int64_t *pred;      // pred map
int64_t root;       // root vertex
size_t *rowinserts; // tmp buffer for construct CSR

#define MAX_LEVEL 100
uint64_t nactives[MAX_LEVEL];

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc < 5) {
        if (rank == 0) printf("Syntax: %s root N output input ...\n", argv[0]);
        return 0;
    }

    root = strtoull(argv[1], NULL, 0);
    if (root < 0) srand((unsigned) root);
    nglobalverts = strtoull(argv[2], NULL, 0);
    std::string output = argv[3];
    std::vector<std::string> input;
    for (int i = 4; i < argc; i++) {
        input.push_back(argv[i]);
    }

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

    if (rank == 0) fprintf(stdout, "make CSR graph start.\n");

    // partition edges
    MimirContext<int64_t, int64_t, char *, void> *mimir
        = new MimirContext<int64_t, int64_t, char *, void>(
            input, output, MPI_COMM_WORLD, NULL, mypartition);
    mimir->map(fileread);

    rowstarts = new size_t[nlocalverts + 1];
    rowinserts = new size_t[nlocalverts];
    rowstarts[0] = 0;
    for (int64_t i = 0; i < nlocalverts; i++) {
        rowstarts[i + 1] = 0;
        rowinserts[i] = 0;
    }
    nlocaledges = 0;
    mimir->scan(countedge);

    for (int64_t i = 0; i < nlocalverts; i++) {
        rowstarts[i + 1] += rowstarts[i];
    }

    ncolumn = (int) (nlocaledges / ncolumnedge) + 1;
    if (rank == 0) {
        fprintf(stdout, "ncolumn=%d\n", ncolumn);
        fflush(stdout);
    }
    int64_t edge_left = nlocaledges;
    columns = new int64_t *[ncolumn];
    for (int i = 0; i < ncolumn - 1; i++) {
        columns[i] = (int64_t *) mem_aligned_malloc(
            4096, ncolumnedge * sizeof(int64_t));
        if (columns[i] == NULL) {
            fprintf(stderr, "Error: allocate buffer for edges (%ld) failed!\n",
                    nlocaledges);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        edge_left -= ncolumnedge;
    }
    columns[ncolumn - 1]
        = (int64_t *) mem_aligned_malloc(4096, edge_left * sizeof(int64_t));

    if (rank == 0) fprintf(stdout, "begin make graph.\n");

    mimir->scan(makegraph);
    delete[] rowinserts;

    if (rank == 0) fprintf(stdout, "make CSR graph end.\n");

    // Make data structure
    int64_t bitmapsize = (nlocalverts + LONG_BITS - 1) / LONG_BITS;
    vis = new unsigned long[bitmapsize];
    pred = new int64_t[nlocalverts];

    if (rank == 0) fprintf(stdout, "BFS traversal start.\n");

    // Choose the root vertex
    if (root < 0) root = getrootvert();
    memset(vis, 0, sizeof(unsigned long) * (bitmapsize));
    for (int64_t i = 0; i < nlocalverts; i++) {
        pred[i] = -1;
    }

    if (rank == 0) fprintf(stdout, "Traversal start. (root=%ld)\n", root);

    MimirContext<int64_t, int64_t> *bfs_mimir
        = new MimirContext<int64_t, int64_t>(std::vector<std::string>(),
                                             std::string(), MPI_COMM_WORLD,
                                             NULL, mypartition);

    // Ensure stat file being a single file
    delete mimir;

    bfs_mimir->map(rootvisit);

    // BFS search
    int level = 0;
    do {
        nactives[level] = bfs_mimir->map(expand);
        if (rank == 0)
            fprintf(stdout, "level=%d, nactives=%ld\n", level, nactives[level]);
        level++;
    } while (nactives[level - 1]);

    delete bfs_mimir;

    char filename[1024];
    sprintf(filename, "%s%d.%d", output.c_str(), size, rank);
    FILE *fp = fopen(filename, "w");
    if (!fp) {
        fprintf(stderr, "Output error!\n");
        exit(1);
    }
    printresult(fp, pred, nlocalverts);
    fclose(fp);

    if (rank == 0) fprintf(stdout, "BFS traversal end.\n");

    delete[] vis;
    delete[] pred;

    delete[] rowstarts;
    for (int i = 0; i < ncolumn; i++) free(columns[i]);
    delete[] columns;

    MPI_Finalize();
}

// partiton <key,value> based on the key
int mypartition(int64_t *v, int64_t *v1, int partition)
{
    //int64_t v = *(int64_t *) key;
    int v_rank = 0;
    if (*v < quot * rem + rem)
        v_rank = (int) (*v / (quot + 1));
    else
        v_rank = (int) ((*v - rem) / quot);
    return v_rank;
}

// count edge number of each vertex
void countedge(int64_t *v0, int64_t *v1, void *ptr)
{
    nlocaledges += 1;
    rowstarts[*v0 - nvertoffset + 1] += 1;
}

// read edge list from files
void fileread(Readable<char *, void> *input, Writable<int64_t, int64_t> *output,
              void *ptr)
{
    char *word;
    while (input->read(&word, NULL) == true) {
        char sep[10] = " ";
        char *v0, *v1;
        char *saveptr = NULL;
        v0 = strtok_r(word, sep, &saveptr);
        v1 = strtok_r(NULL, sep, &saveptr);

        // skip self-loop edge
        if (strcmp(v0, v1) == 0) {
            continue;
        }
        int64_t int_v0 = strtoull(v0, NULL, 0);
        int64_t int_v1 = strtoull(v1, NULL, 0);
        if (int_v0 >= nglobalverts || int_v1 >= nglobalverts) {
            fprintf(stderr,
                    "The vertex index <%ld,%ld> is larger than maximum value "
                    "%ld!\n",
                    int_v0, int_v1, nglobalverts);
            exit(1);
        }
        output->write(&int_v0, &int_v1);
    }
}

// make CSR graph based edge list
void makegraph(int64_t *v0, int64_t *v1, void *ptr)
{
    int64_t v0_local = *v0 - nvertoffset;
    size_t pos = rowstarts[v0_local] + rowinserts[v0_local];
    columns[GET_COL_IDX(pos)][GET_COL_OFF(pos)] = *v1;
    rowinserts[v0_local]++;
}

// expand child vertexes of root
void rootvisit(Readable<int64_t, int64_t> *input,
               Writable<int64_t, int64_t> *output, void *ptr)
{
    if (mypartition(&root, NULL, 0) == rank) {
        int64_t root_local = root - nvertoffset;
        pred[root_local] = root;
        SET_VISITED(root_local, vis);
        size_t p_end = rowstarts[root_local + 1];
        for (size_t p = rowstarts[root_local]; p < p_end; p++) {
            int64_t v1 = columns[GET_COL_IDX(p)][GET_COL_OFF(p)];
            output->write(&v1, &root);
        }
    }
}

// keep active vertexes in next level only
void expand(Readable<int64_t, int64_t> *input,
            Writable<int64_t, int64_t> *output, void *ptr)
{
    int64_t v, v0;
    while (input->read(&v, &v0) == true) {
        int64_t v_local = v - nvertoffset;

        if (!TEST_VISITED(v_local, vis)) {
            SET_VISITED(v_local, vis);
            pred[v_local] = v0;
            size_t p_end = rowstarts[v_local + 1];
            for (size_t p = rowstarts[v_local]; p < p_end; p++) {
                int64_t v1 = columns[GET_COL_IDX(p)][GET_COL_OFF(p)];
                output->write(&v1, &v);
            }
        }
    }
}

// compress KV with the rank key
void combiner(Combinable<int64_t, int64_t> *combiner, int64_t *key,
              int64_t *val0, int64_t *val1, int64_t *rval, void *ptr)
{
    *rval = *val0 + *val1;
}

// rondom chosen a vertex in the CC part of the graph
int64_t getrootvert()
{
    int64_t myroot;
    // Get a root
    do {
        if (rank == 0) {
            myroot = rand() % nglobalverts;
        }
        MPI_Bcast((void *) &myroot, 1, MPI_INT64_T, 0, MPI_COMM_WORLD);
        int64_t myroot_proc = mypartition(&myroot, NULL, 0);
        if (myroot_proc == rank) {
            int64_t root_local = myroot - nvertoffset;
            if (rowstarts[root_local + 1] - rowstarts[root_local] == 0) {
                myroot = -1;
            }
        }
        MPI_Bcast((void *) &myroot, 1, MPI_INT64_T, (int) myroot_proc,
                  MPI_COMM_WORLD);
    } while (myroot == -1);
    return myroot;
}

void printresult(FILE *fp, int64_t *pred, size_t nlocalverts)
{
    for (size_t i = 0; i < nlocalverts; i++) {
        size_t v = nvertoffset + i;
        fprintf(fp, "%ld %ld\n", v, pred[i]);
    }
}
