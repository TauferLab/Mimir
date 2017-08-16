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

#define SPAN_TREE_TAG   0x00
#define EDGE_LIST_TAG   0x01
#define ACTIVE_EDGE_TAG 0x02

#define GET_TAG(v)      (((v & 0xc000000000000000) >> 62) & 0x3UL)
#define GET_VAL(v)        (v & 0x3fffffffffffffff)
#define SET_VAL(v,tag)  (((tag & 0x3UL) << 62) | (v & 0x3fffffffffffffff))

void fileread (Readable<char*, void> *input,
               Writable<int64_t, int64_t> *output, void *ptr);
void init_root(Readable<int64_t, int64_t> *input,
               Writable<int64_t, int64_t> *output, void *ptr);
void map_copy(Readable<int64_t, int64_t> *input,
              Writable<int64_t, int64_t> *output, void *ptr);
void join_edge_list_reduce(Readable<int64_t, int64_t> *input,
                           Writable<int64_t, int64_t> *output, void *ptr);
void add_span_tree(Readable<int64_t, int64_t> *input,
                   Writable<int64_t, int64_t> *output, void *ptr);
void add_active_vertex(Readable<int64_t, int64_t> *input,
                       Writable<int64_t, int64_t> *output, void *ptr);
void deduplicate(Readable<int64_t, int64_t> *input,
                 Writable<int64_t, int64_t> *output, void *ptr);
void cut_edge_list(Readable<int64_t, int64_t> *input,
                   Writable<int64_t, int64_t> *output, void *ptr);
void print_kv (int64_t *v0, int64_t *v1, void *ptr);

int rank, size;
int64_t nglobalverts;
int64_t root;

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

    root         = strtoull(argv[1], NULL, 0);
    nglobalverts = strtoull(argv[2], NULL, 0);
    std::string output = argv[3];
    std::vector<std::string> input;
    for (int i = 4; i < argc; i++) {
        input.push_back(argv[i]);
    }

    // Load graphs
    MimirContext<int64_t,int64_t,char*,void> *graph_loader
        = new MimirContext<int64_t,int64_t,char*,void>(input);
    graph_loader->map(fileread);

    //MimirContext<int64_t,int64_t> *edge_list = new MimirContext<int64_t,int64_t>();

    // Initialize root
    int tag = 0;
    MimirContext<int64_t,int64_t>* active_edge = new MimirContext<int64_t,int64_t>();
    tag = ACTIVE_EDGE_TAG;
    active_edge->map(init_root, &tag);

    MimirContext<int64_t,int64_t>* span_tree = new MimirContext<int64_t,int64_t>(
                                                 std::vector<std::string>(),
                                                 output);
    tag = SPAN_TREE_TAG;
    span_tree->map(init_root, &tag);

    int level = 0;
    do {
        // Join with edge list
        //if (level == 0) {
        active_edge->insert_data_handle(graph_loader->get_data_handle());
        //} else {
        //    active_edge->insert_data_handle(edge_list->get_data_handle());
        //}
        active_edge->map(map_copy);
        nactives[level] = active_edge->reduce(join_edge_list_reduce);
        if (rank == 0) {
            fprintf(stdout, "level=%d, nactives=%ld\n", level, nactives[level]);
        }
        // Join with span tree
        active_edge->insert_data_handle(span_tree->get_data_handle());
        active_edge->map(map_copy);
        nactives[level] = active_edge->reduce(deduplicate);
        // Add active edges to span tree
        span_tree->insert_data_handle(active_edge->get_data_handle());
        span_tree->map(add_span_tree);
        // Cut visited edges
        //if (level == 0)
        //    edge_list->insert_data_handle(graph_loader->get_data_handle());
        //edge_list->insert_data_handle(active_edge->get_data_handle());
        //edge_list->map(add_active_vertex);
        //if (level == 0) delete graph_loader;
        //edge_list->reduce(cut_edge_list);
        level ++;
    } while(nactives[level - 1]);

    // Output span tree
    span_tree->output("text");

    delete graph_loader;
    //delete edge_list;
    delete active_edge;
    delete span_tree;

    MPI_Finalize();
}

// read edge list from files
void fileread(Readable<char*, void> *input,
              Writable<int64_t, int64_t> *output, void *ptr)
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
                "The vertex index <%ld,%ld> is larger than maximum value %ld!\n",
                int_v0, int_v1, nglobalverts);
            exit(1);
        }
        int64_t val = SET_VAL(int_v1, EDGE_LIST_TAG);
        output->write(&int_v0, &val);
    }
}

// expand child vertexes of root
void init_root(Readable<int64_t, int64_t> *input,
               Writable<int64_t, int64_t> *output, void *ptr)
{
    if (rank == 0) {
        int tag = *(int*)ptr;
        int64_t val = SET_VAL(root, tag);
        output->write(&root, &val);
    }
}

void map_copy(Readable<int64_t, int64_t> *input,
              Writable<int64_t, int64_t> *output, void *ptr) {
    int64_t key;
    int64_t val;
    while (input->read(&key, &val) == true) {
        output->write(&key, &val);
    }
}

void add_span_tree(Readable<int64_t, int64_t> *input,
                   Writable<int64_t, int64_t> *output, void *ptr) {
    int64_t key;
    int64_t val;
    while (input->read(&key, &val) == true) {
        val = SET_VAL(val, SPAN_TREE_TAG);
        output->write(&key, &val);
    }
}

void add_active_vertex(Readable<int64_t, int64_t> *input,
                       Writable<int64_t, int64_t> *output, void *ptr) {
    int64_t key;
    int64_t val;
    while (input->read(&key, &val) == true) {
        int64_t tmp = val;
        val = SET_VAL(key,GET_TAG(tmp));
        key = GET_VAL(tmp);
        output->write(&key, &val);
    }
}


void join_edge_list_reduce(Readable<int64_t, int64_t> *input,
                           Writable<int64_t, int64_t> *output, void *ptr) {
    int64_t key;
    int64_t val;
    std::vector<int64_t> db;
    while (input->read(&key, &val) == true) {
        if (GET_TAG(val) == ACTIVE_EDGE_TAG) {
            db.push_back(val);
        }
    }

    if (db.size() != 0) {
        while (input->read(&key, &val) == true) {
            if (GET_TAG(val) == EDGE_LIST_TAG) {
                int64_t newval = SET_VAL(key, ACTIVE_EDGE_TAG);
                val = SET_VAL(val,  0);
                output->write(&val, &newval);
            }
        }
    }
}

void deduplicate(Readable<int64_t, int64_t> *input,
                 Writable<int64_t, int64_t> *output, void *ptr) {
    int64_t key;
    int64_t val;
    bool is_visited = false;
    while (input->read(&key, &val) == true) {
        if (GET_TAG(val) == SPAN_TREE_TAG) {
            is_visited = true;
        }
    }

    if (!is_visited) {
        output->write(&key, &val);
    }
}


void cut_edge_list(Readable<int64_t, int64_t> *input,
                   Writable<int64_t, int64_t> *output, void *ptr) {
    int64_t key;
    int64_t val;
    bool is_visited = false;
    while (input->read(&key, &val) == true) {
        if (GET_TAG(val) == ACTIVE_EDGE_TAG) {
            is_visited = true;
            break;
        }
    }

    if (!is_visited) {
        while (input->read(&key, &val) == true) {
            output->write(&key, &val);
        }
    }
}

void print_kv (int64_t *v0, int64_t *v1, void *ptr) {
    printf("tag=%lx, v0=%ld, v1=%ld\n", GET_TAG(*v1), *v0, GET_VAL(*v1));
}
