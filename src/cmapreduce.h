#ifndef MIMIR_C_MAPREDUCE_H
#define MIMIR_C_MAPREDUCE_H

#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/// map callback to init KVs
typedef void (*CUserInitKV)(void*, void *);

/// map callback to map files
typedef void (*CUserMapFile)(void*, char *, void *);

/// map callback to map KVs
typedef void (*CUserMapKV) (void*, char *, int, char *, int, void *);

/// reduce callback
typedef void (*CUserReduce)(void*, char *, int,  void*);

/// combiner callback
typedef void (*CUserCombiner)(void*, const char *, int, const char *, int, const char *, int, void*);

/// hash callback
typedef  int (*CUserHash)(const char *, int);

/// User-defined scan function
typedef void (*CUserScan)(char *, int, char *, int ,void *);


void* create_mr_object(MPI_Comm comm);
void destroy_mr_object(void*);

uint64_t init_key_value(void*, CUserInitKV myinit, void *ptr, int comm);
uint64_t map_text_file(void*, char *filename, int shared, int recurse,
      const char *seperator, CUserMapFile mymap, void *ptr, int comm);
uint64_t map_key_value(void*, void*, CUserMapKV mymap,  void *ptr, int comm);

uint64_t reduce(void*, CUserReduce _myreduce, void* ptr);

void scan(void*, CUserScan myscan, void * ptr);

uint64_t bcast(void*, int rank);
uint64_t collect(void*, int rank);

void add_key_value(void*, const char *key, int keybytes, const char *value, int valuebytes);
void update_key_value(void*, const char *key, int keybytes, const char *value, int valuebytes);

void set_combiner(void*, CUserCombiner combiner);
void set_hash(void*, CUserHash _myhash);
void set_key_length(void*, int kszie);
void set_value_length(void*, int vsize);

const void *get_first_value(void*);
const void *get_next_value(void*); 

void output(void *, FILE *fp, int key, int val);

#ifdef __cplusplus
}
#endif

#endif
