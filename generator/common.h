#ifndef MIMIR_GENERATOR_COMMON_H
#define MIMIR_GENERATOR_COMMON_H

#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <limits.h>

#include <fstream>
#include <iostream>
#include <map>
#include <vector>
#include <string>
#include <random>
#include <cmath>
#include <set>
#include <unordered_set>
#include <algorithm>

#include <mpi.h>

#include <vector>
#include <string>

int get_word_length(int mean, double sd);
int generate_unique_words(uint64_t n_unique, std::vector<std::string>& output,
                          int len_mean, double len_sd);
void gen_dist_map(uint64_t zipf_n, double zipf_alpha,
                  double* dist_map, uint64_t* div_idx_map,
                  double *div_dist_map);
void repartition_dist_map(uint64_t zipf_n, double* dist_map,
                          uint64_t* div_idx_map, double* dist_new_map);
void repartition_unique_words(std::vector<std::string>& unique_words,
                              std::vector<std::string>& unique_new_words,
                              uint64_t* div_idx_map);

#endif
