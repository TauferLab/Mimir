#ifndef STAT_H
#define STAT_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <vector>
#include <string>

//#include "config.h"

namespace MAPREDUCE_NS {

class Stat{
public:
  Stat(int _ntimer, int _ncounter);
  ~Stat();

  void inc_counter(int, int, uint64_t inc=1);
  void inc_timer(int, int, double inc=0.0);

  void print(int verb=0, FILE *out=stdout);

  void clear();

public:
  int tnum, ntimer, ncounter;
  uint64_t **counters;
  double **timers;
};
}

#if GATHER_STAT
extern Stat st;
#endif

#endif
