#include "stat.h"
#include "config.h"

#include <omp.h>

using namespace MAPREDUCE_NS;

#if GATHER_STAT
//Stat st(TIMER_NUM, COUNTER_NUM);
#endif

Stat::Stat(int _ntimer, int _ncounter){

#pragma omp parallel
{
  tnum = omp_get_num_threads();
}
 
  ntimer = _ntimer;
  ncounter = _ncounter;

  counters     = new uint64_t*[tnum];
  timers       = new double*[tnum];

  for(int i=0; i<tnum; i++){
    timers[i] = new double[ntimer];
    counters[i] = new uint64_t[ncounter] ;
  }

  clear();
}

Stat::~Stat(){
  for(int i=0; i<tnum; i++){
    delete [] timers[i];
    delete [] counters[i];
  }

  delete [] counters;
  delete [] timers;
}

void Stat::inc_counter(int tid, int id, uint64_t inc){
  counters[tid][id]+=inc;
}

void Stat::inc_timer(int tid, int id, double inc){
  timers[tid][id] += inc;
}

void Stat::print(int verb, FILE *out){
  for(int i=0; i<tnum; i++){
    fprintf(out, "%d", i);
    for(int j=0;j<ncounter; j++)
      fprintf(out, ",%lu", counters[i][j]);
    for(int j=0;j<ntimer;j++)
      fprintf(out, ",%g", timers[i][j]);
    fprintf(out, "\n");
  }
}

void Stat::clear(){
  for(int i=0; i<tnum; i++){
    for(int j=0; j<ntimer; j++)
      timers[i][j]=0.0;
    for(int j=0; j<ncounter; j++)
      counters[i][j]=0;
  }
}
