#include "stat.h"
#include "config.h"

//using namespace MAPREDUCE_NS;

#if GATHER_STAT
Stat st;
#endif

Stat::Stat(int _nmax){
  nmax = _nmax;

  counters     = new uint64_t[nmax];
  counter_verb = new int[nmax];
  ncounter     = 0;

  timers       = new double[nmax];
  timer_verb   = new int[nmax];
  ntimer       = 0;
}

Stat::~Stat(){
  delete [] counters;
  delete [] counter_verb;
  delete [] timers;
  delete [] timer_verb;
}

// multi-thread safe
int Stat::init_counter(const char *name, int verb){
  int id = __sync_fetch_and_add(&ncounter, 1);

  counters[id] = 0;
  counter_verb[id] = verb;
  counter_str.push_back(std::string(name));

  return id;
}

void Stat::inc_counter(int id, int inc){
  counters[id]++;
}

void Stat::print_counters(int verb, FILE *out){
  fprintf(out, "Total Countes: %d\n", ncounter);
  for(int i=0; i<ncounter; i++){
    if(verb>=counter_verb[i])
      fprintf(out, "Counter %d %s=%ld\n", i, counter_str[i].c_str(), counters[i]);
  }
}

int Stat::init_timer(const char *name, int verb){
  int id = __sync_fetch_and_add(&ntimer, 1);

  timers[id] = 0.0;
  timer_verb[id] = verb;
  timer_str.push_back(std::string(name));

  return id;
}

void Stat::inc_timer(int id, double inc){
  timers[id] += inc;
}

void Stat::print_timers(int verb, FILE *out){
  fprintf(out, "Total Timers: %d\n", ntimer);
  for(int i=0; i<ntimer; i++){
    if(verb>=timer_verb[i])
      fprintf(out, "Timer %d %s=%g\n", i, timer_str[i].c_str(), timers[i]);
  }
}

void Stat::print(int verb, FILE *out){
  print_counters(verb, out);
  print_timers(verb, out);
}

void Stat::clear(){
  ncounter = ntimer = 0;
  counter_str.clear();
  timer_str.clear();
}
