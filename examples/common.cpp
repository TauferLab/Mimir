#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

#include "mapreduce.h"
#include "common.h"

using namespace MIMIR_NS;

void output()
{
    MapReduce::output_stat("test");
    MPI_Barrier(MPI_COMM_WORLD);
    if(rank==0){
    }
}

#if 0
void output(const char *filenarank, const char *outdir, const char *prefix, double density, MapReduce *mr1, MapReduce *mr2){
  char header[1000];
  char tmp[1000];

  if(estimate)
    sprintf(header, "%s/%s_d%.2f-c%s-b%s-i%s-f%d-%s.%d", \
      outdir, prefix, density, gbufsize, blocksize, inputsize, factor, commmode, size);
  else
    sprintf(header, "%s/%s_d%.2f-c%s-b%s-i%s-h%d-%s.%d", \
      outdir, prefix, density, gbufsize, blocksize, inputsize, nbucket, commmode, size);

  sprintf(tmp, "%s.%d.txt", header, rank);

  FILE *fp = fopen(tmp, "w+");
  //mr1->print_stat(fp);
  MapReduce::print_stat(mr2, fp);
  fclose(fp);

  MPI_Barrier(MPI_COMM_WORLD);

  if(rank==0){
    time_t t = time(NULL);
    struct tm tm = *localtime(&t);
    char tirankstr[1024];
    sprintf(tirankstr, "%d-%d-%d-%d:%d:%d", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
    char infile[1024+1];
    sprintf(infile, "%s.*.txt", header);
    char outfile[1024+1];
    sprintf(outfile, "%s_%s.txt", header, tirankstr);
#ifdef BGQ
    FILE* finalize_script = fopen(prefix, "w");
    fprintf(finalize_script, "#!/bin/zsh\n");
    fprintf(finalize_script, "cat %s>>%s\n", infile, outfile);
    fprintf(finalize_script, "rm %s\n", infile);
    fclose(finalize_script);
#else
    char cmd[8192+1];
    sprintf(cmd, "cat %s>>%s", infile, outfile);
    system(cmd);
    sprintf(cmd, "rm %s", infile);
    system(cmd);
#endif
  }
}
#endif
