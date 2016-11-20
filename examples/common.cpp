#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

#include "mapreduce.h"
#include "common.h"

using namespace MIMIR_NS;

const char* commsize = NULL;
const char* pagesize = NULL;
const char* ibufsize = NULL;

void get_time_str(char *timestr){
    time_t t = time(NULL);
    struct tm tm = *localtime(&t);
    sprintf(timestr, "%d-%d-%d-%d:%d:%d", \
        tm.tm_year + 1900, 
        tm.tm_mon + 1, 
        tm.tm_mday, 
        tm.tm_hour, 
        tm.tm_min, 
        tm.tm_sec);
}

void check_envars(int rank, int size){
    if(getenv("MIMIR_COMM_SIZE")==NULL ||\
        getenv("MIMIR_PAGE_SIZE")==NULL ||\
        getenv("MIMIR_IBUF_SIZE")==NULL){
        if(rank==0)
            printf("Please set MIMIR_COMM_SIZE, MIMIR_PAGE_SIZE and MIMIR_IBUF_SIZE environments!\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

void output(int rank, int size, \
    const char*prefix, const char *outdir)
{
    //MapReduce::output_stat("test");
    MPI_Barrier(MPI_COMM_WORLD);

    commsize = getenv("MIMIR_COMM_SIZE");
    pagesize = getenv("MIMIR_PAGE_SIZE");
    ibufsize = getenv("MIMIR_IBUF_SIZE");

    char timestr[1024];
    get_time_str(timestr);

    char filename[1024];
    sprintf(filename, "%s/%s-%d_c%s-p%s-i%s_%s", outdir, prefix, size, \
        commsize, pagesize, ibufsize, timestr);

    MapReduce::output_stat(filename);

    MPI_Barrier(MPI_COMM_WORLD);

}

#if 0
    if(rank==0){
        char timestr[1024];
        get_time_str(timestr);

        char filter[1024],outfile[1024];

        sprintf(filter,"%s.profiler.%d.*", filename, size);
        sprintf(outfile,"%s.profiler.%d_%s.txt",filename,size,timestr);

        printf("filter=%s, outfile=%s\n", filter, outfile);

#ifdef BQG
        FILE* finalize_script = fopen(prefix, "w");
        fprintf(finalize_script, "#!/bin/zsh\n");
        fprintf(finalize_script, "cat %s>>%s\n", filter, outfile);
        fprintf(finalize_script, "rm %s\n", filter);
#else
        char cmd[8192];
        sprintf(cmd, "cat %s>>%s", filter, outfile);
        int ret = system((const char*)cmd);
        sprintf(cmd, "rm %s", filter);
        ret = system((const char*)cmd);
#endif

        sprintf(filter,"%s.trace.%d.*", filename, size);
        sprintf(outfile,"%s.trace.%d_%s.txt",filename,size,timestr);

        printf("filter=%s, outfile=%s\n", filter, outfile);

#ifdef BQG
        fprintf(finalize_script, "cat %s>>%s\n", filter, outfile);
        fprintf(finalize_script, "rm %s\n", filter);
        fclose(finalize_script);
#else
        sprintf(cmd, "cat %s>>%s", filter, outfile);
        ret = system((const char*)cmd);
        sprintf(cmd, "rm %s", filter);
        ret = system((const char*)cmd);
#endif
    }
#endif


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
