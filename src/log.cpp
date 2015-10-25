#include "log.h"

using namespace MAPREDUCE_NS; 

Log *Log::logger = NULL;

Log::Log(MPI_Comm _comm){
  comm = _comm;
  MPI_Comm_size(comm, &size);
  MPI_Comm_rank(comm, &me);
}

void Log::print(int _level, const char *_str){
  if(level > _level){
    fprintf(stdout, "%d[%d] %s\n", me, size, _str);
  }
}

void Log::error(const char * _str){
    fprintf(stderr, "%d[%d] ERROR: %s\n", me, size, _str);
    MPI_Abort(comm, 1);
}

void Log::warning(const char *_str){
    fprintf(stderr, "%d[%d] Warning: %s\n", me, size, _str);
}

Log *Log::createLogger(MPI_Comm _comm){
  if(Log::logger!=NULL)
    return Log::logger;
  
  Log::logger = new Log(_comm);
  return Log::logger;
}

Log *Log::getLogger(){
  return Log::logger;
}
