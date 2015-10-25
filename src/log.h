#ifndef LOG_H
#define LOG_H

#include "mpi.h"

#ifdef _DEBUG
#define LOG_PRINT(level, str) (Log::getLogger()->print((level), (str)));
#else
#define LOG_PRINT(level, str)
#endif
#define LOG_WARNING(str) (Log::getLogger()->warning((str)));
#define LOG_ERROR(str) (Log::getLogger()->error((str)));

namespace MAPREDUCE_NS {

class Log{
public:
    Log(MPI_Comm);
    void print(int level, const char *); 
    void error(const char *);
    void warning(const char *);

    static Log *logger;
    static Log *getLogger();    

    static Log* createLogger(MPI_Comm);

private:
    MPI_Comm comm;
    int me, size;
    int level = 0;
    int mod = 0; // 0 for standard output, 1 for log file
};

}
#endif
