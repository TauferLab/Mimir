#ifndef KEY_MULTI_VALUE
#define KEY_MULTI_VALUE

#include "dataobject.h"

namespace MAPREDUCE_NS{

class KeyMultiValue : public DataObject{

public:
  KeyMultiValue(int, 
    int blocksize=1,
    int maxblock=4, 
    int memsize=4,
    int outofcore=0,
    std::string a6=std::string(""));

  ~KeyMultiValue();

  int getNextKMV(int, int, char **, int &, int &, char **, int **);

  void print(int type=0, FILE *fp=stdout, int format=0);

private:
  int kmvtype; // only 0 is used
};

}

#endif
